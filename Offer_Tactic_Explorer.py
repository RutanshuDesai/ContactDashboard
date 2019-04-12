
# coding: utf-8

# In[3]:


import datascience as ds
from gspread_pandas import Spread
import os


# ### Loading data from VDM to RS

# In[2]:


# STEP 1
# query from VDM for contact_inqrs table
inqr = ds.query_JDV("""SELECT  a_offerid1, c_emailaddress1, a_timestamp1,a_offerdetails_campaignname1, a_tacticid_external1 , a_tacticdetails_campaignname1
                        FROM eloqua_contact_inqrs WHERE a_timestamp1 BETWEEN '2018-03-01' AND CURDATE() 
                        and  status_flag = true  and c_country1 in ('US','United States','United States of America','UNITED STATES','Canada','CA') 
                        """, vdm='marketing_vdm_dynamic' )


# In[3]:


# STEP 2
# transfering accounts data to redshift instance
inqr.to_redshift('contacts_inqr','na_field_marketing',group ='na_field_marketing_rw', if_exists='replace',  instance='DS')


# ### Offer ID data

# In[4]:


## deleting the existing tables
ds.query_RS('drop table na_field_marketing.contacts_inqr_first_offer' , instance='DS', option='execute')
ds.query_RS('drop table na_field_marketing.contacts_inqr_first_tactic' , instance='DS', option='execute')
ds.query_RS('drop table na_field_marketing.prod_offertacticexp_multi' , instance='DS', option='execute')


# In[5]:


## Creating the first interaction of a person with a unique offer
ds.query_RS ( """create table na_field_marketing.contacts_inqr_first_offer as (
select a_offerid1, c_emailaddress1,
       min(a_timestamp1) as "First_timestamp_offer"

from na_field_marketing.contacts_inqr
group by a_offerid1, c_emailaddress1
);""", instance = 'DS', option = 'execute')


# In[6]:


## Creating the first interaction of a person with a unique tactic
ds.query_RS ( """create table na_field_marketing.contacts_inqr_first_tactic as (
select a_tacticid_external1, c_emailaddress1,
       min(a_timestamp1) as "First_timestamp_tactic"

from na_field_marketing.contacts_inqr
group by a_tacticid_external1, c_emailaddress1 ) ;""", instance = 'DS', option = 'execute')


# In[7]:


## changing the data types

ds.query_RS ( """-- CHANGING DATA TYPE FOR TIMESTAMP in the Inquiries_first table
ALTER TABLE na_field_marketing.contacts_inqr_first_offer ADD COLUMN "first_timestamp_new" date ;
UPDATE na_field_marketing.contacts_inqr_first_offer SET "first_timestamp_new" = cast ("first_timestamp_offer" as date );
ALTER TABLE na_field_marketing.contacts_inqr_first_offer DROP COLUMN "first_timestamp_offer";
ALTER TABLE na_field_marketing.contacts_inqr_first_offer RENAME COLUMN "first_timestamp_new" TO "first_timestamp_offer";



ALTER TABLE na_field_marketing.contacts_inqr_first_tactic ADD COLUMN "first_timestamp_new" date ;
UPDATE na_field_marketing.contacts_inqr_first_tactic SET "first_timestamp_new" = cast ("first_timestamp_tactic" as date );
ALTER TABLE na_field_marketing.contacts_inqr_first_tactic DROP COLUMN "first_timestamp_tactic";
ALTER TABLE na_field_marketing.contacts_inqr_first_tactic RENAME COLUMN "first_timestamp_new" TO "first_timestamp_tactic";


  -- CHANGING DATA TYPE FOR First_TIMESTAMP in the Inquiries  table
ALTER TABLE na_field_marketing.contacts_inqr ADD COLUMN "a_timestamp1_new" date ;
UPDATE na_field_marketing.contacts_inqr SET "a_timestamp1_new" = cast ("a_timestamp1" as date );
ALTER TABLE na_field_marketing.contacts_inqr DROP COLUMN "a_timestamp1";
ALTER TABLE na_field_marketing.contacts_inqr RENAME COLUMN "a_timestamp1_new" TO "a_timestamp1";



  -- CHANGING DATA TYPE FOR is_publicsector  in the contacts_all_flagged table
ALTER TABLE na_field_marketing.nafm_contacts_all_flagged ADD COLUMN "is_publicsector_new" varchar ;
UPDATE na_field_marketing.nafm_contacts_all_flagged SET "is_publicsector_new" = cast ("is_publicsector" as varchar);
ALTER TABLE na_field_marketing.nafm_contacts_all_flagged DROP COLUMN "is_publicsector";
ALTER TABLE na_field_marketing.nafm_contacts_all_flagged RENAME COLUMN "is_publicsector_new" TO "is_publicsector";"""
             , instance = 'DS', option = 'execute')



# In[9]:


## Creating the intermediate table table
ds.query_RS ( """create table na_field_marketing.cncts_offerid_tacticid as (
  select  c.*,i.*,

   CASE
        when c.sfdc_account_classification is null or c.sfdc_account_classification = '' then 'No Account Classification'
        when c.sfdc_account_classification in ('Unclassified','Duplicate Account - Delete','Hosting/T2 OEM','TERRITORY','UNALLOCATED','EMEA - Hosting','GLOBAL') then 'Other'
        when c.sfdc_account_classification in ('Partner - Premier','Partner - OEM','Partner - Advanced Partner','Partner - Distributor','Partner - ISV','Partner - Certified Services Partner','Partner - Systems Integrator','Partner - Ready Partner','Partner - Service/Cloud Provider','Partner - Training','Partner - Other','Partner - Unaffiliated') then 'Partner'
        when c.sfdc_account_classification in ( 'Sales - EDU', 'Sales - US Federal Government', 'Sales - State and Local') then 'PS'
        when c.sfdc_account_classification in ('Strategic','STRATEGIC') then 'Strategic'
        else c.sfdc_account_classification
        END AS "Grand_Acc_Classification",

        split_part(i.c_emailaddress1,'@',2) as domain_inqr


from na_field_marketing.contacts_inqr i
    LEFT join na_field_marketing.nafm_contacts_all_flagged c on lower(c.c_emailaddress) = lower(i.c_emailaddress1)
    where  lower(domain_inqr) != 'redhat.com'


    );""", instance = 'DS', option = 'execute')


# In[10]:


## Joinign the first timestamp feature with the offer_id table

ds.query_RS ( """-- ADDING FIRST_TIMESTAMP FIELD AND NEW CONTACT LOGIC
create table na_field_marketing.prod_offertacticexp_multi as (

  select o.*,offer.first_timestamp_offer, tac.first_timestamp_tactic,

   case
        when offer.first_timestamp_offer = o.c_datecreated   then 'New Contact' else 'Existing Contact'
    END as "new_contact_offer",

     case
        when tac.First_timestamp_tactic = o.c_datecreated   then 'New Contact' else 'Existing Contact'
    END as "new_contact_tactic"

  from na_field_marketing.cncts_offerid_tacticid o
  LEFT JOIN na_field_marketing.contacts_inqr_first_offer offer on ((o.a_offerid1 = offer.a_offerid1) and (o.c_emailaddress1 = offer.c_emailaddress1))
  left join na_field_marketing.contacts_inqr_first_tactic tac on ((o.a_tacticid_external1 = tac.a_tacticid_external1) and (o.c_emailaddress1 = tac.c_emailaddress1))
  where  lower(o.domain_inqr) != 'redhat.com'

);

drop table na_field_marketing.cncts_offerid_tacticid;






""", instance = 'DS', option = 'execute')


