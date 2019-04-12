
# coding: utf-8

# ### Importing modules

# In[23]:


import datascience as ds
from gspread_pandas import Spread
import os
import pandas as pd


# ### Extracting and Loading Data

# In[13]:


## Accounts: Querying VDM
accounts = ds.query_JDV("""SELECT sfdc_account_classification, sfdc_account_id, sfdc_account_record_type_name, sfdc_account_organization, sfdc_account_owner_sub_region, sfdc_account_sub_organization, sfdc_account_owner_name 
                            FROM APL_VDB_ELOQUA.eloqua_accnts  
                            WHERE   sfdc_account_record_type_name != '' and sfdc_account_owner_sub_region != ''"""
                        , vdm='marketing_vdm_dynamic' )


# In[ ]:


## Accounts: Loading to RS
accounts.to_redshift('nafm_accounts_c','na_field_marketing',group ='na_field_marketing_rw', if_exists='replace', instance='DS')


# In[ ]:


## Contacts: Querying VDM
contacts_2010 = ds.query_JDV(""" SELECT c_address1, c_address2, c_address3, c_busphone, c_city, c_company,c_country, c_datecreated, c_derived__persona1, c_dwm_completeness_level1, c_emailaddress, 
                            CAST(SUBSTRING(c_emailaddress, LOCATE('@',c_emailaddress)+1, 255) AS STRING) AS domain_all, c_sfdcleadid,
                            c_engagement_status1,c_engagement_status_last_modified_timestamp1,c_firstname,c_lastname,c_metro_core_based_statistical_area1, c_mobilephone,c_sfdcaccountid,c_sfdccontactid,c_super_region1, c_zip_postal, contact_id, isbounced, issubscribed, status_flag,stg_del_flg,c_industry1 
                            FROM eloqua_cntcts 
                            WHERE  c_super_region1 = 'NA' AND  status_flag = 1 AND stg_del_flg = 'false' AND c_engagement_status1  IN ('Most Active','Lapsing','Lapsed' ,'Inactive', 'Pending', 'Invalid') AND c_derived__persona1 IN ('','Business Analyst','IT Manager','IT Executive','Lead Developer','System Administrator','Architect', 'Other') AND c_derived__persona1 IN ('','Business Analyst','IT Manager','IT Executive','Lead Developer','System Administrator','Architect', 'Other') and
                               year (c_datecreated) = '2010'"""                     
                    , vdm='marketing_vdm_dynamic')


# In[ ]:


## Contacts: Querying VDM
contacts_2011 = ds.query_JDV(""" SELECT c_address1, c_address2, c_address3, c_busphone, c_city, c_company,c_country, c_datecreated, c_derived__persona1, c_dwm_completeness_level1, c_emailaddress, 
                            CAST(SUBSTRING(c_emailaddress, LOCATE('@',c_emailaddress)+1, 255) AS STRING) AS domain_all, c_sfdcleadid,
                            c_engagement_status1,c_engagement_status_last_modified_timestamp1,c_firstname,c_lastname,c_metro_core_based_statistical_area1, c_mobilephone,c_sfdcaccountid,c_sfdccontactid,c_super_region1, c_zip_postal, contact_id, isbounced, issubscribed, status_flag,stg_del_flg,c_industry1 
                            FROM eloqua_cntcts 
                            WHERE  c_super_region1 = 'NA' AND  status_flag = 1 AND stg_del_flg = 'false' AND c_engagement_status1  IN ('Most Active','Lapsing','Lapsed' ,'Inactive', 'Pending', 'Invalid') AND c_derived__persona1 IN ('','Business Analyst','IT Manager','IT Executive','Lead Developer','System Administrator','Architect', 'Other') AND c_derived__persona1 IN ('','Business Analyst','IT Manager','IT Executive','Lead Developer','System Administrator','Architect', 'Other') and
                               year (c_datecreated) = '2011'"""                     
                    , vdm='marketing_vdm_dynamic')


# In[ ]:


## Contacts: Querying VDM
contacts_2012 = ds.query_JDV(""" SELECT c_address1, c_address2, c_address3, c_busphone, c_city, c_company,c_country, c_datecreated, c_derived__persona1, c_dwm_completeness_level1, c_emailaddress, 
                            CAST(SUBSTRING(c_emailaddress, LOCATE('@',c_emailaddress)+1, 255) AS STRING) AS domain_all, c_sfdcleadid,
                            c_engagement_status1,c_engagement_status_last_modified_timestamp1,c_firstname,c_lastname,c_metro_core_based_statistical_area1, c_mobilephone,c_sfdcaccountid,c_sfdccontactid,c_super_region1, c_zip_postal, contact_id, isbounced, issubscribed, status_flag,stg_del_flg,c_industry1 
                            FROM eloqua_cntcts 
                            WHERE  c_super_region1 = 'NA' AND  status_flag = 1 AND stg_del_flg = 'false' AND c_engagement_status1  IN ('Most Active','Lapsing','Lapsed' ,'Inactive', 'Pending', 'Invalid') AND c_derived__persona1 IN ('','Business Analyst','IT Manager','IT Executive','Lead Developer','System Administrator','Architect', 'Other') AND c_derived__persona1 IN ('','Business Analyst','IT Manager','IT Executive','Lead Developer','System Administrator','Architect', 'Other') and
                               year (c_datecreated) = '2012'"""                     
                    , vdm='marketing_vdm_dynamic')


# In[ ]:


## Contacts: Querying VDM
contacts_2013 = ds.query_JDV(""" SELECT c_address1, c_address2, c_address3, c_busphone, c_city, c_company,c_country, c_datecreated, c_derived__persona1, c_dwm_completeness_level1, c_emailaddress, 
                            CAST(SUBSTRING(c_emailaddress, LOCATE('@',c_emailaddress)+1, 255) AS STRING) AS domain_all, c_sfdcleadid,
                            c_engagement_status1,c_engagement_status_last_modified_timestamp1,c_firstname,c_lastname,c_metro_core_based_statistical_area1, c_mobilephone,c_sfdcaccountid,c_sfdccontactid,c_super_region1, c_zip_postal, contact_id, isbounced, issubscribed, status_flag,stg_del_flg,c_industry1 
                            FROM eloqua_cntcts 
                            WHERE  c_super_region1 = 'NA' AND  status_flag = 1 AND stg_del_flg = 'false' AND c_engagement_status1  IN ('Most Active','Lapsing','Lapsed' ,'Inactive', 'Pending', 'Invalid') AND c_derived__persona1 IN ('','Business Analyst','IT Manager','IT Executive','Lead Developer','System Administrator','Architect', 'Other') AND c_derived__persona1 IN ('','Business Analyst','IT Manager','IT Executive','Lead Developer','System Administrator','Architect', 'Other') and
                               year (c_datecreated) = '2013'"""                     
                    , vdm='marketing_vdm_dynamic')


# In[ ]:


## Contacts: Querying VDM
contacts_2014 = ds.query_JDV(""" SELECT c_address1, c_address2, c_address3, c_busphone, c_city, c_company,c_country, c_datecreated, c_derived__persona1, c_dwm_completeness_level1, c_emailaddress, 
                            CAST(SUBSTRING(c_emailaddress, LOCATE('@',c_emailaddress)+1, 255) AS STRING) AS domain_all, c_sfdcleadid,
                            c_engagement_status1,c_engagement_status_last_modified_timestamp1,c_firstname,c_lastname,c_metro_core_based_statistical_area1, c_mobilephone,c_sfdcaccountid,c_sfdccontactid,c_super_region1, c_zip_postal, contact_id, isbounced, issubscribed, status_flag,stg_del_flg,c_industry1 
                            FROM eloqua_cntcts 
                            WHERE  c_super_region1 = 'NA' AND  status_flag = 1 AND stg_del_flg = 'false' AND c_engagement_status1  IN ('Most Active','Lapsing','Lapsed' ,'Inactive', 'Pending', 'Invalid') AND c_derived__persona1 IN ('','Business Analyst','IT Manager','IT Executive','Lead Developer','System Administrator','Architect', 'Other') AND c_derived__persona1 IN ('','Business Analyst','IT Manager','IT Executive','Lead Developer','System Administrator','Architect', 'Other') and
                               year (c_datecreated) = '2014'"""                     
                    , vdm='marketing_vdm_dynamic')


# In[ ]:


## Contacts: Querying VDM
contacts_2015 = ds.query_JDV(""" SELECT c_address1, c_address2, c_address3, c_busphone, c_city, c_company,c_country, c_datecreated, c_derived__persona1, c_dwm_completeness_level1, c_emailaddress, 
                            CAST(SUBSTRING(c_emailaddress, LOCATE('@',c_emailaddress)+1, 255) AS STRING) AS domain_all, c_sfdcleadid,
                            c_engagement_status1,c_engagement_status_last_modified_timestamp1,c_firstname,c_lastname,c_metro_core_based_statistical_area1, c_mobilephone,c_sfdcaccountid,c_sfdccontactid,c_super_region1, c_zip_postal, contact_id, isbounced, issubscribed, status_flag,stg_del_flg,c_industry1 
                            FROM eloqua_cntcts 
                            WHERE  c_super_region1 = 'NA' AND  status_flag = 1 AND stg_del_flg = 'false' AND c_engagement_status1  IN ('Most Active','Lapsing','Lapsed' ,'Inactive', 'Pending', 'Invalid') AND c_derived__persona1 IN ('','Business Analyst','IT Manager','IT Executive','Lead Developer','System Administrator','Architect', 'Other') AND c_derived__persona1 IN ('','Business Analyst','IT Manager','IT Executive','Lead Developer','System Administrator','Architect', 'Other') and
                               year (c_datecreated) = '2015'"""                     
                    , vdm='marketing_vdm_dynamic')


# In[18]:


## Contacts: Querying VDM
contacts_2016 = ds.query_JDV(""" SELECT c_address1, c_address2, c_address3, c_busphone, c_city, c_company,c_country, c_datecreated, c_derived__persona1, c_dwm_completeness_level1, c_emailaddress, 
                            CAST(SUBSTRING(c_emailaddress, LOCATE('@',c_emailaddress)+1, 255) AS STRING) AS domain_all, c_sfdcleadid,
                            c_engagement_status1,c_engagement_status_last_modified_timestamp1,c_firstname,c_lastname,c_metro_core_based_statistical_area1, c_mobilephone,c_sfdcaccountid,c_sfdccontactid,c_super_region1, c_zip_postal, contact_id, isbounced, issubscribed, status_flag,stg_del_flg,c_industry1 
                            FROM eloqua_cntcts 
                            WHERE  c_super_region1 = 'NA' AND  status_flag = 1 AND stg_del_flg = 'false' AND c_engagement_status1  IN ('Most Active','Lapsing','Lapsed' ,'Inactive', 'Pending', 'Invalid') AND c_derived__persona1 IN ('','Business Analyst','IT Manager','IT Executive','Lead Developer','System Administrator','Architect', 'Other') AND c_derived__persona1 IN ('','Business Analyst','IT Manager','IT Executive','Lead Developer','System Administrator','Architect', 'Other') and
                               year (c_datecreated) = '2016'"""                     
                    , vdm='marketing_vdm_dynamic')


# In[19]:


## Contacts: Querying VDM
contacts_2017 = ds.query_JDV(""" SELECT c_address1, c_address2, c_address3, c_busphone, c_city, c_company,c_country, c_datecreated, c_derived__persona1, c_dwm_completeness_level1, c_emailaddress, 
                            CAST(SUBSTRING(c_emailaddress, LOCATE('@',c_emailaddress)+1, 255) AS STRING) AS domain_all, c_sfdcleadid,
                            c_engagement_status1,c_engagement_status_last_modified_timestamp1,c_firstname,c_lastname,c_metro_core_based_statistical_area1, c_mobilephone,c_sfdcaccountid,c_sfdccontactid,c_super_region1, c_zip_postal, contact_id, isbounced, issubscribed, status_flag,stg_del_flg,c_industry1 
                            FROM eloqua_cntcts 
                            WHERE  c_super_region1 = 'NA' AND  status_flag = 1 AND stg_del_flg = 'false' AND c_engagement_status1  IN ('Most Active','Lapsing','Lapsed' ,'Inactive', 'Pending', 'Invalid') AND c_derived__persona1 IN ('','Business Analyst','IT Manager','IT Executive','Lead Developer','System Administrator','Architect', 'Other') AND c_derived__persona1 IN ('','Business Analyst','IT Manager','IT Executive','Lead Developer','System Administrator','Architect', 'Other') and
                               year (c_datecreated) = '2017'"""                     
                    , vdm='marketing_vdm_dynamic')


# In[20]:


## Contacts: Querying VDM
contacts_2018 = ds.query_JDV(""" SELECT c_address1, c_address2, c_address3, c_busphone, c_city, c_company,c_country, c_datecreated, c_derived__persona1, c_dwm_completeness_level1, c_emailaddress, 
                            CAST(SUBSTRING(c_emailaddress, LOCATE('@',c_emailaddress)+1, 255) AS STRING) AS domain_all, c_sfdcleadid,
                            c_engagement_status1,c_engagement_status_last_modified_timestamp1,c_firstname,c_lastname,c_metro_core_based_statistical_area1, c_mobilephone,c_sfdcaccountid,c_sfdccontactid,c_super_region1, c_zip_postal, contact_id, isbounced, issubscribed, status_flag,stg_del_flg,c_industry1 
                            FROM eloqua_cntcts 
                            WHERE  c_super_region1 = 'NA' AND  status_flag = 1 AND stg_del_flg = 'false' AND c_engagement_status1  IN ('Most Active','Lapsing','Lapsed' ,'Inactive', 'Pending', 'Invalid') AND c_derived__persona1 IN ('','Business Analyst','IT Manager','IT Executive','Lead Developer','System Administrator','Architect', 'Other') AND c_derived__persona1 IN ('','Business Analyst','IT Manager','IT Executive','Lead Developer','System Administrator','Architect', 'Other') and
                               year (c_datecreated) = '2018'"""                     
                    , vdm='marketing_vdm_dynamic')


# In[21]:


## Contacts: Querying VDM
contacts_2019 = ds.query_JDV(""" SELECT c_address1, c_address2, c_address3, c_busphone, c_city, c_company,c_country, c_datecreated, c_derived__persona1, c_dwm_completeness_level1, c_emailaddress, 
                            CAST(SUBSTRING(c_emailaddress, LOCATE('@',c_emailaddress)+1, 255) AS STRING) AS domain_all, c_sfdcleadid,
                            c_engagement_status1,c_engagement_status_last_modified_timestamp1,c_firstname,c_lastname,c_metro_core_based_statistical_area1, c_mobilephone,c_sfdcaccountid,c_sfdccontactid,c_super_region1, c_zip_postal, contact_id, isbounced, issubscribed, status_flag,stg_del_flg,c_industry1 
                            FROM eloqua_cntcts 
                            WHERE  c_super_region1 = 'NA' AND  status_flag = 1 AND stg_del_flg = 'false' AND c_engagement_status1  IN ('Most Active','Lapsing','Lapsed' ,'Inactive', 'Pending', 'Invalid') AND c_derived__persona1 IN ('','Business Analyst','IT Manager','IT Executive','Lead Developer','System Administrator','Architect', 'Other') AND c_derived__persona1 IN ('','Business Analyst','IT Manager','IT Executive','Lead Developer','System Administrator','Architect', 'Other') and
                               year (c_datecreated) = '2019'"""                     
                    , vdm='marketing_vdm_dynamic')


# In[24]:


## unioning all the contacts
union_contacts= pd.concat([contacts_2010,contacts_2011,contacts_2012,contacts_2013,contacts_2014,contacts_2015,contacts_2016,contacts_2017,contacts_2018,contacts_2019], ignore_index=True)


# In[25]:


## Contacts:  Data Cleaning

# since status_flag is of datatype = int64, it cant be handled by the string operator which would be run next.
cols = list(union_contacts)
i = cols.index('status_flag')
cols.pop(i) 


# need to run this string operator because -> contacts table has data which does not comply with unicodes.
#                                             so certains string values has to be removed

for x in cols:
    union_contacts[x].replace({r'[^\x00-\x7F]+':''}, regex=True, inplace=True)


# In[26]:


## Contacts: Loading to RS
union_contacts.to_redshift('nafm_contacts_c','na_field_marketing',group ='na_field_marketing_rw', if_exists='replace', instance='DS')


# ### Transformations in Redshift

# #### Step 1: Deleting Existing tables

# In[29]:


ds.query_RS('drop table na_field_marketing.nafm_contacts_all' , instance='DS', option='execute')
ds.query_RS('drop table na_field_marketing.nafm_contacts_all_flagged' , instance='DS', option='execute')
ds.query_RS('drop table na_field_marketing.nafm_comm' , instance='DS', option='execute')
ds.query_RS('drop table na_field_marketing.nafm_PS' , instance='DS', option='execute')


# #### Step 2: Joining tables and transformations for base Contacts-Accounts Data

# In[30]:


## joining
ds.query_RS("""create table na_field_marketing.nafm_contacts_all as 
                (select c.*, a.*, m.*, ag.*, d.*, i.*,p.*,      
                case when c."c_engagement_status_last_modified_timestamp1" = '' then null  
                    else c."c_engagement_status_last_modified_timestamp1"   
                    end as "c_engagement_status_last_modified_timestamp2" ,
                    
                    

                  case
                    when a.sfdc_account_classification = '' or a.sfdc_account_classification is null then 'No Account Classification'
                    else a.sfdc_account_classification
                  end as sfdc_account_classification_new
                    
                    
                    
                from na_field_marketing.nafm_contacts_c c left join  na_field_marketing.nafm_accounts_c a on c.c_sfdcaccountid = a.sfdc_account_id 
                    left join na_field_marketing.metro_corr_sheet m on c.c_metro_core_based_statistical_area1 = m."derived metro" 
                    left join na_field_marketing.metro_corr_agg_unique_metros ag on ag.derived_metro_agg = c.c_metro_core_based_statistical_area1 
                    left join na_field_marketing.ps_filter_account_segment_domains d on c.domain_all = d.add_domain 
                    left join na_field_marketing.ps_filters_account_segment_si_account_ids i on c.c_sfdcaccountid = i.add_si_accountid 
                    left join na_field_marketing.ps_filters_pod_level_filter p on p.accountid_pod = c.c_sfdcaccountid)""" 
                    , instance='DS', option='execute')


# In[31]:


## transformations
ds.query_RS("""ALTER TABLE na_field_marketing.nafm_contacts_all DROP COLUMN "c_engagement_status_last_modified_timestamp1";
                
                ALTER TABLE na_field_marketing.nafm_contacts_all 
                RENAME COLUMN "c_engagement_status_last_modified_timestamp2" TO "c_engagement_status_last_modified_timestamp1"
                
                """ 
                    , instance='DS', option='execute')




ds.query_RS("""
                ALTER TABLE na_field_marketing.nafm_contacts_all DROP COLUMN sfdc_account_classification;
                ALTER TABLE na_field_marketing.nafm_contacts_all RENAME COLUMN sfdc_account_classification_new TO sfdc_account_classification; """ 
                    , instance='DS', option='execute')



## ADDING THE DATA REFRESH DATE
ds.query_RS( """ ALTER TABLE na_field_marketing.nafm_contacts_all
                 ADD COLUMN "mc_run_date" DATE
                 DEFAULT TRUNC(GETDATE()) """
                    , instance='DS', option='execute')


# In[32]:


## CHANGING DATA TYPES
    ## c_datecreated   str => date
    
ds.query_RS( """ ALTER TABLE na_field_marketing.nafm_contacts_all ADD COLUMN "c_datecreated_new" date ;
                 UPDATE na_field_marketing.nafm_contacts_all SET "c_datecreated_new" = cast ("c_datecreated" as date );
                 ALTER TABLE na_field_marketing.nafm_contacts_all DROP COLUMN "c_datecreated";
                 ALTER TABLE na_field_marketing.nafm_contacts_all RENAME COLUMN "c_datecreated_new" TO "c_datecreated" """
                    , instance='DS', option='execute')


    ## c_engagement_status_last_modified_timestamp1    str => date
    
ds.query_RS( """ ALTER TABLE na_field_marketing.nafm_contacts_all ADD COLUMN "c_engagement_status_last_modified_timestamp1_new" date ;
                 UPDATE na_field_marketing.nafm_contacts_all SET "c_engagement_status_last_modified_timestamp1_new" =  cast ("c_engagement_status_last_modified_timestamp1" as date ) ;
                 ALTER TABLE na_field_marketing.nafm_contacts_all DROP COLUMN "c_engagement_status_last_modified_timestamp1";
                 ALTER TABLE na_field_marketing.nafm_contacts_all RENAME COLUMN "c_engagement_status_last_modified_timestamp1_new" TO "c_engagement_status_last_modified_timestamp1" """
                    , instance='DS', option='execute')


# #### STEP 3: Creating flags for Public Sector

# In[33]:


ds.query_RS( """ create table na_field_marketing.nafm_contacts_all_flagged as
                    (
                    select a.*,
                     lower(trim(reverse(split_part(reverse(domain_all), '.', 1)))) as extension,
                    case when
                    extension in ('edu','gov','mil','us') or  domain_all ilike '%k12%'
                          or
                          sfdc_account_record_type_name = 'Fed/Sled'
                          or
                          sfdc_account_organization in ('Federal Civilian' ,'Intelligence','Independent/Other Agencies','State and Local',
                                  'Department of Defense','EDU', 'System Integrator')
                          or
                          add_acc_organization in ('System Integrator','Education','State and Local')
                          or
                          sfdc_account_owner_sub_region in ('FED Civ1','FED Civ2','FED DoD','FED Intel','FED SI','SLED Central','SLED East','SLED West')
                          or
                          id_acc_organization = 'System Integrator'
                          or
                          c_industry1 in ('Education', 'Government')
                          then 'NAPS'
                          else 'NAComm'
                          end as is_PublicSector

                    from na_field_marketing.nafm_contacts_all a) """
                    , instance='DS', option='execute')


# #### STEP 4: Creating new NA Comm table

# In[34]:


ds.query_RS( """ create table na_field_marketing.nafm_comm as
                    (
                    select *
                    from na_field_marketing.nafm_contacts_all_flagged
                    where is_publicsector = 'NAComm'
                    ) """
                , instance='DS', option='execute')


# #### STEP 5: creating new PS table and flags for DoD, PODs, Segments in PS Table

# In[35]:


ds.query_RS( """ create table na_field_marketing.nafm_PS as

                    (
                    select f.*,

                    -- FED DoD Logic
                    case
                      when sfdc_account_owner_sub_region = 'FED DoD'
                            or pod in ('DoD - USAF/DISA','DoD - Navy/USMC','DoD - Army/OSD/DMDC/Co-Comm')
                            or extension = 'mil'

                      then 'FED DoD'
                      else 'Non FED Dod'
                      end as is_FED_DoD,


                    -- Public Sector Segment Logic

                    case
                      when add_acc_organization = 'System Integrator' or sfdc_account_organization = 'System Integrator' or sfdc_account_owner_sub_region = 'FED SI'
                            or id_acc_organization = 'System Integrator' then 'System Integrator'
                      when add_acc_organization = 'Education' or (domain_all   ilike '%k12%')  or sfdc_account_organization = 'EDU' or extension = 'edu' then 'Education'
                      when add_acc_organization = 'State and Local' or sfdc_account_organization = 'State and Local' then 'State and Local'
                      else 'US Federal Government'
                      end as PS_Segment,


                    -- POD CORRECT

                    case
                      when sfdc_account_owner_sub_region = 'SLED East' then 'SLED East'
                      when sfdc_account_owner_sub_region = 'SLED West' then 'SLED West'
                      when sfdc_account_owner_sub_region = 'SLED Central' then 'SLED Central'
                      when sfdc_account_owner_sub_region = 'FED SI' or sfdc_account_organization = 'System Integrator' OR PS_Segment = 'System Integrator' then 'SI'
                      when sfdc_account_owner_sub_region = 'FED Intel' or sfdc_account_organization = 'Intelligence' then 'Intel'
                      when sfdc_account_sub_organization in ( 'Department of the Air Force (USAF)','Defense Information Systems Agency (DISA)') or pod = 'DoD - USAF/DISA'
                                      then 'DoD - USAF/DISA'
                      when sfdc_account_sub_organization in ('Department of the Navy (USN)','United States Marine Corps (USMC)' ) or pod = 'DoD - Navy/USMC'  then 'DoD - Navy/USMC'

                      when sfdc_account_sub_organization in ('Office of the Secretary of Defense (OSD)','Department of the Army (USA)','Missile Defense Agency (MDA)',
                                     'Unified Combatant Commands (COCOMs)') or pod = 'DoD - Army/OSD/DMDC/Co-Comm'
                                      then 'DoD - Army/OSD/DMDC/Co-Comm'

                      when sfdc_account_sub_organization in ('Department of Justice (DOJ)','Department of Homeland Security (DHS)','Department of State (DOS)') or
                                      pod = 'Civ - Law & Justice'

                                    then 'Civ - Law & Justice'



                      when sfdc_account_sub_organization in ('Environmental Protection Agency (EPA)','Department of Energy (DOE)','General Services Administration(GSA)',
                                     'National Aeronautics and Space Administration (NASA)','Department of Transportation (DOT)','Department of Agriculture (USDA)',
                                     'Department of Labor (DOL)','Federal Communications Commission (FCC)','Consumer Product Safety Commission (CPSC)','Federal Trade Commission (FTC)',
                                      'Small Business Administration (SBA)','National Science Foundation (NSF)','Library of Congress','U.S. Senate','U.S. House of Representative',
                                     'Government Printing Office (GPO)','Peace Corps') or pod = 'Civ - Independent'
                                      then 'Civ - Independent'
                      when sfdc_account_sub_organization in ('Department of Health & Human Services (HHS)','Department of the Interior (DOI)','Department of Veterans Affairs (VA)')
                                      or pod = 'Civ - Health'
                                      then 'Civ - Health'
                      when (sfdc_account_sub_organization in ('Department of Treasury (TRE)','Federal Reserve Bank','Commodity Futures Trading Commission (CFTC)',
                                      'Pension Benefit Guaranty Corporation','Department of Housing & Urban Development (HUD)','Federal Retirement Thrift Investment Board',
                                      'Export-Import Bank of the United States','Department of Commerce (DOC)') or pod = 'Civ - Financials')

                                      then 'Civ - Financials'
                      else 'No POD Classification'
                      end as PS_POD


                    from na_field_marketing.nafm_contacts_all_flagged f
                    where is_publicsector = 'NAPS'
                    ) """
                , instance='DS', option='execute')

