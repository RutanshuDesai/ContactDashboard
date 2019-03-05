-- First get the refreshed data from VDM using Python and push to Redshift.



-- deleting old joined tables
drop table rhdsrs.na_field_marketing.nafm_contacts_all;
drop table rhdsrs.na_field_marketing.nafm_contacts_all_flagged;
drop table rhdsrs.na_field_marketing.nafm_comm;
drop table rhdsrs.na_field_marketing.nafm_PS;




-- check for the old counts
  -- accounts
select count(*) from na_field_marketing.nafm_accounts_c;  -- 720k, 725k, 752k
--contacts
select count( distinct c_emailaddress) from na_field_marketing.nafm_contacts_c --2,024,391, 2,044,718
-- NA Comm
select count( distinct c_emailaddress) from na_field_marketing.nafm_comm -- 1,733,701
-- NA PS
select count( distinct c_emailaddress) from na_field_marketing.nafm_PS --306,634, 307k, 311k
-- NA contacts all
select count( distinct c_emailaddress) from na_field_marketing.nafm_contacts_all
-- NA flagged contacts all
select count( distinct c_emailaddress) from na_field_marketing.nafm_contacts_all_flagged






-- STEP 1: joining all the tables for NAFM schema
create table na_field_marketing.nafm_contacts_all as (
select c.*, a.*, m.*, ag.*, d.*, i.*,p.*,
      case
          when c."c_engagement_status_last_modified_timestamp1" = '' then null
          else c."c_engagement_status_last_modified_timestamp1"
      end as "c_engagement_status_last_modified_timestamp2"




from na_field_marketing.nafm_contacts_c c
left join  na_field_marketing.nafm_accounts_c a on c.c_sfdcaccountid = a.sfdc_account_id
 left join na_field_marketing.metro_corr_sheet m on c.c_metro_core_based_statistical_area1 = m."derived metro"
 left join na_field_marketing.metro_corr_agg_unique_metros ag on ag.derived_metro_agg = c.c_metro_core_based_statistical_area1
 left join na_field_marketing.ps_filter_account_segment_domains d on c.domain_all = d.add_domain
 left join na_field_marketing.ps_filters_account_segment_si_account_ids i on c.c_sfdcaccountid = i.add_si_accountid
 left join na_field_marketing.ps_filters_pod_level_filter p on p.accountid_pod = c.c_sfdcaccountid)
 ;

ALTER TABLE na_field_marketing.nafm_contacts_all DROP COLUMN "c_engagement_status_last_modified_timestamp1";
ALTER TABLE na_field_marketing.nafm_contacts_all RENAME COLUMN "c_engagement_status_last_modified_timestamp2" TO "c_engagement_status_last_modified_timestamp1";






 -- ADDING THE DATA REFRESH DATE
 ALTER TABLE na_field_marketing.nafm_contacts_all
 ADD COLUMN "mc_run_date" DATE
 DEFAULT TRUNC(GETDATE());


 -- CHANGING DATA TYPES
    -- c_datecreated   str => date

ALTER TABLE na_field_marketing.nafm_contacts_all ADD COLUMN "c_datecreated_new" date ;
UPDATE na_field_marketing.nafm_contacts_all SET "c_datecreated_new" = cast ("c_datecreated" as date );
ALTER TABLE na_field_marketing.nafm_contacts_all DROP COLUMN "c_datecreated";
ALTER TABLE na_field_marketing.nafm_contacts_all RENAME COLUMN "c_datecreated_new" TO "c_datecreated";





    -- c_engagement_status_last_modified_timestamp1    str => date

ALTER TABLE na_field_marketing.nafm_contacts_all ADD COLUMN "c_engagement_status_last_modified_timestamp1_new" date ;
UPDATE na_field_marketing.nafm_contacts_all SET "c_engagement_status_last_modified_timestamp1_new" =  cast ("c_engagement_status_last_modified_timestamp1" as date ) ;
ALTER TABLE na_field_marketing.nafm_contacts_all DROP COLUMN "c_engagement_status_last_modified_timestamp1";
ALTER TABLE na_field_marketing.nafm_contacts_all RENAME COLUMN "c_engagement_status_last_modified_timestamp1_new" TO "c_engagement_status_last_modified_timestamp1";







-- STEP 2: creating flags for Public Sector

create table na_field_marketing.nafm_contacts_all_flagged as
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

      then 1

      else 0

      end as is_PublicSector

from na_field_marketing.nafm_contacts_all a);




-- STEP 3: creating new NA Comm table
create table na_field_marketing.nafm_comm as
(
select *
from na_field_marketing.nafm_contacts_all_flagged
where is_publicsector = 0
);






-- STEP 4: creating new PS table and flags for DoD, PODs, Segments in PS Table
create table na_field_marketing.nafm_PS as

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
where is_publicsector = 1
);



