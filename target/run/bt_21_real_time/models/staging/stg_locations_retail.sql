
  
    

        create or replace transient table PC_FIVETRAN_DB.dbt_real_time.stg_locations_retail  as
        (with 
-- select customers from the nm trace schema

selected as  (
    select
        org,
        id as location,
        name as location_name,
        licensenum,
        left(address1, 49) as address1,
        left(address2, 49) as address2,
        city,
        upper(state) as state,
        zip,
        left(phone, 20) as phone,
        locationtype,
        medical as med,
        case when medical = 1 then 0 else 1 end as rec,
        current_timestamp() as extract_date,
        coalesce(id, 0) as locid,
        LEFT(name, 50) as locname,
        current_timestamp() as date
    from postgres_cann_replication_public.locations_raw where _fivetran_deleted = false
)

select * from selected
        );
      
  