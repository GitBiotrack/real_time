
  
    

        create or replace transient table PC_FIVETRAN_DB.dbt_real_time.retail_dispensary_all_views  as
        (with pos_transaction_detail as (
    select *
    from PC_FIVETRAN_DB.dbt_real_time.pos_transaction_detail
),

select_and_filter as (
    select
        distinct dispensary_id,
        organization_name::string as organization_name,
        trim(dispensary_name) AS dispensary_name,
        dispensary_store_size::string as dispensary_store_size,
        dispensary_license_type::string as dispensary_license_type,
        dispensary_zip3::string as dispensary_zip3,
        dispensary_zip5,
        dispensary_latitude::double as dispensary_latitude,
        dispensary_longitude::double as dispensary_longitude,
        dispensary_state,
        null as dispensary_state_name,
        source_dispensary_id::integer as source_dispensary_id,
        source_dispensary_location_id::integer as source_dispensary_location_id,
        source_dispensary_org_id::integer as source_dispensary_org_id,
        source_master_org_id::integer as source_master_org_id,
        -- DEI-223
        current_timestamp() as extract_date

    from pos_transaction_detail
    where left(dispensary_id, 9) != '10~100047'
        and lower(left(dispensary_license_type::string,5)) = 'dispe'
        and to_date(datetime_timestamp) = (select max(to_date(datetime_timestamp)) from POS_TRANSACTION_DETAIL)
)

select * from select_and_filter
        );
      
  