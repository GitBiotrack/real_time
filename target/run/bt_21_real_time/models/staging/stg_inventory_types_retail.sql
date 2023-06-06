
  
    

        create or replace transient table PC_FIVETRAN_DB.dbt_real_time.stg_inventory_types_retail  as
        (with 
-- select customers from the nm trace schema

selected as (
    select
        org,
        id,
        regexp_replace(name, '([^[:ascii:]])', '') as inventorytype,
        -- DEI-236
        current_timestamp() as extract_date
    from postgres_cann_replication_public.inventorytypes_raw where _fivetran_deleted = false
)

select * from selected
        );
      
  