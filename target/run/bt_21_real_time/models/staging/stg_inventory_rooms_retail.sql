
  
    

        create or replace transient table PC_FIVETRAN_DB.dbt_real_time.stg_inventory_rooms_retail  as
        (with 

selected as(
    select
        org,
        location,
        id,
        coalesce(regexp_replace(roomname, '([^[:ascii:]])', ''), 'NA') as room,
        current_timestamp as extract_date
    from postgres_cann_replication_public.inventoryrooms_raw where _fivetran_deleted = false
)

select * from selected
        );
      
  