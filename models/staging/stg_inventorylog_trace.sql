with 
-- select customers from the nm trace schema

selected as  (
    select
        -- DEI-223 must be distinct as below does not fully dedup
        distinct
        org,
        location,
        id,
        -- DEI-223 dedup by taking the first ever location of a product
        created,
        min(created) over (partition by id) as min_created,
        -- DEI-236
        current_timestamp() as extract_date,
        -- DEI-236
        sessiontime,
        to_timestamp(sessiontime) as sessiontime_timestamp

    from postgres_cann_replication_public.log_bmsi_inventory_raw where _fivetran_deleted = false
)

-- final select
select *
from selected
-- take the original row of this inventory id
where created = min_created
