with 
-- select customers from the nm trace schema

selected as  (
    select
        distinct
        org,
        -- in quotations due since these are key words
        id,
        name,
        -- DEI-236
        current_timestamp() as extract_date

    from postgres_cann_replication_public.bmsi_inventorytypes_raw where _fivetran_deleted = false
)

-- final selection
select *
from selected
