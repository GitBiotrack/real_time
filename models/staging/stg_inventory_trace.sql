with 
-- select customers from the nm trace schema

selected as (
    select
        org,
        orgid,
        location,
        productname,
        id,
        id as productid,
        inventorytype as inventorytypeid,
        requiresweighing,
        ismedicated,
        usableweight,
        strain,
        straintype,
        packageweight,
        sessiontime,
        to_timestamp(sessiontime) as sessiontime_timestamp,

        current_timestamp() as extract_date

    from postgres_cann_replication_public.bmsi_inventory_raw where _fivetran_deleted = false
)

--final selection
select *
from selected
