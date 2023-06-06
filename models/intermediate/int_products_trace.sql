
with
-- select from stage inv
inventory as (
    select *
    from {{ ref('stg_inventory_trace') }}
),

-- select from stage inv types
inventorytypes as (
    select *
    from {{ ref('stg_inventorytypes_trace') }}
),

-- log, already filtered for most recent values
inventorylog as (
    select *
    from {{ ref('stg_inventorylog_trace') }}
),

-- join tables
-- equivalent to track_trace_products_raw
joined_and_ranked as (
    select

        -- from inv
        inventory.org,
        inventory.orgid,
        inventory.location,
        COALESCE(left(inventory.productname, 255), inventory.id) as productname,
        case when inventory.requiresweighing is null then 0 else inventory.requiresweighing END as requires_weighing,
        inventory.ismedicated as isthc,
        inventory.usableweight as useable,
        inventory.sessiontime,
        inventory.sessiontime_timestamp,
        inventory.id as productid,
        inventory.packageweight,
        inventory.strain,
        inventory.straintype,

        -- from inv type
        inventorytypes.name as category,
        inventorytypes.name as inventorytype,

        -- cann 2.1 addition DEI-200
        -- from log
        inventorylog.location as manufacturer_location,

        -- constants
        current_timestamp() as extract_date,
        ROW_NUMBER() OVER (PARTITION BY inventory.productname, inventory.org, inventory.location, inventorytypes.name, inventory.id ORDER BY inventory.sessiontime DESC ) rank1

    from inventory
    left join inventorylog on inventorylog.id = inventory.id
    left join inventorytypes on inventorytypes.id = inventory.inventorytypeid
),

-- ranking, if necessary, is needed because inventory is not unique
second_rank_rename as (
    select distinct
        productid as bt_legacy_product_id,
        productid,
        org,
        location,
        manufacturer_location,
        left(trim(productname), 255) as product_name,
        category as category,
        category as updated_cat,
        inventorytype as inventorytype,
        strain as product_strain,
        isthc as isthc,
        straintype as straintype,
        requires_weighing as requires_weighing,
        useable as useable,
        null as unit_type,
        sessiontime,
        sessiontime_timestamp,
        ROW_NUMBER() OVER (PARTITION BY org, inventorytype, bt_legacy_product_id ORDER BY sessiontime DESC) rank2,
        -- DEI-236
        current_timestamp() as extract_date

    from joined_and_ranked
    where rank1 = 1
)

-- final select with filter for rank2
select
    *
from second_rank_rename
-- this rank isn't necessary
--where rank2 = 1
