with
-- select stage sales
sales as (
    select *
    from {{ ref('stg_sales_trace') }}
),

-- select stage inventory
inventory as (
    select *
    from {{ ref('stg_inventory_trace') }}
),

-- select stage inv types
inventorytypes as (
    select *
    from {{ ref('stg_inventorytypes_trace') }}
),

-- select staged refunds, which is just select * from sales where refunded = 1
refunds as (
    select *
    from {{ ref('stg_refunds_trace') }}
),

/*
This is essentially a self join to update refunded transactions
A new column is created with the updated refund flag when applicable
 */
update_refunds as (
    select
        sales.*,
        -- when refund occured
        case when refunds.refunded = 1
            -- update column to indicate a refund
            then 1
            -- or leave it as is
            else sales.refunded
        -- name it as a new column
        end as refunded_updated
    from sales
    left join refunds
        -- join reference id on original id
        on refunds.transactionid_original = sales.transactionid
        -- location is unique, dont need to join on org
        and refunds.location = sales.location
),

-- join sales with inventory tables
join_inventory as (
    select
        -- from sales
        update_refunds.org,
        update_refunds.location,
        update_refunds.saleid,
        update_refunds.ticketid,
        update_refunds.sessiontime_timestamp - interval '6 hours' as sessiontime_timestamp_tz_converted,
        update_refunds.orgid,
        update_refunds.weight,
        update_refunds.price,
        update_refunds.transactionid,
        update_refunds.item as strain,
        update_refunds.sessiontime,
        update_refunds.sessiontime_timestamp,
        COALESCE(update_refunds.customerid, 'NA') as customerid,
        update_refunds.deleted,
        update_refunds.refunded_updated as refunded,

        -- from inventory
        inventory.productid,
        COALESCE(inventory.strain, inventory.id::text) as prodstrain,
        inventory.straintype,
        inventory.productname,

        -- from inventory types
        inventorytypes.name as tracecat,
        inventorytypes.name,

        -- constants
        current_timestamp() as extract_date,

        -- this rank, if necessary, is caused by the inventory  not being unique. We should not use it.
        ROW_NUMBER() OVER (PARTITION BY
                               update_refunds.saleid,
                               update_refunds.org,
                               update_refunds.customerid,
                               update_refunds.location,
                               update_refunds.transactionid,
                               update_refunds.price,
                               update_refunds.item,
                               inventory.strain
                           ORDER BY update_refunds.sessiontime DESC) rank

    from update_refunds
    left join inventory on inventory.id = update_refunds.inventoryid
    left join  inventorytypes on inventorytypes.id = inventory.inventorytypeid
)

-- final select
select *
from join_inventory
where rank = 1
