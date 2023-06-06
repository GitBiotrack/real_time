with
-- select int products
products as (
    select *
    from {{ ref('int_products_retail') }}
),

-- select int inventory
inventory as (
    select *
    from {{ ref('int_inventory_retail') }}
),

-- join tables
join_transform as (
    select

        -- from products
        products.costperunit as product_cost,
        products.org,
        products.bt_legacy_product_id,
        products.productid as productid,

        -- additional product info for pos prod inv
        COALESCE(products.product_name,inventory.strain) as product_name,
        COALESCE(products.product_strain,inventory.strain) as product_strain ,
        COALESCE(products.straintype, inventory.straintype) as straintype,
        products.manufacturer,
        COALESCE(products.producer, inventory.producer) as producer,
        products.vendor,
        products.packaged_weight,
        products.category,
        products.updated_cat,
        products.inventorytype,
        -- from inventory
        inventory.location as location,
        inventory.inventoryid,
        coalesce(inventory.weight, 0) as quantity,
        coalesce(inventory.room, 'NA') as room,
        -- DEI-236 - making transaction_time sessiontime to match up stream names
        inventory.sessiontime,
        inventory.sessiontime_timestamp,
        coalesce(inventory.expiration :: date :: date, '12/31/9999') as expdate,

        -- constants
        current_timestamp() as modified_date,
        '10' as source,

        -- concat values between products and inventory
        CONCAT(10, '~', products.org, '~', inventory.location, '~', inventory.inventoryid, '~', inventory.room, '~', products.bt_legacy_product_id) as product_inventory_primary_key,

        -- addition for cann 2.1 poc
        concat('10', '~', products.org, '~', products.location, '~', 'xxxx') as dispensary_id

    from products
    join inventory
        -- must use these three for a unique join
        on products.productid = inventory.productid
        and inventory.org = products.org
),

ranked as (
    select
        dispensary_id,
        productid,
        quantity,
        room,
        sessiontime,
        sessiontime_timestamp,
        expdate,
        product_cost,
        modified_date,
        org,
        location,
        inventoryid,
        source,
        product_name,
        product_strain,
        straintype,
        manufacturer,
        producer,
        vendor,
        packaged_weight,
        category,
        updated_cat,
        inventorytype,

        -- this rank should not be necessary. If it is something is wrong
        ROW_NUMBER() OVER (PARTITION BY org, room, location, inventoryid, bt_legacy_product_id  ORDER BY sessiontime DESC)  as rank,
        -- DEI-236
        current_timestamp() as extract_date
    from join_transform
)

select
    *,
    row_number() over (order by (select null)) as sequentialid
from ranked
where rank = 1
