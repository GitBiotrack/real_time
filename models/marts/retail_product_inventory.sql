
with
-- select in prod inv
product_inventory as (
    select *
    from {{ ref('int_product_inventory_retail') }}
),

-- name and fill in with null values
renamed as (
    select
        product_name,
        product_strain,
        straintype,
        manufacturer,
        producer,
        vendor,
        packaged_weight,
        category,
        category as product_category,
        updated_cat,
        inventorytype,
        productid,
        productid as legacy_product_id,
        org as lookedup_org,
        location as lookedup_location,
        quantity,
        -- DEI-236 adding this column for consistency and testing
        sessiontime,
        sessiontime_timestamp,
        sessiontime_timestamp as current_inventory_date,
        product_cost,
        expdate as inventory_expdate,
        room as inventory_room,
        extract(year from to_timestamp(sessiontime_timestamp)) as year,
        extract(month from to_timestamp(sessiontime_timestamp)) as month,

        -- DEI-223
        current_timestamp() as extract_date,

        -- cann 2.1 addition
        concat('10', '~', org, '~', location, '~', 'xxxx') as dispensary_id,
        -- row number to get sequential id like cann 2.0. See DEI-205
        concat('10', '~', org, '~', location, '~', productid, '~', row_number() over (order by (select null))) as guid_product_inventory,

        -- nulls for cann 2.1 poc
        null as resolved_product_name,
        null as product_type,
        null as product_form,
        null as product_subcategory,
        null as product_subcategory1,
        null as product_subcategory2,
        null as resolved_strain_id,
        null as resolved_strain,
        null as resolved_straintype,
        null as resolved_manufacturer

    from product_inventory
)

-- final select
select * from renamed
