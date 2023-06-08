with 
-- select for stage products logs
products_logs as (
   select * from PC_FIVETRAN_DB.dbt_real_time.stg_productslog_retail
),

-- select from stage products
products as (
   select * from PC_FIVETRAN_DB.dbt_real_time.stg_products_retail
),

-- select from stage prod cat
product_categories as (
   select * from PC_FIVETRAN_DB.dbt_real_time.stg_product_categories_retail
),

-- select from stage inv types
inventory_types as (
   select * from PC_FIVETRAN_DB.dbt_real_time.stg_inventory_types_retail
),

-- select from stage inv
inventory as (
    select * from PC_FIVETRAN_DB.dbt_real_time.stg_inventory_retail
),

-- select from stage vendors
vendors as (
    select * from PC_FIVETRAN_DB.dbt_real_time.stg_vendors_retail
),

-- DEI-263 join fact tables
-- make products the dominant table in the left join. Coalesce the two tables favoring products over productlogs
join_facts as (
    select
        --PRODUCTS px
        products.productid,
        products.productid as bt_legacy_product_id,
        products.manufacturer,
        products.producer, 
        products.prod_desc,
        products.useable,  
        products.org,
        products.location,
        products.ismedicated,
        -- DEI-236 - since productid comes from products, the created col (date col) for the recency check should apply to products
        -- originally the date col for this table was products_logs.sessiontime
        -- its more sound to keep the date col from the table where productid is sourced from
        products.created,

        -- DEI-263 coalesce
        coalesce(products.product_categories_id, products_logs.productcategory) as product_categories_id,
        -- products.name was originally defined but never used down stream. 
        -- products_logs.productname has a higher fillrate
        coalesce(products.name, products_logs.productname) as product_name,
        coalesce(products.applymemberdiscount, products_logs.applymemberdiscount) as applymemberdiscount,
        -- no need to coalesce, in products nulls are coerced to false
        products.requires_weighing,
        coalesce(products.strain, products_logs.strain) as product_strain,
        coalesce(products.inventorytype, products_logs.inventorytype) as inventorytype,
        coalesce(products.defaultvendor, products_logs.defaultvendor) as defaultvendor,
        coalesce(products.costperunit, products_logs.costperunit) as costperunit

    -- DEI-263 products is now dominant in the left join
    from products
    left join products_logs
        on products_logs.productid = products.productid 
        and products_logs.org = products.org
),

-- join tables 
join_dimensions as (

    select distinct 
        -- from join_facts
        join_facts.product_name,
        join_facts.applymemberdiscount, 
        Coalesce(join_facts.product_strain, inventory.strain) as product_strain,
        join_facts.ismedicated, 
        join_facts.requires_weighing,
        join_facts.org,
        join_facts.location,
        join_facts.productid,
        join_facts.bt_legacy_product_id,
        join_facts.manufacturer,
        coalesce(join_facts.producer, inventory.producer) as producer,
        join_facts.prod_desc,
        join_facts.useable,  
        -- DEI-236 - since productid comes from products, the created col (date col) for the recency check should apply to products
        -- originally the date col for this table was products_logs.sessiontime
        -- its more sound to keep the date col from the table where productid is sourced from
        join_facts.created,

        -- product_categories
        product_categories.tracecat as category,
        product_categories.tracecat as updated_cat,

        --INVENTORYTYPES i
        inventory_types.inventorytype,

        --INVENTORY ii
        inventory.straintype,

        --VENDORS v
        vendors.vendor_name as vendor,
        vendors.vendorid,
        vendors.vendorname,

        ---COMBO
        COALESCE(join_facts.costperunit, inventory.cost_per_unit) as costperunit,

        --NOT NEEDED or appears to be empty and has no previous reference
        NULL as exp_date,
        NULL as packaged_date,
        NULL as packaged_weight,
        NULL as unit_type,
        10 as source,
        CONCAT(10, '~', NULL,'~', join_facts.productid, '~',product_categories.tracecat) AS product_primary_key, --left out org for the moment that's what the NULL is
        -- DEI-236
        current_timestamp() as extract_date

    from join_facts
    left join product_categories 
        on product_categories.product_categories_id = join_facts.product_categories_id 
        and product_categories.org = join_facts.org
    left join inventory_types
        on inventory_types.id = join_facts.inventorytype
        and inventory_types.org = join_facts.org
    left join inventory
        on inventory.productid = join_facts.productid
        and inventory.org = join_facts.org
    left join vendors
        on join_facts.defaultvendor = vendors.vendorid
        and join_facts.org = vendors.org
),

-- ranking, if necessary, only occurs because inventory is not unique.  
ranked_once as (
    select 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY 
                product_name, 
                org, 
                location, 
                category, 
                productid 
            ORDER BY 
                created 
            DESC) rank_1  
    from join_dimensions
),

--rank two is like rank one but doesn't include productname or location
ranked_twice as (
    select 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY 
                org, 
                --category, 
                bt_legacy_product_id --aka id
            ORDER BY 
                created 
            DESC) rank_2  
    from ranked_once where rank_1 = 1
)

-- final select with filter for rank2
select 
    *,
    row_number() over (order by (select null)) as sequentialid -- not necessary
from ranked_twice 
where rank_2 = 1