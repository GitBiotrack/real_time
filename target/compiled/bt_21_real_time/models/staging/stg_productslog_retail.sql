with 
-- select customers from the nm trace schema

selected as  (
    select
        org,
		logid,
        -- window fun to get the max logid per product-org to filter on later
        -- this is accurate but this table can be removed.
        -- see confluence doc "BT 1.0 source data" section product/inventory > inventory for an explanation
        max(logid) over (partition by id, org) as max_logid,
        id as productid,
		coalesce(regexp_replace(name, '([^[:ascii:]])', ''), id::text) as productname,
        name,
		applymemberdiscount,
		regexp_replace(strain, '([^[:ascii:]])', '') as strain,
		regexp_replace(pricepoint, '([^[:ascii:]])', '') as pricepoint,
		ismedicated,
		location,
        sessiontime,
        -- DEI-236
        to_timestamp(sessiontime) as sessiontime_timestamp,
		productcategory,
		inventorytype,
		taxcategory,
		defaultvendor,
		costperunit,
        sharedcategories,
        -- DEI-236
        current_timestamp() as extract_date

	from postgres_cann_replication_public.productslog_raw where _fivetran_deleted = false
)

select * from selected where logid = max_logid