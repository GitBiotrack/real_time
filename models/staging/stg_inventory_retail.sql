with 

selected as (
    select
        regexp_replace(strain, '([^[:ascii:]])', '') as strain,
		coalesce(location, 0) as location,
		--coalesce (productid,-1)  as product_id,
		coalesce (productid,-1)  as productid,
        id,
        org,
        to_timestamp(expiration) as expiration,
        regexp_replace(straintype, '([^[:ascii:]])', '') as straintype,
        cost_per_unit,
        cost_per_unit as costperunit,
        pricein,
        vendorid,
        -- DEI-236
        sessiontime,
        to_timestamp(sessiontime) as sessiontime_timestamp,
        -- DEI-236
        current_timestamp() as extract_date,
        producer,
        inventorytype

    from postgres_cann_replication_public.inventory_raw where _fivetran_deleted = false
)

select * from selected
