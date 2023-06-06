with 
-- select customers from the nm trace schema

selected as  (
    select
            id as vendorid,
            org,
            regexp_replace( LEFT(name, 100), '([^[:ascii:]])', '') as vendorname,
            name as vendor_name,
            current_timestamp() as extract_date
    from postgres_cann_replication_public.vendors_raw where _fivetran_deleted = false
)
select * from selected