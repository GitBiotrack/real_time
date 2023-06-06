with 
-- select customers from the nm trace schema

selected as  (
    select
        orgid as orgid,
        masterorg as masterorg,
        orgname as orgname,
        contact_name as time_zone

    from postgres_cann_replication_public.org where _fivetran_deleted = false
)

select * from selected