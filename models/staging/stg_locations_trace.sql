with 
-- select customers from the nm trace schema

selected as  (
    select
        -- constant for org
        org,
        id as location,
        orgid as sourceorgid,
        name,
        address1,
        address2,
        city,
        state,
        zip,
        phone,
        case when locationtype in ( 1, 2, 3) then 'Grow' When locationtype in ( 4, 5, 6) Then 'Processor' when locationtype = 7 then 'Processor' when locationtype in( 8, 10, 12) then 'Dispensary - Medical' ELSE '0' END as locationtype,
        licensenum as licensenum,  case when locationtype in ( 1, 2, 3) then 1 When locationtype in ( 4, 5, 6) Then 4 when locationtype = 7 then 4 when locationtype in( 8, 10, 12) then 8 ELSE -1 END as location_type_desc_id ,true as
        medical,
        false as recreational,
        -- DEI-236
        current_timestamp() as extract_date,
        -- DEI-246 legacy location id
        orgid || id as legacy_2_0_location

    from postgres_cann_replication_public.bmsi_locations_raw where _fivetran_deleted = false
)

-- final selection
select * from selected
