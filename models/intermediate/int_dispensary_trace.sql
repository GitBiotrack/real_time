with
-- select staged locations
locations as (
    select *
    from {{ ref('stg_locations_trace') }}
),

-- select seed org -- this should be changed to the stage table
org as (
    select *
    from {{ ref('stg_org_retail') }}
),

-- join tables
joined as (
    SELECT

        -- from locations
        locations.org,
        locations.sourceorgid,
        locations.location,
        locations.licensenum,
        locations.name as location_name,
        left(locations.address1, 49) as address1,
        left(locations.address2, 49) as address2,
        locations.city as city,
        locations.state as state,
        locations.zip as zip,
        left(locations.phone, 20) as phone,
        locations.locationtype as locationtype,
        locations.medical as med,
        case when locations.medical = false then true else false end as rec,
        '" + str(extract_date) + "' as date, -- this is old py code
        locations.location_type_desc_id,
        -- locationtype is the same as whats in location_type_desc, so no need to join with that table
        locations.locationtype as location_type_desc,
        10 as source,
        -- DEI-246 legacy location id
        locations.legacy_2_0_location,

        -- from org
        org.masterorg as masterorg,
        org.orgname as orgname,

        -- DEI-236
        current_timestamp() as extract_date

    from locations
    left join org
        -- org is a constant here but this join needs to happen
        on org.orgid = locations.org
)

-- final selection
select *
from joined
