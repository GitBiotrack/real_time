with
-- select from stage locations
locations as (
    select
       *
    from {{ ref('stg_locations_retail') }}
),

-- select from stage org
org as (
    select *
    from {{ ref('stg_org_retail') }}
),

-- select from location type look up table
location_type_desc as (
    select *
    from {{ ref('stg_location_type_desc') }}
),

-- join above tables
join_tables as (
    select

        -- placeholder for seq id created in DDL
        -- this is not used downstream and the name is misleading (you'd think it'd be the concatenation of org, loc, etc but its not). It can be removed.
        null as dispensaryid,

        -- from locations
        locations.org,
        locations.location,
        locations.location_name,
        locations.licensenum,
        locations.address1,
        locations.address2,
        locations.city,
        locations.state,
        locations.zip,
        locations.phone,
        locations.locationtype,
        locations.med,
        locations.rec,
        CONCAT(10, '~', locations.org, '~', locations.location) AS dispensary_primary_key,

        -- from org
        org.masterorg,
        org.orgname,

        -- from location_type_desc
        -- column name modified for DEI-252
        location_type_desc.description as location_type_desc,

        -- seq id -- not needed
        row_number() over (order by (select null)) as sequentialid --hacky solution to get sequentialid

    from locations
    left join org
        on org.orgid = locations.org
    left join location_type_desc
        -- join columns modified for DEI-252
        on location_type_desc.state = locations.state
        and location_type_desc.locationtype = locations.locationtype
),

renamed as (
    select

        -- simple selects
        location,
        location_name,
        licensenum,
        address1,
        address2,
        city,
        state,
        zip,
        phone,
        locationtype,
        med,
        rec::smallint,
        masterorg,
        orgname,
        org,
        location_type_desc,
        dispensaryid::int,
        sequentialid,

        -- constants
        current_timestamp() as extract_date,
        current_timestamp() as read_time,
        current_timestamp() as load_time,
        10 as source,
        null as active_cann_client,
        null as data_rights,

        -- combinations
        CONCAT(10, '~', org, '~', location) AS dispensary_primary_key

    from join_tables

)

-- final select
select * from renamed
