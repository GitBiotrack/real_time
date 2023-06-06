with 

selected as  (
    select
        org,
        location,
        logid,
        id as inventoryid,
        newweight as new_weight,
        newweight as weight,
        -- DEI-236
        sessiontime,
        to_timestamp(sessiontime) as sessiontime_timestamp,
        coalesce(newweight, 0) as quantity,
        roomdata,
        -- DEI-236
        current_timestamp() as extract_date,
        -- DEI-280
        max(sessiontime) over (partition by org, location, id) as maxsessiontime,
        max(logid) over (partition by org, location, id) as maxlogid

    from postgres_cann_replication_public.inventorylog_raw where _fivetran_deleted = false
)

select *
from selected
where sessiontime = maxsessiontime
    and logid = maxlogid
