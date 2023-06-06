with 
-- select customers from the nm trace schema

selected as  (
    -- get distinct values
    -- it should be distinct?
    select distinct
        org,
        orgid,
        id as saleid,
        ticketid,
        inventoryid,
        sessiontime,
        to_timestamp(sessiontime) as sessiontime_timestamp,
        -- in quotes because of key word
        location,
        price,
        weight,
        transactionid,
        transactionid_original,
        item,
        -- non-refunds are null
        coalesce(refunded, 0) as refunded,
        -- non-deletes are null
        coalesce(deleted, 0) as deleted,
        customerid,
        -- DEI-236
        current_timestamp() as extract_date

    from postgres_cann_replication_public.bmsi_dispensing_raw where _fivetran_deleted = false and to_timestamp(sessiontime) > GETDATE() - interval '1095 days'
)

-- final selection
select * from selected
