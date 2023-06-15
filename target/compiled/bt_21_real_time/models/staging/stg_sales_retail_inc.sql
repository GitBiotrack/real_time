with 
-- select customers from the nm trace schema
selected as  (
    select
        s.org,
        location,
        -- there is confusion about which price column to use. either price_adjusted_for_ticket_discounts or price_post_discount
        price - price_adjusted_for_ticket_discounts as discountamt, --price_adjusted_for_ticket_discounts
        -- there is confusion about which price column to use. either price_adjusted_for_ticket_discounts or price_post_discount
        price_adjusted_for_ticket_discounts as price_post_discount,
        ticketid,
        refticketid as refund_ticketid,
        replication_val,
        datetime,
        to_timestamp(datetime) as datetime_timestamp,
        convert_timezone('UTC', o.contact_name, to_timestamp(datetime)) as datetime_timestamp_tz,
        o.contact_name as time_zone,
        coalesce( REGEXP_REPLACE( LEFT(strain, 100), '([^[:ascii:]])', ''), id :: text) as strain,
        coalesce(price, 0) as price,
        weight as weight,
        (weighheavy :: smallint):: int :: boolean as weighheavy,
        LEFT(pricepoint, 100) as pricepoint,
        id as saleid,
        productid as productid,
        -- absolutely not coalescing this this with ticketid, leave transactionid as is
        --coalesce(transactionid, ticketid :: float) as transactionid,
        transactionid as transactionid,
        transactionid_original,
        inventoryid,
        customerid,
        taxcat,
        -- so far no refunds/deletes are null, for trace that is the default value
        refunded,
        deleted,
        tax_collected as pretaxprice,
        -- DEI-23423
        tax_collected_excise,
        -- DEI-236
        current_timestamp() as extract_date,
        s._fivetran_synced as last_sync
    from postgres_cann_replication_public.sales_raw s join postgres_cann_replication_public.org o on s.org = o.orgid 
    where s._fivetran_deleted = false and to_timestamp(datetime) > GETDATE() - interval '1095 days' 
    and s._fivetran_synced > (select max(last_sync) from stg_sales_retail_inc)
)
select * from selected