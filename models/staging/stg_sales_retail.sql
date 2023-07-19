with 
-- select customers from the nm trace schema
selected as  (
    select
        s.org,
        s.location,
        -- there is confusion about which price column to use. either price_adjusted_for_ticket_discounts or price_post_discount
        s.price - s.price_adjusted_for_ticket_discounts as discountamt, --price_adjusted_for_ticket_discounts
        -- there is confusion about which price column to use. either price_adjusted_for_ticket_discounts or price_post_discount
        s.price_adjusted_for_ticket_discounts as price_post_discount,
        s.ticketid,
        s.refticketid as refund_ticketid,
        s.replication_val,
        s.datetime,
        to_timestamp(s.datetime) as datetime_timestamp,
        convert_timezone('UTC', o.contact_name, to_timestamp(s.datetime)) as datetime_timestamp_tz,
        o.contact_name as time_zone,
        coalesce( REGEXP_REPLACE( LEFT(s.strain, 100), '([^[:ascii:]])', ''), id :: text) as strain,
        coalesce(s.price, 0) as price,
        s.weight as weight,
        (s.weighheavy :: smallint):: int :: boolean as weighheavy,
        LEFT(s.pricepoint, 100) as pricepoint,
        s.id as saleid,
        s.productid as productid,
        -- absolutely not coalescing this this with ticketid, leave transactionid as is
        --coalesce(transactionid, ticketid :: float) as transactionid,
        s.transactionid as transactionid,
        s.transactionid_original,
        s.inventoryid,
        s.customerid,
        s.taxcat,
        -- so far no refunds/deletes are null, for trace that is the default value
        s.refunded,
        s.deleted,
        s.tax_collected as pretaxprice,
        -- DEI-23423
        s.tax_collected_excise,
        -- DEI-236
        current_timestamp() as extract_date,
        s._fivetran_synced as last_sync
    from postgres_cann_replication_public.sales_raw s
    left join prod_analytics_db.prod.stg_sales_retail r
    on s.org = r.org and s.id = r.saleid 
    join postgres_cann_replication_public.org o on s.org = o.orgid
    where r.org is null and s._fivetran_deleted = 0 
    and to_timestamp(s.datetime) > GETDATE() - interval '2 days'
     
)
select * from selected
