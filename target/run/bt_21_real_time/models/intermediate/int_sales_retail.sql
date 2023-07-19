
  
    

        create or replace transient table prod_analytics_db.dbt_real_time.int_sales_retail  as
        (with
-- select from stage payments
payments as (
    select *
    from prod_analytics_db.prod.stg_payments_retail
),

-- select from stage sales
sales as (
    select *
    from prod_analytics_db.dbt_real_time.stg_sales_retail
),

-- select from stage tickets
tickets as (
    select *
    from prod_analytics_db.prod.stg_tickets_retail
),


-- get refunds only from sales
refunds as (
    -- make distinct so later join doesn't explode
    select distinct
        org,
        location,
        -- this col is the reference to the refunded transactionid
        transactionid_original,
        refunded
    from sales
    where refunded = 1
),

/*
This is essentially a self join to update refunded transactions
A new column is created with the updated refund flag when applicable
 */
sales_updated as (
    select
        sales.*,
        -- when refund occured
        case when refunds.refunded = 1
            -- update column to indicate a refund
            then 1
            -- or leave it as is
            else sales.refunded
        -- name it as a new column
        end as refunded_updated--, A.ls
    from sales --left join (select max(last_sync) as ls, org from sales group by 2) A on A.org = s.org 
    left join prod_analytics_db.prod.int_sales_retail r
    on sales.org = r.org and sales.saleid = r.saleid
    left join refunds
        -- join reference id on original id
        on refunds.transactionid_original = sales.transactionid
        and refunds.org = sales.org
        and refunds.location = sales.location
    where r.org is null and to_timestamp(sales.datetime) > GETDATE() - interval '2 days'
    --where sales.last_sync < sales.ls
),

-- join above tables. This was originally the largest query, the final boss, the behemoth
the_behemoth as (
    select

        -- from sales
        sales_updated.org,
        sales_updated.discountamt,
        sales_updated.price_post_discount,
        sales_updated.ticketid,
        sales_updated.refund_ticketid,
        sales_updated.datetime,
        sales_updated.datetime_timestamp,
        sales_updated.datetime_timestamp_tz,
        sales_updated.strain,
        sales_updated.weight,
        sales_updated.pricepoint,
        sales_updated.customerid,
        sales_updated.transactionid,
        sales_updated.saleid,
        sales_updated.productid,
        sales_updated.weighheavy,
        coalesce(sales_updated.price, 0) as price,
        sales_updated.pretaxprice,
        sales_updated.deleted,
        -- replace refund column with new updated col
        sales_updated.refunded_updated as refunded,
        sales_updated.location,
        sales_updated.tax_collected_excise,

        -- from tickets
        tickets.employee_userid,

        -- from payments
        payments.paymentmethod,

        -- constants
        0 as producer,
        -- DEI-236
        current_timestamp() as extract_date,
        sales_updated.last_sync

    from sales_updated
    left join tickets
        on tickets.ticketid = sales_updated.ticketid
        and tickets.location = sales_updated.location
        and tickets.org = sales_updated.org
    left join payments on payments.ticketid = sales_updated.ticketid
        and payments.org = sales_updated.org
        and payments.location = sales_updated.location
    --where sales_updated.last_sync > (select max(last_sync) from int_sales_retail)
)

-- final select
select * from the_behemoth
        );
      
  