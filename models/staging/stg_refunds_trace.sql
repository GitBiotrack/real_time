with
sales as (
    select *
    from {{ ref('stg_sales_trace') }}
),

-- select only necessary columns needed for a join and filter for refunded transactions
filtered as (
    select distinct
        org,
        -- in quotes since its a keyword
        location,
        transactionid_original,
        refunded,
        current_timestamp() as extract_date

    from sales
    where refunded = 1
)

-- final selection
select * from filtered
