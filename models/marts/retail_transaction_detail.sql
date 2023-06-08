
with
-- select mart transaction item
sales as (
    select *
    from prod_analytics_db.prod.int_sales_retail
),

-- select int products
products as (
    select *
    from prod_analytics_db.prod.int_products_retail
),

-- select mart dispensary
dispensary as (
    select *
    from prod_analytics_db.prod.int_dispensary_retail
),

-- select int customers
customers as (
    select *
    from prod_analytics_db.prod.int_customers_retail
),

-- group by product to get max product cost
-- ideally we'd dedup well before this step. This will suffice for now
products_aggregate as (
    select
        productid,
        org,
        category,
        manufacturer,
        producer,
        product_strain,
        straintype,
        vendor,
        inventorytype,
        product_name,
        max(costperunit) as costperunit
    from
        products
    where productid is not null
    group by
        productid,
        org,
        category,
        manufacturer,
        producer,
        product_strain,
        straintype,
        vendor,
        inventorytype,
        product_name
),

transaction_joins as (

    select
        -- transaction item
        sales.employee_userid as employee_name,
        sales.productid,
        sales.productid as legacy_product_id,
        sales.saleid,
        sales.ticketid,
        sales.tax_collected_excise,

        --cast(sales.transaction_time as date) as transaction_date,
        cast(sales.datetime_timestamp_tz as date) as transaction_date,
        cast(sales.weight as double precision) as item_quantity_weight,
        --cast(sales.item_cost as double precision) as item_cost,
        cast(sales.price as double precision) as item_prediscount_price,
        cast(sales.discountamt as double precision) as item_total_discount,
        cast(sales.price_post_discount as double precision) as item_final_pretax_price,
        cast(sales.price + sales.pretaxprice as double precision) as item_final_price,
        -- epoch time, utc 0
        sales.datetime,
        -- timestamp utc 0
        sales.datetime_timestamp,
        -- timestamp, time zone corrected
        sales.datetime_timestamp_tz,
        -- rename above for looker query
        sales.datetime_timestamp_tz as date_time,
        extract(year from sales.datetime_timestamp_tz) as year,
        extract(month from sales.datetime_timestamp_tz) as month,
        extract(day from sales.datetime_timestamp_tz) as day,

        -- for cann 2.1 poc
        sales.refunded as is_refunded,
        -- adding this col, it is not originally in cann 2.0
        sales.deleted as is_deleted,
        -- replicate whats in pos trans detail
        sha2(concat(sales.employee_userid,
                sales.org,
                sales.location)) as employee_id_hash,

        -- dispensary and zip
        dispensary.location_name as dispensary_name,
        dispensary.locationtype as source_location_type,
        dispensary.location_type_desc AS dispensary_license_type,
        dispensary.orgname as organization_name,
        dispensary.state as dispensary_state,
        concat(dispensary.state, ' - ', dispensary.location_name) as dispensary_state_name,
        dispensary.org as source_dispensary_org_id,
        dispensary.location as source_dispensary_location_id,
        dispensary.masterorg as source_master_org_id,
        substr(dispensary.zip, 1, 3) as dispensary_zip3,
        dispensary.zip as dispensary_zip5,

        -- products
        products_aggregate.inventorytype as source_inventorytype,
        products_aggregate.category as source_category,
        -- copy of above for cann 2.1 poc
        products_aggregate.category as product_category,
        products_aggregate.product_name as source_product_name,
        -- DEI-236 adding this in without the rename
        products_aggregate.product_name,

        -- For cann 2.1
        cast(products_aggregate.costperunit as double precision) as item_cost,
        products_aggregate.category,
        products_aggregate.manufacturer as source_manufacturer,
        products_aggregate.producer as source_producer,
        products_aggregate.product_strain as source_product_strain,
        products_aggregate.product_strain as product_strain_name,
        products_aggregate.straintype as source_straintype,
        products_aggregate.vendor as source_vendor,
        -- originally for DEI-198, modified for DEI-223,
        -- not sure if this is the most accurate solution, but it will populate
        coalesce(products_aggregate.manufacturer, products_aggregate.vendor, products_aggregate.producer) as product_manufacturer_name,

        -- customers
        -- for cann 2.1 poc
        customers.zip as person_zip5,
        substring(customers.zip, 1, 3) as person_zip3,
        extract(year from customers.dob) as person_date_of_birth_yyyy,
        customers.state as person_state,
        customers.gender as person_gender,
        CAST(CASE WHEN ismember > 0 THEN 1 ELSE 0 END AS boolean) as customer_loyalty_member,

        -- consatns
        cast(50 as double precision) as employee_upsell_target,
        cast(0 as double precision) as item_total_tax,
        -- cann 2.1 DEI-188
        sales.price_post_discount*0.4 as estimated_cost,
        -- DEI-223
        current_timestamp() as extract_date,

        -- concat
        concat('10', '~', sales.org, '~', sales.location, '~', 'xxxx') as dispensary_id,
        concat('10', '~', sales.org, '~', sales.location, '~', sales.ticketid) as transaction_id,
        concat('10', '~', sales.org, '~', sales.location, '~', sales.saleid) as transaction_item_id,
        concat('10', '~', sales.org, '~', sales.location, '~', sales.saleid) as transaction_item_id_hash,
        concat('10', '~', sales.org, '~', sales.location, '~', sales.customerid) as resolved_cluster_id,
        -- same as above.
        concat('10', '~', sales.org, '~', sales.location, '~', sales.customerid) as person_id_hash,

        -- filling in null columns needed for some dashboards
        null as unit_type,
        null as dispensary_store_size,
        null as dispensary_latitude,
        null as dispensary_longitude,
        null as employee_sales_target,
        null as product_id,
        null as product_type,
        null as product_form,
        null as product_strain_type,
        null as product_thc_content,
        null as product_cbd_content,
        null as product_thc_cbd_content_ratio,
        null as patient_claims_payer,
        null as descriptors,
        null as product_subcategory,
        null as product_subcategory1,
        null as product_subcategory2,
        null as resolved_store_name,
        null as resolved_store_address,
        null as source_guid_customer_hashed,
        null as source_dispensary_id,
        null as sales_id,
        null as ticket_id,
        null as bt_transaction_id,

        -- from customers dataset
        null as person_latitude,
        null as person_longitude,
        null as consumer_age_in_two_year_increments_person,
        null as consumer_marital_status_in_the_household_100_description,
        null as consumer_number_of_children_100,
        null as consumer_home_owner_renter_100_description,
        null as consumer_education_person_description,
        null as consumer_occupation_person_description,
        null as consumer_income_estimated_household_higher_ranges_description,
        null as consumer_health_and_well_being_segmentation_the_invincibles_score,
        null as consumer_health_and_well_being_segmentation_trusting_patients_score,
        null as consumer_health_and_well_being_segmentation_healthy_holistics_score,
        null as consumer_exercise_health_grouping,
        null as consumer_make_a_purchase_via_internet_financial_score,
        null as consumer_filled_rx_via_mail_order_in_the_last_12_months_score,
        null as consumer_looked_for_medical_information_on_the_web_score,
        null as consumer_consume_media_via_a_cell_phone_score,
        null as consumer_consume_media_via_primetime_tv_score,
        null as consumer_consume_media_via_radio_score,
        null as consumer_consume_media_via_the_internet_score,
        null as consumer_economic_stability_indicator_score,
        null as consumer_lifestage_segment_description,
        null as consumer_lifestage_group_description,
        null as consumer_lifestage_insurance_group_description,
        null as consumer_electronics,
        -- DEI-256 removing data from this column, but keeping it in the table
        null as location_type_desc_id

    from sales
    left join products_aggregate
        on sales.productid = products_aggregate.productid
        -- org should suffice, no location needed
        and products_aggregate.org = sales.org
    left join dispensary on dispensary.org = sales.org
        and dispensary.location = sales.location
    left join customers on customers.customerid = sales.customerid
        and customers.org  = sales.org
        and customers.location = sales.location

)

-- final select, filter out refunds and deletes
select *
from transaction_joins
where is_refunded = 0
    and is_deleted = 0
    and lower(left(dispensary_license_type, 5)) = 'dispe'
