
with
-- select mart transaction item
sales as (
    select *
    from {{ ref('int_sales_trace') }}
),

-- select int products
products as (
    select *
    from {{ ref('int_products_trace') }}
),

-- select int dispensary
dispensary as (
    select *
    from {{ ref('int_dispensary_trace') }}
),

-- select int customers
customers as (
    select *
    from {{ ref('int_customers_trace') }}
),

-- join them
transaction_joins as (

    select
        -- transaction item
        sales.productid,
        sales.saleid,
        sales.saleid as sales_id,
        sales.ticketid,
        sales.ticketid as ticket_id,
        sales.transactionid,
        sales.transactionid as bt_transaction_id,
        sales.price as item_final_pretax_price,
        sales.sessiontime_timestamp as date_time_notz,
        sales.refunded as is_refunded,
        sales.deleted as is_deleted,
        sales.org as source_dispensary_org_id,
        sales.location as source_dispensary_location_id,

        -- DEI-236 adding this col for testing and consistency
        sales.sessiontime,
        sales.sessiontime_timestamp,
        -- sales casting and functions
        cast(sales.sessiontime_timestamp as date) as transaction_date,
        cast(sales.weight as double precision) as item_quantity_weight,
        cast(sales.price as double precision) as item_prediscount_price,
        convert_timezone('UTC', 'GMT', sales.sessiontime_timestamp) as date_time,
        extract(year from sales.sessiontime_timestamp) as year,
        extract(month from sales.sessiontime_timestamp) as month,
        extract(day from sales.sessiontime_timestamp) as day,

        -- dispensary
        dispensary1.location_name as dispensary_name,
        dispensary1.locationtype as source_location_type,
        dispensary1.location_type_desc_id as location_type_desc_id,
        dispensary1.location_type_desc AS dispensary_license_type,
        dispensary1.orgname as organization_name,
        dispensary1.state as dispensary_state,
        concat(dispensary1.state, ' - ', dispensary1.location_name) as dispensary_state_name,
        dispensary1.masterorg as source_master_org_id,
        dispensary1.sourceorgid as source_dispensary_id,
        dispensary1.zip as dispensary_zip5,
        -- DEI-246 legacy location id
        dispensary1.legacy_2_0_location,

        -- cann 2.1 ask DEI-200
        -- dispensary2 is joined on products.manufacturer_location
        dispensary2.location_name as product_manufacturer_name,
        dispensary2.location_name as source_manufacturer,

        -- products
        products.inventorytype as source_inventorytype,
        products.category as source_category,
        products.product_name as source_product_name,
        products.straintype as source_strain_type,
        products.product_strain,

        -- customers
        customers.zip as person_zip5,
        substring(customers.zip, 1, 3) as person_zip3,
        customers.birthyear as person_date_of_birth_yyyy,
        customers.state as person_state,
        customers.gender as person_gender,

        -- consatns
        cast(50 as double precision) as employee_upsell_target,
        cast(0 as double precision) as item_total_tax,
        -- cann 2.1 DEI-188
        sales.price*0.4 as estimated_cost,
        -- DEI-223
        current_timestamp() as extract_date,

        -- concat
        concat('10', '~', sales.org, '~', dispensary1.sourceorgid, sales.location, '~', 'xxxx') as dispensary_id,
        concat('10', '~', sales.org, '~', sales.location, '~', sales.saleid) as transaction_item_id,
        concat('10', '~', sales.org, '~', sales.location, '~', sales.saleid) as transaction_item_id_hash,
        concat('10', '~', sales.org, '~', sales.location, '~', sales.customerid) as resolved_cluster_id,
        concat('10', '~', sales.org, '~', sales.location, '~', sales.customerid) as person_id_hash,
        -- addition for cann 2.1
        concat('10', '~', sales.org, '~', sales.location, '~', sales.transactionid) as transaction_id,

        -- null columns for cann 2.1 poc
        null as dispensary_store_size,
        null as dispensary_zip3,
        null as dispensary_latitude,
        null as dispensary_longitude,
        null as employee_id_hash,
        null as employee_sales_target,
        null as product_id,
        null as product_name,
        null as product_type,
        null as product_form,
        null as product_strain_type,
        null as product_strain_name,
        null as product_thc_content,
        null as product_cbd_content,
        null as product_thc_cbd_content_ratio,
        null as patient_claims_payer,
        null as product_category,
        null as descriptors,
        null as product_subcategory,
        null as product_subcategory1,
        null as product_subcategory2,
        null as source_vendor,
        null as resolved_store_name,
        null as resolved_store_address,
        null as source_guid_customer_hashed,
        null as legacy_product_id,
        null as customer_loyalty_member,
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
        null as item_unit_type,

        -- cols in pos detail that are not in trace detail
        null as employee_name,
        null as item_cost,
        null as item_total_discount,
        null as source_producer,
        null as category,
        null as datetime_timestamp,
        null as source_product_strain,
        null as source_straintype,
        null as item_final_price

    from sales
    left join products on sales.productid = products.productid
    -- join dispensary first time for most location info
    left join dispensary dispensary1 on dispensary1.location = sales.location
    -- join dispensary second time on products for manufacturer name
    left join dispensary dispensary2 on dispensary2.location = products.manufacturer_location
    -- customerid is unique and location is not relevant for this join
    left join customers on customers.customerid = sales.customerid
)

-- final select, filter out deletes and refunds
select *
from transaction_joins
where is_refunded = 0
    and is_deleted = 0
