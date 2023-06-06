with 
-- select customers from the nm trace schema

selected as (
    select
        org,
        customerid,
        state,
        case
            when sex = 0 then 'M'
			when sex = 1 then 'F'
        end as gender,
        location,
        Left(zip, 20) as zip,
        Case when birthyear is null then null when birthday is null or birthday = '' or birthday::int > 31
         then  CONCAT(birthyear, '/', birthmonth, '/01') when birthmonth is null
         then CONCAT(birthyear, '/01', birthday)  else CONCAT ( birthyear, '/', birthmonth, '/', birthday) END as dob,
        birthyear,
        created,
        max(created) over (partition by location, customerid) as max_created,
        current_timestamp() as extract_date
     from postgres_cann_replication_public.bmsi_customers_raw where _fivetran_deleted = false
)

select *
from selected
where created = max_created
