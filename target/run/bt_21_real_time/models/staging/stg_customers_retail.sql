
  
    

        create or replace transient table PC_FIVETRAN_DB.dbt_real_time.stg_customers_retail  as
        (with 

selected as (
select
    org,
    customerid,
    COALESCE(location::numeric,0)::int  as location,
    regexp_replace(left(lastname,100),'([^[:ascii:]])', '') as lastname,
    regexp_replace(left(firstname,100),'([^[:ascii:]])', '') as firstname ,
    -- case when to handle combining date of birth info ?
    date(Case when birthday = '' or birthmonth = '' or birthyear = '' then Null
            when birthmonth::int > 12 or birthyear::int > 2023 or birthday::int > 31 or  birthyear::int < 1920 then null
            WHEN birthmonth::int <= 0 THEN ('01/01/'||birthyear)::date
            When birthday::int >= 29 or birthday::int <=0 THEN (birthmonth||'/01/'||birthyear)::date
            WHEN birthmonth::int <= 0  THEN ('01/'||birthday||'/'||birthyear)::date
            ELSE (birthmonth||'/'||birthday||'/'||birthyear)::date
    END) as dob,
    regexp_replace(Left(phone,20), '([^[:ascii:]])', '') as phone,
    regexp_replace(left(address1,100), '([^[:ascii:]])', '') as address1,
    regexp_replace(left(address2,100), '([^[:ascii:]])', '') as address2,
    regexp_replace(left(city,50), '([^[:ascii:]])', '') as city,
    regexp_replace(left(state,25), '([^[:ascii:]])', '') as state,
    regexp_replace(left(zip,20), '([^[:ascii:]])', '') as zip,
    regexp_replace(left(email,50), '([^[:ascii:]])', '') as email,
    regexp_replace(left(cell,25), '([^[:ascii:]])', '') as cell,
    null as  createddate,
    visits,
    amountspent,
    case when deleted is null then (0::smallint)::int::boolean else (deleted::smallint)::int::boolean END as deleted ,
    to_timestamp(membersince)  as membersince,
    case when iscaregiver is null then (0::smallint)::int::boolean else (iscaregiver::smallint)::int::boolean END as iscaregiver,
    CASE when ismember = 1 then 1 when points > 0 then 1 else 0 end as ismember,
    regexp_replace(redcard, '([^[:ascii:]])', '') as mmj,
    resolved_gender as gender,
    modified as modified ,
    -- time of record creation
    created,
    CONCAT(10, '~', org, '~', location,'~',customerid) AS customer_primary_key, --NEED to refactor to account for ORG ID
    --rank by unique columns (this table should be unique)
    ROW_NUMBER() OVER (PARTITION BY customerid, org, location ORDER BY created DESC) rank,
    birthyear,
    resolved_gender as sex,
    case when birthyear is null  then 0 when birthyear = '' then 0 else (cast( EXTRACT( YEAR FROM current_timestamp()) as int) - Cast(birthyear as int)) end as Age,
    --DEI-236
    current_timestamp() as extract_date
from postgres_cann_replication_public.customers_raw where _fivetran_deleted = false
)

select * from selected where rank = 1
        );
      
  