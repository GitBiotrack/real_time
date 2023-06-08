
  
    

        create or replace transient table PC_FIVETRAN_DB.dbt_real_time.int_customers_retail  as
        (with

-- simple select from stage, no logic required
customers as (
   select * from PC_FIVETRAN_DB.dbt_real_time.stg_customers_retail
)

select
    *,
    -- we dont need sequential id, this can be removed
    row_number() over (order by (select null)) as sequentialid --hacky solution to get sequentialid
from customers
        );
      
  