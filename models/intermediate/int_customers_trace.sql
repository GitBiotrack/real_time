-- select from stage customers, no logic needed
select
    *
from {{ ref('stg_customers_trace') }}
