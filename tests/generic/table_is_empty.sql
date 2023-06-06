{% test table_is_empty(model) %}

with
-- count all rows for a table
count_rows as (
    select 
        count(*) as row_count
    from {{ model }}
),

-- failure occurs if the test has any rows
test as (
    select *
    from count_rows
    -- keep the row count only if its 0, ie it fails
    where row_count = 0
)

select *
from test

{% endtest %}
