
  
    

        create or replace transient table PC_FIVETRAN_DB.dbt_real_time.stg_payments_retail  as
        (with 
-- select customers from the nm trace schema

selected as  (
    select
        org,
        location,
        -- group each payment method cell into a comma separated list
        listagg(paymentmethod, ',') as paymentmethod,
        ticketid,
        current_timestamp() as extract_date

    from postgres_cann_replication_public.payments_raw where _fivetran_deleted = false
    group by
        org,
        location,
        ticketid
)
select * from selected where ticketid is not null
        );
      
  