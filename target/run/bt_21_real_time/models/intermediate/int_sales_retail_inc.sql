-- back compat for old kwarg name
  
  begin;
    

        insert into PC_FIVETRAN_DB.dbt_real_time.int_sales_retail_inc ("ORG", "DISCOUNTAMT", "PRICE_POST_DISCOUNT", "TICKETID", "REFUND_TICKETID", "DATETIME", "DATETIME_TIMESTAMP", "DATETIME_TIMESTAMP_TZ", "STRAIN", "WEIGHT", "PRICEPOINT", "CUSTOMERID", "TRANSACTIONID", "SALEID", "PRODUCTID", "WEIGHHEAVY", "PRICE", "PRETAXPRICE", "DELETED", "REFUNDED", "LOCATION", "TAX_COLLECTED_EXCISE", "EMPLOYEE_USERID", "PAYMENTMETHOD", "PRODUCER", "EXTRACT_DATE", "LAST_SYNC")
        (
            select "ORG", "DISCOUNTAMT", "PRICE_POST_DISCOUNT", "TICKETID", "REFUND_TICKETID", "DATETIME", "DATETIME_TIMESTAMP", "DATETIME_TIMESTAMP_TZ", "STRAIN", "WEIGHT", "PRICEPOINT", "CUSTOMERID", "TRANSACTIONID", "SALEID", "PRODUCTID", "WEIGHHEAVY", "PRICE", "PRETAXPRICE", "DELETED", "REFUNDED", "LOCATION", "TAX_COLLECTED_EXCISE", "EMPLOYEE_USERID", "PAYMENTMETHOD", "PRODUCER", "EXTRACT_DATE", "LAST_SYNC"
            from PC_FIVETRAN_DB.dbt_real_time.int_sales_retail_inc__dbt_tmp
        );
    commit;