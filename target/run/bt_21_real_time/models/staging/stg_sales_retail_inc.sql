-- back compat for old kwarg name
  
  begin;
    

        insert into PC_FIVETRAN_DB.dbt_real_time.stg_sales_retail_inc ("ORG", "LOCATION", "DISCOUNTAMT", "PRICE_POST_DISCOUNT", "TICKETID", "REFUND_TICKETID", "REPLICATION_VAL", "DATETIME", "DATETIME_TIMESTAMP", "DATETIME_TIMESTAMP_TZ", "TIME_ZONE", "STRAIN", "PRICE", "WEIGHT", "WEIGHHEAVY", "PRICEPOINT", "SALEID", "PRODUCTID", "TRANSACTIONID", "TRANSACTIONID_ORIGINAL", "INVENTORYID", "CUSTOMERID", "TAXCAT", "REFUNDED", "DELETED", "PRETAXPRICE", "TAX_COLLECTED_EXCISE", "EXTRACT_DATE", "LAST_SYNC")
        (
            select "ORG", "LOCATION", "DISCOUNTAMT", "PRICE_POST_DISCOUNT", "TICKETID", "REFUND_TICKETID", "REPLICATION_VAL", "DATETIME", "DATETIME_TIMESTAMP", "DATETIME_TIMESTAMP_TZ", "TIME_ZONE", "STRAIN", "PRICE", "WEIGHT", "WEIGHHEAVY", "PRICEPOINT", "SALEID", "PRODUCTID", "TRANSACTIONID", "TRANSACTIONID_ORIGINAL", "INVENTORYID", "CUSTOMERID", "TAXCAT", "REFUNDED", "DELETED", "PRETAXPRICE", "TAX_COLLECTED_EXCISE", "EXTRACT_DATE", "LAST_SYNC"
            from PC_FIVETRAN_DB.dbt_real_time.stg_sales_retail_inc__dbt_tmp
        );
    commit;