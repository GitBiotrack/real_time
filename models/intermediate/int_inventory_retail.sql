with

-- select stage inventory log
-- inventory logs should not even be used
-- see the confluence doc "BT 1.0 source data" section product/inventory > inventory
inventory_logs as (
   select * from {{ ref('stg_inventorylogs_retail') }}
),

-- select stage inventory room
inventory_rooms as (
    select * from {{ ref('stg_inventory_rooms_retail') }}
),

-- select stage invenotry
inventory as (
    select * from {{ ref('stg_inventory_retail') }}
),

-- select stage products
products as (
    select * from {{ ref('stg_products_retail') }}
),

-- select stage prod cat
product_categories as (
    select * from {{ ref('stg_product_categories_retail') }}
),

-- join tables
inventory_summary as (
    select
        --from inventory log
		inventory_logs.new_weight,
        inventory_logs.weight,
		inventory_logs.org,
		inventory_logs.sessiontime,
        inventory_logs.sessiontime_timestamp,
		inventory_logs.quantity,
		inventory_logs.inventoryid,

		--from inventory
		inventory.strain,
        inventory.straintype,
		inventory.location,
		inventory.productid,
        inventory.expiration,
        inventory.producer,
        inventory.inventorytype,

		--from products
		products.name,

		--from product categories
		product_categories.tracecat,

		--from inventory rooms
		inventory_rooms.room,

        -- constants
        --DEI-223
        current_timestamp() as extract_date

    from inventory_logs
    left join inventory
        on inventory_logs.inventoryid = inventory.id
        and inventory_logs.org = inventory.org
    left join products
        on products.productid = inventory.productid
        and products.org = inventory.org
    left join product_categories
        on products.product_categories_id = product_categories.product_categories_id
        and product_categories.org = products.org
    left join inventory_rooms
        on inventory_rooms.org = inventory_logs.org
        -- idk where to start here but obviously this is too complicated to be logically sound
        -- inventory logs should not even be used. The inventory room value from the inventory table should be adequate.
        -- see the confluence doc "BT 1.0 source data" section product/inventory > inventory
        and inventory_rooms.id = (CASE
									WHEN (CASE
											WHEN SPLIT_PART(SPLIT_PART(inventory_logs.roomdata,':',1),',',2) != '0'
                    							THEN SPLIT_PART(SPLIT_PART(inventory_logs.roomdata,':',1),',',1)
                    						WHEN SPLIT_PART(SPLIT_PART(inventory_logs.roomdata,':',2),',',2) != '0'
                    							THEN SPLIT_PART(SPLIT_PART(inventory_logs.roomdata,':',2),',',1)
                    						WHEN SPLIT_PART(SPLIT_PART(inventory_logs.roomdata,':',3),',',2) != '0'
                    							THEN SPLIT_PART(SPLIT_PART(inventory_logs.roomdata,':',3),',',1)
                    						ELSE '0' END
										) =''
										THEN '0'
                    				ELSE (CASE
											WHEN SPLIT_PART(SPLIT_PART(inventory_logs.roomdata,':',1),',',2) != '0'
						                    	THEN SPLIT_PART(SPLIT_PART(inventory_logs.roomdata,':',1),',',1)
						                    WHEN SPLIT_PART(SPLIT_PART(inventory_logs.roomdata,':',2),',',2) != '0'
						                    	THEN SPLIT_PART(SPLIT_PART(inventory_logs.roomdata,':',2),',',1)
						                    WHEN SPLIT_PART(SPLIT_PART(inventory_logs.roomdata,':',3),',',2) != '0'
						                    	THEN SPLIT_PART(SPLIT_PART(inventory_logs.roomdata,':',3),',',1)
						                    ELSE '0'END
										)
									END)::int
)

-- final selection
select * from inventory_summary
