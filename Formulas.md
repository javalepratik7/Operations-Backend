****************************************   Inbound / Transit Inventory  ****************************************

📦 Vendor → Increff
vendor_increff = vendor_increff + order_quantity

WHEN:
buyer_name LIKE '%Merhaki%'
AND buyer_wh_vendor LIKE '%Assure%'

📦 Vendor → PC (Hive / FirstCry)
vendor_to_pc = vendor_to_pc + order_quantity

WHEN:
buyer_name LIKE '%Merhaki%'
AND buyer_wh_vendor LIKE '%Hive%' OR '%FirstCry%'

📦 Vendor → Amazon (FBA)
vendor_to_fba = vendor_to_fba + order_quantity

WHEN:
buyer_name LIKE '%Merhaki%'
AND buyer_wh_vendor LIKE '%Amazon%'

📦 Vendor → Flipkart (FBF)
vendor_to_fbf = vendor_to_fbf + order_quantity

WHEN:
buyer_name LIKE '%Merhaki%'
AND buyer_wh_vendor LIKE '%Flipkart%'

📦 Vendor → KV (Brand warehouse)
vendor_to_kv = vendor_to_kv + order_quantity

WHEN:
buyer_name LIKE '%Merhaki%'
AND buyer_wh_vendor LIKE '%Brand%'

📦 PC → Increff
pc_to_increff = pc_to_increff + order_quantity

WHEN:
supplier_wh_vendor LIKE '%Hive%' OR '%FirstCry%'
AND buyer_wh_vendor LIKE '%Assure%'

📦 PC → Amazon (FBA)
pc_to_fba = pc_to_fba + order_quantity

WHEN:
supplier_wh_vendor LIKE '%Hive%' OR '%FirstCry%'
AND buyer_wh_vendor LIKE '%Amazon%'

📦 PC → Flipkart (FBF)
pc_to_fbf = pc_to_fbf + order_quantity

WHEN:
supplier_wh_vendor LIKE '%Hive%' OR '%FirstCry%'
AND buyer_wh_vendor LIKE '%Flipkart%'

📦 KV → Amazon (FBA)
kv_to_fba = kv_to_fba + order_quantity

WHEN:
supplier_wh_vendor LIKE '%Brand%'
AND buyer_wh_vendor LIKE '%Amazon%'

📦 KV → Flipkart (FBF)
kv_to_fbf = kv_to_fbf + order_quantity

WHEN:
supplier_wh_vendor LIKE '%Brand%'
AND buyer_wh_vendor LIKE '%Flipkart%'



****************************************   WAREHOUSE   ****************************************


📦 Increff Allocated On Hold
allocated_on_hold = allocated_on_hold_increff_units

SOURCE:
operations_db.replica_channel_drr.allocated_on_hold_increff_units

📦 Increff Day Cover
increff_day_cover = increff_units

SOURCE:
operations_db.replica_channel_drr.increff_units

📦 Amazon DRR (FBA)
fba_drr = amazon_drr

SOURCE:
operations_db.replica_channel_drr.amazon_drr


📦 Flipkart DRR (FBF)
fbf_drr = flipkart_drr

SOURCE:
operations_db.replica_channel_drr.flipkart_drr


📦 Myntra DRR
myntra_drr = myntra_drr

SOURCE:
operations_db.replica_channel_drr.myntra_drr

- increff_units = stock available at Increff warehouse
- pc_units = stock available at PC warehouse
- allocated_on_hold_pc_units = reserved / blocked stock
- fba_units_gb = Amazon FBA individual units
- fba_bundled_units = Amazon FBA bundled units
- fbf_units_gb = Amazon FBF individual units
- fbf_bundled_units = Amazon FBF bundled units
- myntra_units_gb = Myntra warehouse units
- myntra_bundled_units = Myntra bundled units

- blinkit_marketplace_stock = feeder_store_inventory + dark_store_inventory
- blinkit_marketplace_speed_7_days  = drr_7_days
- blinkit_marketplace_speed_15_days = drr_15_days
- blinkit_marketplace_speed_30_days = drr_30_days

- Zepto_B2B_drr_7d  = drr_7d
- Zepto_B2B_drr_15d = drr_14d
- Zepto_B2B_drr_30d = drr_30d

Swiggy data field mappings used in code:

- swiggy_drr_7d  = drr_7d
  (Average daily units sold over the last 7 days)

- swiggy_drr_14d = drr_14d
  (Average daily units sold over the last 14 days)

- swiggy_drr_30d = drr_30d
  (Average daily units sold over the last 30 days)

- swiggy_stock = units_sold
  (Total units sold / reported units from Swiggy input)

- swiggy_speed = drr_7d
  (7-day DRR is treated as the current selling speed)

- swiggy_state = state
  (State where the Swiggy store is located)

- swiggy_city = city
  (City where the Swiggy store is located)

- swiggy_area_name = area_name
  (Local area / locality of the Swiggy store)

- swiggy_store_id = store_id
  (Unique Swiggy store identifier)

- ean_code = ean
  (Product identifier used to match SKU across systems)




  ****************************************   TOTAL   ****************************************
1) warehouse_total_speed =
  website_drr
+ drr
+ fba_drr
+ fbf_drr
+ myntra_drr


2)warehouse_total_stock =
  increff_units
+ kvt_units
+ pc_units
+ allocated_on_hold
+ allocated_on_hold_pc_units
+ fba_units_gb
+ fba_bundled_units
+ fbf_units_gb
+ fbf_bundled_units
+ myntra_units_gb
+ myntra_bundled_units


3)warehouse_total_days_of_cover =
  warehouse_total_stock / warehouse_total_speed


4)warehouse_speed_7_days =
  (SUM of warehouse_total_stock over last 7 days) / 7

5)warehouse_speed_15_days =
  (SUM of warehouse_total_stock over last 15 days) / 15

6)warehouse_speed_30_days =
  (SUM of warehouse_total_stock over last 30 days) / 30


7)quick_comm_total_stock =
  zepto_stock
+ blinkit_b2b_stock
+ swiggy_stock


8)quick_comm_total_speed =
  zepto_speed
+ blinkit_b2b_speed
+ blinkit_marketplace_speed
+ swiggy_speed

9)quick_comm_total_days_of_cover =
  quick_comm_total_stock / quick_comm_total_speed


10)quickcomm_speed_7_days =
  (SUM of quick_comm_total_stock over last 7 days) / 7

11)quickcomm_speed_15_days =
  (SUM of quick_comm_total_stock over last 15 days) / 15

12)quickcomm_speed_30_days =
  (SUM of quick_comm_total_stock over last 30 days) / 30



13)vendor_transfer_stock =
  vendor_increff
+ vendor_to_pc
+ vendor_to_fba
+ vendor_to_fbf
+ vendor_to_kv
+ pc_to_fba
+ pc_to_fbf
+ pc_to_increff
+ kv_to_fba
+ kv_to_fbf


14)total_stock =
  warehouse_total_stock
+ quick_comm_total_stock
+ vendor_transfer_stock



  ****************************************   FINAL    ****************************************

1)drr_30d =
  quickcomm_speed_30_days
+ warehouse_speed_30_days

2)current_stock = total_stock

3)lead_time_days = lead_time_vendor_lt   ("we need to change this we will get this from sheet ")
4)safety_stock_days = 40

5)upcoming_stock =
  MAX(
    (SUM(order_quantity WHERE external_order_code LIKE '%main%')
     -
     SUM(order_quantity WHERE external_order_code LIKE '%sub%')),
    0
  )

6)in_transit_stock = SUM(`In Transit Quantity`)    or we need to calculate ("vendor_transfer_stock = vendor_increff+ vendor_to_pc+ vendor_to_fba+ vendor_to_fbf+ vendor_to_kv+ pc_to_fba+ pc_to_fbf+ pc_to_increff+ kv_to_fba+ kv_to_fbf")

7)reorder_level = drr_30d × (lead_time_days + safety_stock_days)

8)days_of_cover =(current_stock + in_transit_stock) / drr_30d

9)days_of_cover_with_po =(current_stock + in_transit_stock + upcoming_stock) / drr_30d


10)IF (current + transit + upcoming) <= reorder_level
    → PO_REQUIRED

    ELSE IF (current + transit) <= reorder_level
    → LOW_STOCK

    ELSE IF (current + transit) >= reorder_level
    → OVER_STOCK

    ELSE
    → OK


    
