const historyDb = require('../db/historyDb');

async function writeHistoryOperationBlinkit(blinkitData) {
  if (!blinkitData || blinkitData.length === 0) {
    console.log('ℹ️ No Blinkit data to write');
    return;
  }

  let updateCount = 0;
  let insertCount = 0;
  let skipCount = 0;

  for (const row of blinkitData) {
    const {
      ean,           // From Blinkit data
      drr_30d,
      drr_14d,
      drr_7d,
    } = row;

    // Map Blinkit DRR fields → sku_inventory_report columns
    const blinkit_marketplace_speed_7_days   = drr_7d;
    const blinkit_marketplace_speed_15_days  = drr_14d;
    const blinkit_marketplace_speed_30_days  = drr_30d;

    console.log(`\n📦 Processing EAN: ${ean}`);
    console.log(`   Data: 7d=${blinkit_marketplace_speed_7_days}, 15d=${blinkit_marketplace_speed_15_days}, 30d=${blinkit_marketplace_speed_30_days}`);

    try {
      // 1️⃣ Try UPDATE the last 12 hours
      const [updateResult] = await historyDb.query(
        `UPDATE sku_inventory_report
         SET blinkit_marketplace_speed_7_days = ?, 
             blinkit_marketplace_speed_15_days = ?, 
             blinkit_marketplace_speed_30_days = ?
         WHERE ean_code = ? AND created_at >= NOW() - INTERVAL 12 HOUR`,
        [blinkit_marketplace_speed_7_days, blinkit_marketplace_speed_15_days, blinkit_marketplace_speed_30_days, ean]
      );

      console.log(`   ✓ Update affected rows: ${updateResult.affectedRows}`);

      // 2️⃣ If updated → continue to next row
      if (updateResult.affectedRows > 0) {
        updateCount++;
        continue;
      }

      // 3️⃣ Check if base record exists for this EAN
      const [existingRows] = await historyDb.query(
        `SELECT COUNT(*) as count FROM sku_inventory_report WHERE ean_code = ?`,
        [ean]
      );

      console.log(`   ✓ Existing records for EAN ${ean}: ${existingRows[0].count}`);

      if (existingRows[0].count === 0) {
        console.log(`   ⚠️ No base record found for EAN ${ean}, skipping INSERT`);
        skipCount++;
        continue;
      }

      // 4️⃣ INSERT a new row by copying the last snapshot
      const [insertResult] = await historyDb.query(
        `INSERT INTO sku_inventory_report (
          brand,
          gb_sku,
          asin,
          multiple_listing,
          ean_code,
          product_title,
          category,
          mrp,
          selling_price,
          cogs,
          pack_size,
          lead_time_vendor_lt,
          vendor_name,
          launch_date,
          is_bundle,

          vendor_increff,
          vendor_to_pc,
          vendor_to_fba,
          vendor_to_fbf,
          vendor_to_kv,

          pc_to_increff,
          pc_to_fba,
          pc_to_fbf,
          kv_to_fba,
          kv_to_fbf,

          kv_allocated_on_hold,

          increff_units,
          website_drr,
          Allocated_On_Hold_increff_Units,
          increff_day_cover,

          kvt_units,
          drr,
          kvt_day_cover,

          pc_units,
          allocated_on_hold_pc_units,

          fba_units_gb,
          bundled_fba_units_gb,
          fba_drr,

          fbf_units_gb,
          bundled_fbf_units_gb,
          fbf_drr,

          myntra_units_gb,
          bundled_myntra_units_gb,
          myntra_drr,

          rk_world_stock,
          rk_world_speed,
          rk_world_day_cover,

          marketplace_speed_7_days,
          marketplace_speed_15_days,
          marketplace_speed_30_days,

          instamart_stock,
          instamart_speed,
          instamart_day_cover,

          zepto_stock,
          zepto_speed,
          zepto_days_of_cover,

          blinkit_b2b_stock,
          blinkit_b2b_speed,
          blinkit_b2b_days_of_cover,

          blinkit_marketplace_stock,
          blinkit_marketplace_speed,
          blinkit_marketplace_days_of_cover,
          blinkit_marketplace_speed_7_days,
          blinkit_marketplace_speed_15_days,
          blinkit_marketplace_speed_30_days,

          quickcomm_speed_7_days,
          quickcomm_speed_15_days,
          quickcomm_speed_30_days,

          bigbasket_stock,
          bigbasket_speed,
          bigbasket_days_of_cover,

          purple_stock,
          purple_speed,
          purple_days_of_cover,

          reliance_stock,
          reliance_speed,
          reliance_days_of_cover,

          nykaa_stock,
          nykaa_speed,
          nykaa_days_of_cover,

          meesho_stock,
          meesho_speed,
          meesho_days_of_cover,

          golocal_export_stock,
          golocal_export_speed,
          golocal_export_days_of_cover,

          pop_club_stock,
          pop_club_speed,
          pop_club_days_of_cover,

          b2b_speed_7_days,
          b2b_speed_15_days,
          b2b_speed_30_days,

          warehouse_total_stock,
          warehouse_total_speed,
          warehouse_total_days_of_cover,

          marketplace_total_stock,
          marketplace_total_speed,
          marketplace_total_days_of_cover,

          quick_comm_total_stock,
          quick_comm_total_speed,
          quick_comm_total_days_of_cover,

          b2b_stock,
          b2b_speed,
          b2b_days_of_cover,

          total_price_at_mrp,
          total_price_at_selling,

          total_stock,
          total_speed,
          total_day_cover,
          total_cogs,

          Zepto_B2B_drr_7d,
          Zepto_B2B_drr_15d,
          Zepto_B2B_drr_30d,
          created_at
        )
        SELECT
          brand,
          gb_sku,
          asin,
          multiple_listing,
          ean_code,
          product_title,
          category,
          mrp,
          selling_price,
          cogs,
          pack_size,
          lead_time_vendor_lt,
          vendor_name,
          launch_date,
          is_bundle,

          vendor_increff,
          vendor_to_pc,
          vendor_to_fba,
          vendor_to_fbf,
          vendor_to_kv,

          pc_to_increff,
          pc_to_fba,
          pc_to_fbf,
          kv_to_fba,
          kv_to_fbf,

          kv_allocated_on_hold,

          increff_units,
          website_drr,
          Allocated_On_Hold_increff_Units,
          increff_day_cover,

          kvt_units,
          drr,
          kvt_day_cover,

          pc_units,
          allocated_on_hold_pc_units,

          fba_units_gb,
          bundled_fba_units_gb,
          fba_drr,

          fbf_units_gb,
          bundled_fbf_units_gb,
          fbf_drr,

          myntra_units_gb,
          bundled_myntra_units_gb,
          myntra_drr,

          rk_world_stock,
          rk_world_speed,
          rk_world_day_cover,

          marketplace_speed_7_days,
          marketplace_speed_15_days,
          marketplace_speed_30_days,

          instamart_stock,
          instamart_speed,
          instamart_day_cover,

          zepto_stock,
          zepto_speed,
          zepto_days_of_cover,

          blinkit_b2b_stock,
          blinkit_b2b_speed,
          blinkit_b2b_days_of_cover,

          blinkit_marketplace_stock,
          blinkit_marketplace_speed,
          blinkit_marketplace_days_of_cover,
          ?, ?, ?,

          quickcomm_speed_7_days,
          quickcomm_speed_15_days,
          quickcomm_speed_30_days,

          bigbasket_stock,
          bigbasket_speed,
          bigbasket_days_of_cover,

          purple_stock,
          purple_speed,
          purple_days_of_cover,

          reliance_stock,
          reliance_speed,
          reliance_days_of_cover,

          nykaa_stock,
          nykaa_speed,
          nykaa_days_of_cover,

          meesho_stock,
          meesho_speed,
          meesho_days_of_cover,

          golocal_export_stock,
          golocal_export_speed,
          golocal_export_days_of_cover,

          pop_club_stock,
          pop_club_speed,
          pop_club_days_of_cover,

          b2b_speed_7_days,
          b2b_speed_15_days,
          b2b_speed_30_days,

          warehouse_total_stock,
          warehouse_total_speed,
          warehouse_total_days_of_cover,

          marketplace_total_stock,
          marketplace_total_speed,
          marketplace_total_days_of_cover,

          quick_comm_total_stock,
          quick_comm_total_speed,
          quick_comm_total_days_of_cover,

          b2b_stock,
          b2b_speed,
          b2b_days_of_cover,

          total_price_at_mrp,
          total_price_at_selling,

          total_stock,
          total_speed,
          total_day_cover,
          total_cogs,

          Zepto_B2B_drr_7d,
          Zepto_B2B_drr_15d,
          Zepto_B2B_drr_30d,
          NOW()
        FROM sku_inventory_report
        WHERE ean_code = ?
        ORDER BY created_at DESC
        LIMIT 1`,
        [blinkit_marketplace_speed_7_days, blinkit_marketplace_speed_15_days, blinkit_marketplace_speed_30_days, ean]
      );

      console.log(`   ✓ Insert affected rows: ${insertResult.affectedRows}`);
      
      if (insertResult.affectedRows > 0) {
        insertCount++;
      }

    } catch (error) {
      console.error(`   ❌ Error processing EAN ${ean}:`, error.message);
      // Continue with next row instead of stopping the entire process
      continue;
    }
  }

  console.log(`\n✅ Blinkit history sync completed:`);
  console.log(`   📊 Total rows processed: ${blinkitData.length}`);
  console.log(`   🔄 Updated: ${updateCount}`);
  console.log(`   ➕ Inserted: ${insertCount}`);
  console.log(`   ⏭️  Skipped: ${skipCount}`);
}

module.exports = {
  writeHistoryOperationBlinkit,
};