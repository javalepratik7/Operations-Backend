const historyDb = require('../db/historyDb');

async function writeOperationsChannelDRR(channelData) {
  if (!channelData || channelData.length === 0) {
    console.log('ℹ️ No Channel DRR data to write');
    return;
  }

  let updateCount = 0;
  let insertCount = 0;
  let skipCount = 0;

  for (const row of channelData) {
    const {
      ean,
      allocated_on_hold_increff_units,
      increff_units,
      amazon_drr,
      flipkart_drr,
      myntra_drr,
    } = row;

    // Map operations_db fields → sku_inventory_report columns
    const allocated_on_hold = allocated_on_hold_increff_units;
    const increff_day_cover_value = increff_units;
    const fba_drr = amazon_drr;
    const fbf_drr = flipkart_drr;
    const myntra_drr_value = myntra_drr;

    // console.log(`\n📦 Processing EAN: ${ean}`);
    // console.log(`   Data: allocated_on_hold=${allocated_on_hold}, increff_day_cover=${increff_day_cover_value}`);
    // console.log(`   DRR: fba=${fba_drr}, fbf=${fbf_drr}, myntra=${myntra_drr_value}`);

    try {
      // 1️⃣ Try UPDATE the last 12 hours
      const [updateResult] = await historyDb.query(
        `UPDATE sku_inventory_report
         SET allocated_on_hold = ?,
             increff_day_cover = ?,
             fba_drr = ?,
             fbf_drr = ?,
             myntra_drr = ?
         WHERE ean_code = ? AND created_at >= NOW() - INTERVAL 12 HOUR`,
        [allocated_on_hold, increff_day_cover_value, fba_drr, fbf_drr, myntra_drr_value, ean]
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
        // console.log(`   ⚠️ No base record found for EAN ${ean}, skipping INSERT`);
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
          allocated_on_hold,
          increff_day_cover,

          kvt_units,
          drr,
          kvt_day_cover,

          pc_units,
          allocated_on_hold_pc_units,

          fba_units_gb,
          fba_bundled_units,
          fba_drr,

          fbf_units_gb,
          fbf_bundled_units,
          fbf_drr,

          myntra_units_gb,
          myntra_bundled_units,
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

          swiggy_state,
          swiggy_city,
          swiggy_area_name,
          swiggy_store_id,
          swiggy_drr_7d,
          swiggy_drr_14d,
          swiggy_drr_30d,

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
          ?,
          ?,

          kvt_units,
          drr,
          kvt_day_cover,

          pc_units,
          allocated_on_hold_pc_units,

          fba_units_gb,
          fba_bundled_units,
          ?,

          fbf_units_gb,
          fbf_bundled_units,
          ?,

          myntra_units_gb,
          myntra_bundled_units,
          ?,

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

          swiggy_state,
          swiggy_city,
          swiggy_area_name,
          swiggy_store_id,
          swiggy_drr_7d,
          swiggy_drr_14d,
          swiggy_drr_30d,

          NOW()
        FROM sku_inventory_report
        WHERE ean_code = ?
        ORDER BY created_at DESC
        LIMIT 1`,
        [allocated_on_hold, increff_day_cover_value, fba_drr, fbf_drr, myntra_drr_value, ean]
      );

      // console.log(`   ✓ Insert affected rows: ${insertResult.affectedRows}`);
      
      if (insertResult.affectedRows > 0) {
        insertCount++;
      }

    } catch (error) {
      console.error(`   ❌ Error processing EAN ${ean}:`, error.message);
      // Continue with next row instead of stopping the entire process
      continue;
    }
  }

  console.log(`\n✅ Channel DRR sync completed:`);
  console.log(`   📊 Total rows processed: ${channelData.length}`);
  console.log(`   🔄 Updated: ${updateCount}`);
  console.log(`   ➕ Inserted: ${insertCount}`);
  console.log(`   ⏭️  Skipped: ${skipCount}`);
}

module.exports = {
  writeOperationsChannelDRR,
};