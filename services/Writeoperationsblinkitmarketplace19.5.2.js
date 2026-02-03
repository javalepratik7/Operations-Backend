const historyDb = require('../db/historyDb');

async function writeBlinkitMarketplaceData(blinkitData) {
  if (!blinkitData || blinkitData.length === 0) {
    console.log('ℹ️ No Blinkit Marketplace data to write');
    return;
  }

  let updateCount = 0;
  let insertCount = 0;
  let skipCount = 0;

  for (const row of blinkitData) {
    const {
      ean,
      feeder_store_inventory,
      dark_store_inventory,
      drr_7_days,
      drr_15_days,
      drr_30_days,
    } = row;

    // ✅ Correct column mapping
    const blinkit_marketplace_stock =
      (parseFloat(feeder_store_inventory) || 0) +
      (parseFloat(dark_store_inventory) || 0);

    const blinkit_marketplace_speed_7_days = drr_7_days;
    const blinkit_marketplace_speed_15_days = drr_15_days;
    const blinkit_marketplace_speed_30_days = drr_30_days;

    console.log(`\n📦 Processing EAN: ${ean}`);
    console.log(`   Total Stock: ${blinkit_marketplace_stock}`);
    console.log(
      `   Speed: 7d=${blinkit_marketplace_speed_7_days}, 15d=${blinkit_marketplace_speed_15_days}, 30d=${blinkit_marketplace_speed_30_days}`
    );

    try {
      // 1️⃣ UPDATE (last 12 hours)
      const [updateResult] = await historyDb.query(
        `UPDATE sku_inventory_report
         SET blinkit_marketplace_stock = ?,
             blinkit_marketplace_speed_7_days = ?,
             blinkit_marketplace_speed_15_days = ?,
             blinkit_marketplace_speed_30_days = ?
         WHERE ean_code = ?
           AND created_at >= NOW() - INTERVAL 12 HOUR`,
        [
          blinkit_marketplace_stock,
          blinkit_marketplace_speed_7_days,
          blinkit_marketplace_speed_15_days,
          blinkit_marketplace_speed_30_days,
          ean,
        ]
      );

      if (updateResult.affectedRows > 0) {
        updateCount++;
        continue;
      }

      // 2️⃣ Check base record
      const [existingRows] = await historyDb.query(
        `SELECT COUNT(*) AS count
         FROM sku_inventory_report
         WHERE ean_code = ?`,
        [ean]
      );

      if (existingRows[0].count === 0) {
        skipCount++;
        continue;
      }

      // 3️⃣ INSERT new snapshot
      const [insertResult] = await historyDb.query(
        `INSERT INTO sku_inventory_report (
          brand, gb_sku, asin, multiple_listing, ean_code, product_title, category,
          mrp, selling_price, cogs, pack_size, lead_time_vendor_lt, vendor_name,
          launch_date, is_bundle,
          vendor_increff, vendor_to_pc, vendor_to_fba, vendor_to_fbf, vendor_to_kv,
          pc_to_increff, pc_to_fba, pc_to_fbf, kv_to_fba, kv_to_fbf,
          kv_allocated_on_hold, increff_units, website_drr, allocated_on_hold,
          increff_day_cover, kvt_units, drr, kvt_day_cover, pc_units,
          allocated_on_hold_pc_units, fba_units_gb, fba_bundled_units, fba_drr,
          fbf_units_gb, fbf_bundled_units, fbf_drr, myntra_units_gb,
          myntra_bundled_units, myntra_drr, rk_world_stock, rk_world_speed,
          rk_world_day_cover,
          blinkit_marketplace_stock,
          blinkit_marketplace_speed_7_days,
          blinkit_marketplace_speed_15_days,
          blinkit_marketplace_speed_30_days,
          created_at
        )
        SELECT
          brand, gb_sku, asin, multiple_listing, ean_code, product_title, category,
          mrp, selling_price, cogs, pack_size, lead_time_vendor_lt, vendor_name,
          launch_date, is_bundle,
          vendor_increff, vendor_to_pc, vendor_to_fba, vendor_to_fbf, vendor_to_kv,
          pc_to_increff, pc_to_fba, pc_to_fbf, kv_to_fba, kv_to_fbf,
          kv_allocated_on_hold, increff_units, website_drr, allocated_on_hold,
          increff_day_cover, kvt_units, drr, kvt_day_cover, pc_units,
          allocated_on_hold_pc_units, fba_units_gb, fba_bundled_units, fba_drr,
          fbf_units_gb, fbf_bundled_units, fbf_drr, myntra_units_gb,
          myntra_bundled_units, myntra_drr, rk_world_stock, rk_world_speed,
          rk_world_day_cover,
          ?, ?, ?, ?, NOW()
        FROM sku_inventory_report
        WHERE ean_code = ?
        ORDER BY created_at DESC
        LIMIT 1`,
        [
          blinkit_marketplace_stock,
          blinkit_marketplace_speed_7_days,
          blinkit_marketplace_speed_15_days,
          blinkit_marketplace_speed_30_days,
          ean,
        ]
      );

      if (insertResult.affectedRows > 0) {
        insertCount++;
      }

    } catch (error) {
      console.error(`❌ Error processing EAN ${ean}:`, error.message);
    }
  }

  console.log(`\n✅ Blinkit Marketplace sync completed`);
  console.log(`🔄 Updated: ${updateCount}`);
  console.log(`➕ Inserted: ${insertCount}`);
  console.log(`⏭️ Skipped: ${skipCount}`);
}

module.exports = { writeBlinkitMarketplaceData };
