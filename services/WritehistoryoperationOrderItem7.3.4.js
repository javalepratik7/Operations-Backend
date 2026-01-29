const historyDb = require('../db/historyDb');


async function writeB2BOrderData(b2bOrderData) {
  if (!b2bOrderData || b2bOrderData.length === 0) {
    console.log('ℹ️ No B2B Order data to write');
    return;
  }

  // 📊 Step 1: Aggregate data by EAN
  const eanMap = new Map();

  for (const row of b2bOrderData) {
    const {
      ean,
      supplier_name,
      supplier_wh_vendor,
      buyer_name,
      buyer_wh_vendor,
      order_quantity,
    } = row;

    if (!ean) continue;

    // Initialize EAN entry if not exists
    if (!eanMap.has(ean)) {
      eanMap.set(ean, {
        vendor_increff: 0,
        vendor_to_pc: 0,
        vendor_to_fba: 0,
        vendor_to_fbf: 0,
        vendor_to_kv: 0,
        pc_to_increff: 0,
        pc_to_fba: 0,
        pc_to_fbf: 0,
        kv_to_fba: 0,
        kv_to_fbf: 0,
      });
    }

    const eanData = eanMap.get(ean);
    const qty = parseFloat(order_quantity) || 0;

    // 1️⃣ Vendor - Increff
    if (buyer_name === 'merhaki' && buyer_wh_vendor === 'assure') {
      eanData.vendor_increff += qty;
    }

    // 2️⃣ Vendor to PC
    if (buyer_name === 'merhaki' && ['hive', 'firstcry'].includes(buyer_wh_vendor)) {
      eanData.vendor_to_pc += qty;
    }

    // 3️⃣ Vendor to FBA
    if (buyer_name === 'merhaki' && buyer_wh_vendor === 'amazon') {
      eanData.vendor_to_fba += qty;
    }

    // 4️⃣ Vendor to FBF
    if (buyer_name === 'merhaki' && buyer_wh_vendor === 'flipkart') {
      eanData.vendor_to_fbf += qty;
    }

    // 5️⃣ Vendor to KV
    if (buyer_name === 'merhaki' && buyer_wh_vendor === 'brand') {
      eanData.vendor_to_kv += qty;
    }

    // 6️⃣ PC to Increff
    if (['firstcry', 'hive'].includes(supplier_wh_vendor) && buyer_wh_vendor === 'assure') {
      eanData.pc_to_increff += qty;
    }

    // 7️⃣ PC to FBA
    if (['firstcry', 'hive'].includes(supplier_wh_vendor) && buyer_wh_vendor === 'amazon') {
      eanData.pc_to_fba += qty;
    }

    // 8️⃣ PC to FBF
    if (['firstcry', 'hive'].includes(supplier_wh_vendor) && buyer_wh_vendor === 'flipkart') {
      eanData.pc_to_fbf += qty;
    }

    // 9️⃣ KV to FBA
    if (supplier_wh_vendor === 'brand' && buyer_wh_vendor === 'amazon') {
      eanData.kv_to_fba += qty;
    }

    // 🔟 KV to FBF
    if (supplier_wh_vendor === 'brand' && buyer_wh_vendor === 'flipkart') {
      eanData.kv_to_fbf += qty;
    }
  }

  // 📝 Step 2: Process each EAN
  let updateCount = 0;
  let insertCount = 0;
  let skipCount = 0;

  for (const [ean, data] of eanMap.entries()) {
    console.log(`\n📦 Processing EAN: ${ean}`);
    console.log(`   Vendor Flow: increff=${data.vendor_increff}, pc=${data.vendor_to_pc}, fba=${data.vendor_to_fba}, fbf=${data.vendor_to_fbf}, kv=${data.vendor_to_kv}`);
    console.log(`   PC Flow: increff=${data.pc_to_increff}, fba=${data.pc_to_fba}, fbf=${data.pc_to_fbf}`);
    console.log(`   KV Flow: fba=${data.kv_to_fba}, fbf=${data.kv_to_fbf}`);

    try {
      // 1️⃣ Try UPDATE the last 12 hours
      const [updateResult] = await historyDb.query(
        `UPDATE sku_inventory_report
         SET vendor_increff = ?,
             vendor_to_pc = ?,
             vendor_to_fba = ?,
             vendor_to_fbf = ?,
             vendor_to_kv = ?,
             pc_to_increff = ?,
             pc_to_fba = ?,
             pc_to_fbf = ?,
             kv_to_fba = ?,
             kv_to_fbf = ?
         WHERE ean_code = ? AND created_at >= NOW() - INTERVAL 12 HOUR`,
        [
          data.vendor_increff,
          data.vendor_to_pc,
          data.vendor_to_fba,
          data.vendor_to_fbf,
          data.vendor_to_kv,
          data.pc_to_increff,
          data.pc_to_fba,
          data.pc_to_fbf,
          data.kv_to_fba,
          data.kv_to_fbf,
          ean
        ]
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

          ?,
          ?,
          ?,
          ?,
          ?,

          ?,
          ?,
          ?,
          ?,
          ?,

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

          NOW()
        FROM sku_inventory_report
        WHERE ean_code = ?
        ORDER BY created_at DESC
        LIMIT 1`,
        [
          data.vendor_increff,
          data.vendor_to_pc,
          data.vendor_to_fba,
          data.vendor_to_fbf,
          data.vendor_to_kv,
          data.pc_to_increff,
          data.pc_to_fba,
          data.pc_to_fbf,
          data.kv_to_fba,
          data.kv_to_fbf,
          ean
        ]
      );

      console.log(`   ✓ Insert affected rows: ${insertResult.affectedRows}`);
      
      if (insertResult.affectedRows > 0) {
        insertCount++;
      }

    } catch (error) {
      console.error(`   ❌ Error processing EAN ${ean}:`, error.message);
      continue;
    }
  }

  console.log(`\n✅ B2B Order sync completed:`);
  console.log(`   📊 Total EANs processed: ${eanMap.size}`);
  console.log(`   🔄 Updated: ${updateCount}`);
  console.log(`   ➕ Inserted: ${insertCount}`);
  console.log(`   ⏭️  Skipped: ${skipCount}`);
}

module.exports = {
  writeB2BOrderData,
};