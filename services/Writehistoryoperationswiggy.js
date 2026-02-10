const historyDb = require('../db/historyDb');

async function writeHistoryOperationSwiggy(swiggyData) {
  if (!swiggyData || swiggyData.length === 0) {
    console.log('ℹ️ No Swiggy data to write');
    return;
  }

  let updateCount = 0;
  let snapshotInsertCount = 0;
  let freshInsertCount = 0;

  for (const row of swiggyData) {
    const {
      state,
      city,
      area_name,
      store_id,
      drr_30d,
      drr_14d,
      drr_7d,
      ean, // ✅ ONLY EAN USED — FROM INPUT
      units_sold, // ✅ NEW FIELD
    } = row;

    if (!ean) {
      console.log('⚠️ Missing EAN in input row, skipping');
      continue;
    }

    const swiggy_state = state;
    const swiggy_city = city;
    const swiggy_area_name = area_name;
    const swiggy_store_id = store_id;
    const swiggy_drr_7d = drr_7d;
    const swiggy_drr_14d = drr_14d;
    const swiggy_drr_30d = drr_30d;
    const swiggy_stock = units_sold; // ✅ units_sold becomes swiggy_stock
    const swiggy_speed = drr_7d; // ✅ using 7-day DRR as speed

    console.log(`\n📦 Processing EAN: ${ean}`);

    try {
      // 1️⃣ UPDATE last 1 day snapshot
      const [updateResult] = await historyDb.query(
        `UPDATE sku_inventory_report
         SET swiggy_state = ?,
             swiggy_city = ?,
             swiggy_area_name = ?,
             swiggy_store_id = ?,
             swiggy_drr_7d = ?,
             swiggy_drr_14d = ?,
             swiggy_drr_30d = ?,
             swiggy_stock = ?,
             swiggy_speed = ?
         WHERE ean_code = ?
           AND created_at >= NOW() - INTERVAL 12 HOUR`,
        [
          swiggy_state,
          swiggy_city,
          swiggy_area_name,
          swiggy_store_id,
          swiggy_drr_7d,
          swiggy_drr_14d,
          swiggy_drr_30d,
          swiggy_stock,
          swiggy_speed,
          ean,
        ]
      );

      if (updateResult.affectedRows > 0) {
        updateCount++;
        console.log(`   ✅ Updated ${updateResult.affectedRows} record(s)`);
        continue;
      }

      // 2️⃣ Check if EAN exists in table
      const [[{ count }]] = await historyDb.query(
        `SELECT COUNT(*) AS count
         FROM sku_inventory_report
         WHERE ean_code = ?`,
        [ean]
      );

      // 3️⃣ If EAN exists → insert snapshot copy
      if (count > 0) {
        await historyDb.query(
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

            swiggy_state,
            swiggy_city,
            swiggy_area_name,
            swiggy_store_id,
            swiggy_drr_7d,
            swiggy_drr_14d,
            swiggy_drr_30d,
            swiggy_stock,
            swiggy_speed,

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

            ?, ?, ?, ?, ?, ?, ?, ?, ?,
            NOW()
          FROM sku_inventory_report
          WHERE ean_code = ?
          ORDER BY created_at DESC
          LIMIT 1`,
          [
            swiggy_state,
            swiggy_city,
            swiggy_area_name,
            swiggy_store_id,
            swiggy_drr_7d,
            swiggy_drr_14d,
            swiggy_drr_30d,
            swiggy_stock,
            swiggy_speed,
            ean,
          ]
        );

        snapshotInsertCount++;
        console.log(`   📸 Created snapshot with Swiggy data`);
        continue;
      }

      // 4️⃣ EAN not present → insert row USING SAME INPUT EAN
      await historyDb.query(
        `INSERT INTO sku_inventory_report (
          ean_code,
          swiggy_state,
          swiggy_city,
          swiggy_area_name,
          swiggy_store_id,
          swiggy_drr_7d,
          swiggy_drr_14d,
          swiggy_drr_30d,
          swiggy_stock,
          swiggy_speed,
          created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NOW())`,
        [
          ean,
          swiggy_state,
          swiggy_city,
          swiggy_area_name,
          swiggy_store_id,
          swiggy_drr_7d,
          swiggy_drr_14d,
          swiggy_drr_30d,
          swiggy_stock,
          swiggy_speed,
        ]
      );

      freshInsertCount++;
      console.log(`   🆕 Created fresh record with Swiggy data`);

    } catch (error) {
      console.error(`❌ Error processing EAN ${ean}:`, error.message);
    }
  }

  console.log(`\n✅ Swiggy history sync completed`);
  console.log(`🔄 Updated (last 1 day): ${updateCount}`);
  console.log(`📸 Snapshot inserts: ${snapshotInsertCount}`);
  console.log(`🆕 Fresh inserts (new EAN rows): ${freshInsertCount}`);
}

module.exports = {
  writeHistoryOperationSwiggy,
};
