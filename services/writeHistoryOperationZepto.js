const historyDb = require('../db/historyDb');

async function writeHistoryOperationZepto(zeptoData) {
  if (!zeptoData || zeptoData.length === 0) {
    console.log('ℹ️ No Zepto data to write');
    return;
  }

  let updateCount = 0;
  let skipCount = 0;
  let errorCount = 0;

  for (const row of zeptoData) {
    const {
      ean,           // From Zepto data
      drr_30d,
      drr_14d,
      drr_7d,
    } = row;

    // Map Zepto DRR fields → sku_inventory_report columns
    const Zepto_drr_7d   = drr_7d;
    const Zepto_drr_15d  = drr_14d;
    const Zepto_drr_30d  = drr_30d;

    // console.log(`\n📦 Processing EAN: ${ean}`);
    // console.log(
    //   `   Zepto DRR → 7d=${Zepto_drr_7d}, 15d=${Zepto_drr_15d}, 30d=${Zepto_drr_30d}`
    // );

    try {
      // 1️⃣ Check if any record exists for this EAN
      const [existingRows] = await historyDb.query(
        `SELECT COUNT(*) AS count
         FROM sku_inventory_report
         WHERE ean_code = ?`,
        [ean]
      );

      if (existingRows[0].count === 0) {
        // console.log(`   ⚠️ No base record found for EAN ${ean}, skipping`);
        skipCount++;
        continue;
      }

      // 2️⃣ Update the LATEST snapshot (this was your main bug)
      const [updateResult] = await historyDb.query(
        `UPDATE sku_inventory_report
         SET
           Zepto_B2B_drr_7d  = ?,
           Zepto_B2B_drr_15d = ?,
           Zepto_B2B_drr_30d = ?
         WHERE ean_code = ?
         ORDER BY created_at DESC
         LIMIT 1`,
        [Zepto_drr_7d, Zepto_drr_15d, Zepto_drr_30d, ean]
      );

      // console.log(`   ✓ Updated rows: ${updateResult.affectedRows}`);

      if (updateResult.affectedRows > 0) {
        updateCount++;
      } else {
        // console.log(`   ⚠️ Update matched 0 rows (unexpected)`);
        skipCount++;
      }

    } catch (error) {
      errorCount++;
      // console.error(`   ❌ Error processing EAN ${ean}:`, error.message);
      continue;
    }
  }

  console.log(`\n✅ Zepto history sync completed`);
  console.log(`📊 Total rows processed : ${zeptoData.length}`);
  console.log(`🔄 Updated             : ${updateCount}`);
  console.log(`⏭️  Skipped             : ${skipCount}`);
  console.log(`❌ Errors              : ${errorCount}`);
}

module.exports = {
  writeHistoryOperationZepto,
};