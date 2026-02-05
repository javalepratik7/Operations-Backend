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

    // 🔹 Normalize strings (SQL LIKE equivalent)
    const buyer = (buyer_name || '').trim().toLowerCase();
    const buyerWh = (buyer_wh_vendor || '').trim().toLowerCase();
    const supplierWh = (supplier_wh_vendor || '').trim().toLowerCase();

    const qty = parseFloat(order_quantity) || 0;

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

    // 1️⃣ Vendor → Increff
    if (buyer.includes('merhaki') && buyerWh.includes('assure')) {
      eanData.vendor_increff += qty;
    }

    // 2️⃣ Vendor → PC
    if (
      buyer.includes('merhaki') &&
      (buyerWh.includes('hive') || buyerWh.includes('firstcry'))
    ) {
      eanData.vendor_to_pc += qty;
    }

    // 3️⃣ Vendor → FBA
    if (buyer.includes('merhaki') && buyerWh.includes('amazon')) {
      eanData.vendor_to_fba += qty;
    }

    // 4️⃣ Vendor → FBF
    if (buyer.includes('merhaki') && buyerWh.includes('flipkart')) {
      eanData.vendor_to_fbf += qty;
    }

    // 5️⃣ Vendor → KV
    if (buyer.includes('merhaki') && buyerWh.includes('brand')) {
      eanData.vendor_to_kv += qty;
    }

    // 6️⃣ PC → Increff
    if (
      (supplierWh.includes('firstcry') || supplierWh.includes('hive')) &&
      buyerWh.includes('assure')
    ) {
      eanData.pc_to_increff += qty;
    }

    // 7️⃣ PC → FBA
    if (
      (supplierWh.includes('firstcry') || supplierWh.includes('hive')) &&
      buyerWh.includes('amazon')
    ) {
      eanData.pc_to_fba += qty;
    }

    // 8️⃣ PC → FBF
    if (
      (supplierWh.includes('firstcry') || supplierWh.includes('hive')) &&
      buyerWh.includes('flipkart')
    ) {
      eanData.pc_to_fbf += qty;
    }

    // 9️⃣ KV → FBA
    if (supplierWh.includes('brand') && buyerWh.includes('amazon')) {
      eanData.kv_to_fba += qty;
    }

    // 🔟 KV → FBF
    if (supplierWh.includes('brand') && buyerWh.includes('flipkart')) {
      eanData.kv_to_fbf += qty;
    }
  }

  // 📝 Step 2: DB write per EAN
  let updateCount = 0;
  let insertCount = 0;
  let skipCount = 0;

  for (const [ean, data] of eanMap.entries()) {
    try {
      // 🔄 Update recent snapshot (12 hours)
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
         WHERE ean_code = ?
           AND created_at >= NOW() - INTERVAL 12 HOUR`,
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
          ean,
        ]
      );

      if (updateResult.affectedRows > 0) {
        updateCount++;
        continue;
      }

      // 🔎 Check base record
      const [existingRows] = await historyDb.query(
        `SELECT COUNT(*) AS count FROM sku_inventory_report WHERE ean_code = ?`,
        [ean]
      );

      if (existingRows[0].count === 0) {
        skipCount++;
        continue;
      }

      // ➕ Insert new snapshot (clone last row)
      const [insertResult] = await historyDb.query(
        `
        INSERT INTO sku_inventory_report (
          SELECT
            *,
            NOW()
          FROM sku_inventory_report
          WHERE ean_code = ?
          ORDER BY created_at DESC
          LIMIT 1
        )
        `,
        [ean]
      );

      if (insertResult.affectedRows > 0) {
        insertCount++;
      }
    } catch (err) {
      console.error(`❌ Error processing EAN ${ean}:`, err.message);
    }
  }

  console.log(`\n✅ B2B Order Sync Completed`);
  console.log(`📦 EANs Processed: ${eanMap.size}`);
  console.log(`🔄 Updated: ${updateCount}`);
  console.log(`➕ Inserted: ${insertCount}`);
  console.log(`⏭️ Skipped: ${skipCount}`);
}

module.exports = { writeB2BOrderData };
