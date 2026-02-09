const historyDb = require('../db/historyDb');
const {
  getQuickCommHistoricalData,
  getWarehouseHistoricalData
} = require('./Readoperationswarehousequickcomm');

/**
 * Safely convert DB values to numbers
 */
const n = (v) => Number(v) || 0;

async function writeWarehouseQuickCommData(inventoryData) {
  if (!inventoryData || inventoryData.length === 0) return;

  let updateCount = 0;
  let skipCount = 0;
  let errorCount = 0;

  // Pick ONLY the latest row per EAN
  const latestRecordsByEan = {};

  for (const row of inventoryData) {
    const ean = row.ean_code;
    if (
      !latestRecordsByEan[ean] ||
      new Date(row.created_at) > new Date(latestRecordsByEan[ean].created_at)
    ) {
      latestRecordsByEan[ean] = row;
    }
  }

  const uniqueRows = Object.values(latestRecordsByEan);

  for (const row of uniqueRows) {
    const {
      ean_code,
      website_drr,
      drr,
      fba_drr,
      fbf_drr,
      myntra_drr,
      zepto_speed,
      blinkit_b2b_speed,
      blinkit_marketplace_speed,
      swiggy_speed,
      increff_units,
      kvt_units,
      pc_units,
      allocated_on_hold,
      allocated_on_hold_pc_units,
      fba_units_gb,
      fba_bundled_units,
      fbf_units_gb,
      fbf_bundled_units,
      myntra_units_gb,
      myntra_bundled_units,
      zepto_stock,
      blinkit_b2b_stock,
      blinkit_marketplace_stock,
      swiggy_stock,
      vendor_increff,
      vendor_to_pc,
      vendor_to_fba,
      vendor_to_fbf,
      vendor_to_kv,
      pc_to_fba,
      pc_to_fbf,
      pc_to_increff,
      kv_to_fba,
      kv_to_fbf,
      Zepto_B2B_drr_30d,
      swiggy_drr_30d,
      blinkit_b2b_drr_30d,
      blinkit_marketplace_speed_30_days,
      created_at
    } = row;

    try {
      // ================= WAREHOUSE =================
      const warehouse_total_speed =
        n(website_drr) +
        n(drr) +
        n(fba_drr) +
        n(fbf_drr) +
        n(blinkit_marketplace_speed_30_days) +
        n(myntra_drr);

      console.log('warehouse_total_speed', warehouse_total_speed);

      const warehouse_total_stock =
        n(increff_units) +
        n(kvt_units) +
        n(pc_units) +
        n(allocated_on_hold) +
        n(allocated_on_hold_pc_units) +
        n(blinkit_marketplace_stock) +
        n(fba_units_gb) +
        n(fba_bundled_units) +
        n(fbf_units_gb) +
        n(fbf_bundled_units) +
        n(myntra_units_gb) +
        n(myntra_bundled_units);

      const warehouse_total_days_of_cover =
        warehouse_total_speed > 0
          ? warehouse_total_stock / warehouse_total_speed
          : 0;

      const wh7 = await getWarehouseHistoricalData(ean_code, 7);
      const warehouse_speed_7_days =
        wh7.length
          ? wh7.reduce((s, r) => s + n(r.warehouse_total_stock), 0) / 7
          : 0;

      const wh15 = await getWarehouseHistoricalData(ean_code, 15);
      const warehouse_speed_15_days =
        wh15.length
          ? wh15.reduce((s, r) => s + n(r.warehouse_total_stock), 0) / 15
          : 0;

      const wh30 = await getWarehouseHistoricalData(ean_code, 30);
      const warehouse_speed_30_days =
        wh30.length
          ? wh30.reduce((s, r) => s + n(r.warehouse_total_stock), 0) / 30
          : 0;

      // ================= QUICK COMMERCE =================
      const quick_comm_total_stock =
        n(swiggy_stock);

      const quick_comm_total_speed =
        n(zepto_speed) +
        n(blinkit_b2b_speed) +
        n(swiggy_speed);

      const quick_comm_total_days_of_cover =
        quick_comm_total_speed > 0
          ? quick_comm_total_stock / quick_comm_total_speed
          : 0;

      const qc7 = await getQuickCommHistoricalData(ean_code, 7);
      const quickcomm_speed_7_days =
        qc7.length
          ? qc7.reduce((s, r) => s + n(r.quick_comm_total_stock), 0) / 7
          : 0;

      const qc15 = await getQuickCommHistoricalData(ean_code, 15);
      const quickcomm_speed_15_days =
        qc15.length
          ? qc15.reduce((s, r) => s + n(r.quick_comm_total_stock), 0) / 15
          : 0;

      const qc30 = await getQuickCommHistoricalData(ean_code, 30);
      const quickcomm_speed_30_days =
        qc30.length
          ? qc30.reduce((s, r) => s + n(r.quick_comm_total_stock), 0) / 30
          : 0;

      // ================= TOTAL STOCK =================
      const vendor_transfer_stock =
        n(vendor_increff) +
        n(vendor_to_pc) +
        n(vendor_to_fba) +
        n(vendor_to_fbf) +
        n(vendor_to_kv) +
        n(pc_to_fba) +
        n(pc_to_fbf) +
        n(pc_to_increff) +
        n(kv_to_fba) +
        n(kv_to_fbf);

      const total_stock =
        warehouse_total_stock +
        quick_comm_total_stock;

      // ================= UPDATE =================
      const [result] = await historyDb.query(
        `UPDATE history_operations_db.sku_inventory_report
         SET warehouse_total_speed = ?,
             warehouse_total_stock = ?,
             warehouse_total_days_of_cover = ?,
             warehouse_speed_7_days = ?,
             warehouse_speed_15_days = ?,
             warehouse_speed_30_days = ?,
             quick_comm_total_stock = ?,
             quick_comm_total_speed = ?,
             quick_comm_total_days_of_cover = ?,
             quickcomm_speed_7_days = ?,
             quickcomm_speed_15_days = ?,
             quickcomm_speed_30_days = ?,
             total_stock = ?
         WHERE ean_code = ?
           AND created_at = ?`,
        [
          warehouse_total_speed,
          warehouse_total_stock,
          warehouse_total_days_of_cover,
          warehouse_speed_7_days,
          warehouse_speed_15_days,
          warehouse_speed_30_days,
          quick_comm_total_stock,
          quick_comm_total_speed,
          quick_comm_total_days_of_cover,
          quickcomm_speed_7_days,
          quickcomm_speed_15_days,
          quickcomm_speed_30_days,
          total_stock,
          ean_code,
          created_at
        ]
      );

      result.affectedRows ? updateCount++ : skipCount++;

    } catch (err) {
      errorCount++;
      console.error(`❌ Error for EAN ${ean_code}`, err.message);
    }
  }

  console.log(`\n✅ Warehouse & Quick Commerce sync completed`);
  console.log(`📦 EANs processed: ${uniqueRows.length}`);
  console.log(`🔄 Updated: ${updateCount}`);
  console.log(`⏭️ Skipped: ${skipCount}`);
  console.log(`❌ Errors: ${errorCount}`);
}

module.exports = {
  writeWarehouseQuickCommData,
};
