const historyDb = require('../db/historyDb');
const {
  getQuickCommHistoricalData,
  getWarehouseHistoricalData
} = require('./Readoperationswarehousequickcomm');

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
      created_at
    } = row;

    try {
      // ================= WAREHOUSE =================
      const warehouse_total_speed =
        (website_drr || 0) +
        (drr || 0) +
        (fba_drr || 0) +
        (fbf_drr || 0) +
        (myntra_drr || 0);

      const warehouse_total_stock =
        (increff_units || 0) +
        (kvt_units || 0) +
        (pc_units || 0) +
        (allocated_on_hold || 0) +
        (allocated_on_hold_pc_units || 0) +
        (fba_units_gb || 0) +
        (fba_bundled_units || 0) +
        (fbf_units_gb || 0) +
        (fbf_bundled_units || 0) +
        (myntra_units_gb || 0) +
        (myntra_bundled_units || 0);

      const warehouse_total_days_of_cover =
        warehouse_total_speed > 0
          ? warehouse_total_stock / warehouse_total_speed
          : 0;

      const wh7 = await getWarehouseHistoricalData(ean_code, 7);
      const warehouse_speed_7_days =
        wh7.length ? wh7.reduce((s, r) => s + (r.warehouse_total_stock || 0), 0) / 7 : 0;

      const wh15 = await getWarehouseHistoricalData(ean_code, 15);
      const warehouse_speed_15_days =
        wh15.length ? wh15.reduce((s, r) => s + (r.warehouse_total_stock || 0), 0) / 15 : 0;

      const wh30 = await getWarehouseHistoricalData(ean_code, 30);
      const warehouse_speed_30_days =
        wh30.length ? wh30.reduce((s, r) => s + (r.warehouse_total_stock || 0), 0) / 30 : 0;

      // ================= QUICK COMMERCE =================
      const quick_comm_total_stock =
        (zepto_stock || 0) +
        (blinkit_b2b_stock || 0) +
        (blinkit_marketplace_speed || 0) +
        (swiggy_stock || 0);

      const quick_comm_total_speed =
        (zepto_speed || 0) +
        (blinkit_b2b_speed || 0) +
        (swiggy_speed || 0);

      const quick_comm_total_days_of_cover =
        quick_comm_total_speed > 0
          ? quick_comm_total_stock / quick_comm_total_speed
          : 0;

      const qc7 = await getQuickCommHistoricalData(ean_code, 7);
      const quickcomm_speed_7_days =
        qc7.length ? qc7.reduce((s, r) => s + (r.quick_comm_total_stock || 0), 0) / 7 : 0;

      const qc15 = await getQuickCommHistoricalData(ean_code, 15);
      const quickcomm_speed_15_days =
        qc15.length ? qc15.reduce((s, r) => s + (r.quick_comm_total_stock || 0), 0) / 15 : 0;

      const qc30 = await getQuickCommHistoricalData(ean_code, 30);
      const quickcomm_speed_30_days =
        qc30.length ? qc30.reduce((s, r) => s + (r.quick_comm_total_stock || 0), 0) / 30 : 0;

      // ================= TOTAL STOCK =================
      const vendor_transfer_stock =
        (vendor_increff || 0) +
        (vendor_to_pc || 0) +
        (vendor_to_fba || 0) +
        (vendor_to_fbf || 0) +
        (vendor_to_kv || 0) +
        (pc_to_fba || 0) +
        (pc_to_fbf || 0) +
        (pc_to_increff || 0) +
        (kv_to_fba || 0) +
        (kv_to_fbf || 0);

      const total_stock =
        warehouse_total_stock +
        quick_comm_total_stock +
        vendor_transfer_stock;

      // ================= UPDATE (EXACT ROW) =================
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
