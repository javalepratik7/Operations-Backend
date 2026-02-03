const historyDb = require('../db/historyDb');

async function getWarehouseQuickCommData() {
  const [rows] = await historyDb.query(`
    SELECT 
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
      created_at
    FROM history_operations_db.sku_inventory_report
    WHERE ean_code IS NOT NULL
    ORDER BY created_at DESC
  `);

  console.log(`📊 Fetched ${rows.length} rows from sku_inventory_report`);
  return rows;
}

async function getQuickCommHistoricalData(eanCode, days) {
  const [rows] = await historyDb.query(`
    SELECT 
      quick_comm_total_stock,
      created_at
    FROM history_operations_db.sku_inventory_report
    WHERE ean_code = ?
      AND created_at >= NOW() - INTERVAL ? DAY
    ORDER BY created_at DESC
  `, [eanCode, days]);

  return rows;
}

async function getWarehouseHistoricalData(eanCode, days) {
  const [rows] = await historyDb.query(`
    SELECT 
      warehouse_total_stock,
      created_at
    FROM history_operations_db.sku_inventory_report
    WHERE ean_code = ?
      AND created_at >= NOW() - INTERVAL ? DAY
    ORDER BY created_at DESC
  `, [eanCode, days]);

  return rows;
}

module.exports = {
  getWarehouseQuickCommData,
  getQuickCommHistoricalData,
  getWarehouseHistoricalData,
};