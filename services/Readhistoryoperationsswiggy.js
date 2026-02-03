const historyDb = require('../db/historyDb');

async function getSwiggyInventoryDRR() {
  const [rows] = await historyDb.query(`
    SELECT 
      state, 
      city, 
      area_name, 
      store_id, 
      drr_30d, 
      drr_14d, 
      drr_7d, 
      ean, 
      units_sold
    FROM history_operations_db.swiggy_inventory_drr
    WHERE created_at >= CURDATE()
    AND created_at < CURDATE() + INTERVAL 1 DAY;
  `);

  return rows;
}

// ✅ EXPORT IT
module.exports = {
  getSwiggyInventoryDRR,
};