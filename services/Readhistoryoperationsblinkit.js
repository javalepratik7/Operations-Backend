const historyDb = require('../db/historyDb');

async function getBlinkitInventoryDRR() {
  const [rows] = await historyDb.query(`
    SELECT 
      ean,
      brand,
      product_title,
      drr_30d,
      drr_14d,
      drr_7d,
      \`27_jan_26\` AS blinkit_b2b_stock
    FROM history_operations_db.Blinkit_inventory_drr
    WHERE created_at >= CURDATE()
      AND created_at < CURDATE() + INTERVAL 1 DAY;
  `);

  return rows;
}

// ✅ EXPORT IT
module.exports = {
  getBlinkitInventoryDRR,
};