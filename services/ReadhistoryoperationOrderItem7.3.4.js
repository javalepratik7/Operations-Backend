const operationsDb = require('../db/operationsDb');

async function getB2BOrderItemLevelData() {
  const [rows] = await operationsDb.query(`
    SELECT 
      ean,
      supplier_name,
      supplier_wh_vendor,
      buyer_name,
      buyer_wh_vendor,
      order_quantity
    FROM operations_db.replica_b2b_order_itemlevel
  `);

  return rows;
}

module.exports = {
  getB2BOrderItemLevelData,
};