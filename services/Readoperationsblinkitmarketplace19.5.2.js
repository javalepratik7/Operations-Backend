const operationsDb = require('../db/operationsDb');

async function getBlinkitMarketplaceData() {
  const [rows] = await operationsDb.query(`
    SELECT 
      ean,
      feeder_store_inventory,
      dark_store_inventory,
      drr_7_days,
      drr_15_days,
      drr_30_days
    FROM operations_db.replica_blinkit_marketplace_inventory
    WHERE ean IS NOT NULL
  `);

  return rows;
}

module.exports = {
  getBlinkitMarketplaceData,
};