const operationsDb = require('../db/operationsDb');

async function getChannelDRRData() {
  const [rows] = await operationsDb.query(`
    SELECT 
      ean,
      allocated_on_hold_increff_units,
      increff_units,
      amazon_drr,
      flipkart_drr,
      myntra_drr
    FROM operations_db.view_sku_level_inventory_details_channel_drr
  `);

  return rows;
}

// ✅ EXPORT IT
module.exports = {
  getChannelDRRData,
};