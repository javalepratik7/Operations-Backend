const operationsDb = require('../db/operationsDb');

async function getInventoryDetailsData() {
  const [rows] = await operationsDb.query(`
    SELECT 
      ean,
      \`Increff Units\` as increff_units,
      \`PC Units\` as pc_units,
      \`Allocated_On Hold Increff Units\` as allocated_on_hold_increff_units,
      \`FBA Units GB\` as fba_units_gb,
      \`Bundled FBA Units GB\` as bundled_fba_units_gb,
      \`FBF Units GB\` as fbf_units_gb,
      \`Bundled FBF Units GB\` as bundled_fbf_units_gb,
      \`Myntra Units GB\` as myntra_units_gb,
      \`Bundled Myntra Units GB\` as bundled_myntra_units_gb
    FROM operations_db.view_sku_level_inventory_details
  `);

  return rows;
}

// ✅ EXPORT IT
module.exports = {
  getInventoryDetailsData,
};