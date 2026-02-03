const historyDb = require('../db/historyDb');
const { getQuickCommHistoricalData, getWarehouseHistoricalData } = require('./Readoperationswarehousequickcomm');

async function writeWarehouseQuickCommData(inventoryData) {
  if (!inventoryData || inventoryData.length === 0) {
    console.log('ℹ️ No inventory data to process');
    return;
  }

  let updateCount = 0;
  let skipCount = 0;
  let errorCount = 0;

  // Group data by EAN to process latest record for each EAN
  const latestRecordsByEan = {};
  
  for (const row of inventoryData) {
    const ean = row.ean_code;
    if (!latestRecordsByEan[ean] || new Date(row.created_at) > new Date(latestRecordsByEan[ean].created_at)) {
      latestRecordsByEan[ean] = row;
    }
  }

  const uniqueRows = Object.values(latestRecordsByEan);
  console.log(`\n📊 Processing ${uniqueRows.length} unique EANs out of ${inventoryData.length} total rows`);

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
      created_at,
    } = row;

    console.log(`\n📦 Processing EAN: ${ean_code} (Created: ${created_at})`);

    try {
      // ==================== WAREHOUSE CALCULATIONS ====================
      
      // 1️⃣ Calculate warehouse_total_speed
      const warehouse_total_speed = 
        (parseFloat(website_drr) || 0) +
        (parseFloat(drr) || 0) +
        (parseFloat(fba_drr) || 0) +
        (parseFloat(fbf_drr) || 0) +
        (parseFloat(myntra_drr) || 0);

      // 2️⃣ Calculate warehouse_total_stock
      const warehouse_total_stock = 
        (parseFloat(increff_units) || 0) +
        (parseFloat(kvt_units) || 0) +
        (parseFloat(pc_units) || 0) +
        (parseFloat(allocated_on_hold) || 0) +
        (parseFloat(allocated_on_hold_pc_units) || 0) +
        (parseFloat(fba_units_gb) || 0) +
        (parseFloat(fba_bundled_units) || 0) +
        (parseFloat(fbf_units_gb) || 0) +
        (parseFloat(fbf_bundled_units) || 0) +
        (parseFloat(myntra_units_gb) || 0) +
        (parseFloat(myntra_bundled_units) || 0) +
        (parseFloat(fba_drr) || 0) +
        (parseFloat(fbf_drr) || 0);

      // 3️⃣ Calculate warehouse_total_days_of_cover
      const warehouse_total_days_of_cover = 
        warehouse_total_speed > 0 
          ? warehouse_total_stock / warehouse_total_speed 
          : 0;

      console.log(`   📊 Warehouse Calculations:`);
      console.log(`      Speed: ${warehouse_total_speed}`);
      console.log(`      Stock: ${warehouse_total_stock}`);
      console.log(`      Days Cover: ${warehouse_total_days_of_cover.toFixed(2)}`);

      // ==================== WAREHOUSE SPEED HISTORICAL CALCULATIONS ====================
      
      // Get 7 days warehouse historical data
      const warehouseData7Days = await getWarehouseHistoricalData(ean_code, 7);
      const warehouseTotalStock7Days = warehouseData7Days.reduce((sum, record) => sum + (parseFloat(record.warehouse_total_stock) || 0), 0);
      const warehouse_speed_7_days = warehouseData7Days.length > 0 ? warehouseTotalStock7Days / 7 : 0;

      // Get 15 days warehouse historical data
      const warehouseData15Days = await getWarehouseHistoricalData(ean_code, 15);
      const warehouseTotalStock15Days = warehouseData15Days.reduce((sum, record) => sum + (parseFloat(record.warehouse_total_stock) || 0), 0);
      const warehouse_speed_15_days = warehouseData15Days.length > 0 ? warehouseTotalStock15Days / 15 : 0;

      // Get 30 days warehouse historical data
      const warehouseData30Days = await getWarehouseHistoricalData(ean_code, 30);
      const warehouseTotalStock30Days = warehouseData30Days.reduce((sum, record) => sum + (parseFloat(record.warehouse_total_stock) || 0), 0);
      const warehouse_speed_30_days = warehouseData30Days.length > 0 ? warehouseTotalStock30Days / 30 : 0;

      console.log(`   📈 Warehouse Speed (Historical):`);
      console.log(`      7 days: ${warehouse_speed_7_days.toFixed(2)} (${warehouseData7Days.length} records)`);
      console.log(`      15 days: ${warehouse_speed_15_days.toFixed(2)} (${warehouseData15Days.length} records)`);
      console.log(`      30 days: ${warehouse_speed_30_days.toFixed(2)} (${warehouseData30Days.length} records)`);

      // ==================== QUICK COMMERCE CALCULATIONS ====================
      
      // 4️⃣ Calculate quick_comm_total_stock
      const quick_comm_total_stock = 
        (parseFloat(zepto_stock) || 0) +
        (parseFloat(blinkit_b2b_stock) || 0) +
        (parseFloat(blinkit_marketplace_stock) || 0) +
        (parseFloat(swiggy_stock) || 0);

      console.log(`   🚀 Quick Commerce Stock: ${quick_comm_total_stock}`);
      console.log(`      Zepto: ${zepto_stock || 0}`);
      console.log(`      Blinkit B2B: ${blinkit_b2b_stock || 0}`);
      console.log(`      Blinkit Marketplace: ${blinkit_marketplace_stock || 0}`);
      console.log(`      Swiggy: ${swiggy_stock || 0}`);

      // 5️⃣ Calculate quick_comm_total_speed (sum of all quick commerce speeds)
      const quick_comm_total_speed = 
        (parseFloat(zepto_speed) || 0) +
        (parseFloat(blinkit_b2b_speed) || 0) +
        (parseFloat(blinkit_marketplace_speed) || 0) +
        (parseFloat(swiggy_speed) || 0);

      // 6️⃣ Calculate quick_comm_total_days_of_cover
      const quick_comm_total_days_of_cover = 
        quick_comm_total_speed > 0 
          ? quick_comm_total_stock / quick_comm_total_speed 
          : 0;

      console.log(`   ⚡ Quick Commerce Speed: ${quick_comm_total_speed.toFixed(2)}`);
      console.log(`      Zepto Speed: ${zepto_speed || 0}`);
      console.log(`      Blinkit B2B Speed: ${blinkit_b2b_speed || 0}`);
      console.log(`      Blinkit Marketplace Speed: ${blinkit_marketplace_speed || 0}`);
      console.log(`      Swiggy Speed: ${swiggy_speed || 0}`);
      
      console.log(`   📅 Quick Commerce Days Cover: ${quick_comm_total_days_of_cover.toFixed(2)}`);

      // 7️⃣ Calculate quickcomm speeds based on historical data
      
      // Get 7 days historical data
      const data7Days = await getQuickCommHistoricalData(ean_code, 7);
      const totalStock7Days = data7Days.reduce((sum, record) => sum + (parseFloat(record.quick_comm_total_stock) || 0), 0);
      const quickcomm_speed_7_days = data7Days.length > 0 ? totalStock7Days / 7 : 0;

      // Get 15 days historical data
      const data15Days = await getQuickCommHistoricalData(ean_code, 15);
      const totalStock15Days = data15Days.reduce((sum, record) => sum + (parseFloat(record.quick_comm_total_stock) || 0), 0);
      const quickcomm_speed_15_days = data15Days.length > 0 ? totalStock15Days / 15 : 0;

      // Get 30 days historical data
      const data30Days = await getQuickCommHistoricalData(ean_code, 30);
      const totalStock30Days = data30Days.reduce((sum, record) => sum + (parseFloat(record.quick_comm_total_stock) || 0), 0);
      const quickcomm_speed_30_days = data30Days.length > 0 ? totalStock30Days / 30 : 0;

      console.log(`   📈 Quick Commerce Speed:`);
      console.log(`      7 days: ${quickcomm_speed_7_days.toFixed(2)} (${data7Days.length} records)`);
      console.log(`      15 days: ${quickcomm_speed_15_days.toFixed(2)} (${data15Days.length} records)`);
      console.log(`      30 days: ${quickcomm_speed_30_days.toFixed(2)} (${data30Days.length} records)`);

      // ==================== UPDATE RECORD ====================
      
      // Update records within the last 12 hours for this EAN
      const [updateResult] = await historyDb.query(
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
             quickcomm_speed_30_days = ?
         WHERE ean_code = ? 
           AND created_at >= NOW() - INTERVAL 12 HOUR`,
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
          ean_code
        ]
      );

      if (updateResult.affectedRows > 0) {
        updateCount++;
        console.log(`   ✅ Updated ${updateResult.affectedRows} record(s) successfully`);
      } else {
        skipCount++;
        console.log(`   ⚠️ No recent records found (within last 12 hours)`);
      }

    } catch (error) {
      errorCount++;
      console.error(`   ❌ Error processing EAN ${ean_code}:`, error.message);
      console.error(`   Stack:`, error.stack);
      continue;
    }
  }

  console.log(`\n✅ Warehouse & Quick Commerce sync completed:`);
  console.log(`   📊 Total EANs processed: ${uniqueRows.length}`);
  console.log(`   🔄 Updated: ${updateCount}`);
  console.log(`   ⏭️  Skipped: ${skipCount}`);
  console.log(`   ❌ Errors: ${errorCount}`);
}

module.exports = {
  writeWarehouseQuickCommData,
};