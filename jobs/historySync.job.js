const cron = require('node-cron');

const { getZeptoInventoryDRR } = require('../services/ReadHistoryOperationsZepto');
const { writeHistoryOperationZepto } = require('../services/writeHistoryOperationZepto');

const { getBlinkitInventoryDRR } = require('../services/Readhistoryoperationsblinkit');
const { writeHistoryOperationBlinkit } = require('../services/Writehistoryoperationblinkit');

const { getSwiggyInventoryDRR } = require('../services/Readhistoryoperationsswiggy');
const { writeHistoryOperationSwiggy } = require('../services/Writehistoryoperationswiggy');

const { getChannelDRRData } = require('../services/Readoperationschanneldrr2.2.22');
const { writeOperationsChannelDRR } = require('../services/Writeoperationschanneldrr2.2.22');

const { getInventoryDetailsData } = require('../services/Readoperationsinventorydetails2.2.17');
const { writeOperationsInventoryDetails } = require('../services/Writeoperationsinventorydetails2.2.17');

const { getB2BOrderItemLevelData } = require('../services/ReadhistoryoperationOrderItem7.3.4');
const { writeB2BOrderData } = require('../services/WritehistoryoperationOrderItem7.3.4');

const { getBlinkitMarketplaceData } = require('../services/Readoperationsblinkitmarketplace19.5.2');
const { writeBlinkitMarketplaceData } = require('../services/Writeoperationsblinkitmarketplace19.5.2');

const { getWarehouseQuickCommData } = require('../services/Readoperationswarehousequickcomm');
const { writeWarehouseQuickCommData } = require('../services/Writeoperationswarehousequickcomm');

const { writeUpcomingStocksSnapshot } = require('../services/writeUpcomingStocks');

const { writeInventoryPlanningSnapshot } = require('../services/writeInventoryPlanningSnapshot');


function startHistorySyncJob() {
  cron.schedule(
    '14 20 * * *', // every Time run on 2 pm
    // '*/15 * * * *' , // every Time run on 15 min
    async () => {
      console.log('🕒 History sync job started at:', new Date().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' }));

      try {
        // ====================   1111111 BLINKIT SYNC ====================
        console.log('\n🛒 Starting Blinkit sync...');
        const blinkitData = await getBlinkitInventoryDRR();
        console.log(`📦 Blinkit rows fetched: ${blinkitData.length}`);

        if (blinkitData && blinkitData.length > 0) {
          await writeHistoryOperationBlinkit(blinkitData);
          console.log('✅ Blinkit sync completed');
        } else {
          console.log('ℹ️ No Blinkit data found, skipping write');
        }

        // ==================== 2222222 ZEPTO SYNC ====================
        console.log('\n📱 Starting Zepto sync...');
        const zeptoData = await getZeptoInventoryDRR();
        console.log(`📦 Zepto rows fetched: ${zeptoData.length}`);

        if (zeptoData && zeptoData.length > 0) {
          await writeHistoryOperationZepto(zeptoData);
          console.log('✅ Zepto sync completed');
        } else {
          console.log('ℹ️ No Zepto data found, skipping write');
        }


        // ==================== SWIGGY SYNC ====================
        console.log('\n🍔 Starting Swiggy sync...');
        const swiggyData = await getSwiggyInventoryDRR();
        console.log(`📦 Swiggy rows fetched: ${swiggyData.length}`);

        if (swiggyData && swiggyData.length > 0) {
          await writeHistoryOperationSwiggy(swiggyData);
          console.log('✅ Swiggy sync completed');
        } else {
          console.log('ℹ️ No Swiggy data found, skipping write');
        }

        // ==================== CHANNEL DRR SYNC (2.2.22) ====================
        console.log('\n📊 Starting Channel DRR sync from operations_db...');
        const channelData = await getChannelDRRData();
        console.log(`📦 Channel DRR rows fetched: ${channelData.length}`);

        if (channelData && channelData.length > 0) {
          await writeOperationsChannelDRR(channelData);
          console.log('✅ Channel DRR sync completed');
        } else {
          console.log('ℹ️ No Channel DRR data found, skipping write');
        }

        // ==================== INVENTORY DETAILS SYNC (2.2.17) ====================
        console.log('\n📦 Starting Inventory Details sync from operations_db...');
        const inventoryData = await getInventoryDetailsData();
        console.log(`📦 Inventory Details rows fetched: ${inventoryData.length}`);

        if (inventoryData && inventoryData.length > 0) {
          await writeOperationsInventoryDetails(inventoryData);
          console.log('✅ Inventory Details sync completed');
        } else {
          console.log('ℹ️ No Inventory Details data found, skipping write');
        }

        // ==================== B2B ORDER SYNC (7.3.4) ====================
        console.log('\n📦 Starting B2B Order sync from operations_db...');
        const b2bOrderData = await getB2BOrderItemLevelData();
        console.log(`📦 B2B Order rows fetched: ${b2bOrderData.length}`);

        if (b2bOrderData && b2bOrderData.length > 0) {
          await writeB2BOrderData(b2bOrderData);
          console.log('✅ B2B Order sync completed');
        } else {
          console.log('ℹ️ No B2B Order data found, skipping write');
        }

        // ==================== BLINKIT MARKETPLACE SYNC (19.5.2) ====================
        console.log('\n🛒 Starting Blinkit Marketplace sync from operations_db...');
        const blinkitMarketplaceData = await getBlinkitMarketplaceData();
        console.log(`📦 Blinkit Marketplace rows fetched: ${blinkitMarketplaceData.length}`);

        if (blinkitMarketplaceData && blinkitMarketplaceData.length > 0) {
          await writeBlinkitMarketplaceData(blinkitMarketplaceData);
          console.log('✅ Blinkit Marketplace sync completed');
        } else {
          console.log('ℹ️ No Blinkit Marketplace data found, skipping write');
        }

        // ==================== WAREHOUSE & QUICK COMMERCE CALCULATIONS ====================
        console.log('\n🏭 Starting Warehouse & Quick Commerce calculations...');
        const warehouseQuickCommData = await getWarehouseQuickCommData();
        console.log(`📦 Warehouse & Quick Commerce rows fetched: ${warehouseQuickCommData.length}`);

        if (warehouseQuickCommData && warehouseQuickCommData.length > 0) {
          await writeWarehouseQuickCommData(warehouseQuickCommData);
          console.log('✅ Warehouse & Quick Commerce calculations completed');
        } else {
          console.log('ℹ️ No Warehouse & Quick Commerce data found, skipping calculations');
        }

        // ==================== UPCOMING STOCKS SNAPSHOT ====================
        console.log('\n📈 Starting Upcoming Stocks snapshot insert...');

        const upcomingStocksResult = await writeUpcomingStocksSnapshot();


        // ==================== INVENTORY PLANNING SNAPSHOT ====================
        console.log('\n📊 Starting Inventory Planning snapshot...');
        await writeInventoryPlanningSnapshot();
        console.log('✅ Inventory Planning snapshot completed');


        console.log(
          `✅ Upcoming Stocks snapshot completed. Rows inserted: ${upcomingStocksResult.affectedRows}`
        );


        console.log('\n🎉 All history sync completed successfully at:', new Date().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' }));
        
        // ==================== FINAL SUMMARY ====================
        console.log('\n📊 ========== SYNC SUMMARY ==========');
        console.log(`📱 Zepto Records: ${zeptoData?.length || 0}`);
        console.log(`🛒 Blinkit Records: ${blinkitData?.length || 0}`);
        console.log(`🍔 Swiggy Records: ${swiggyData?.length || 0}`);
        console.log(`📊 Channel DRR Records: ${channelData?.length || 0}`);
        console.log(`📦 Inventory Details Records: ${inventoryData?.length || 0}`);
        console.log(`📦 B2B Order Records: ${b2bOrderData?.length || 0}`);
        console.log(`🛒 Blinkit Marketplace Records: ${blinkitMarketplaceData?.length || 0}`);
        console.log(`🏭 Warehouse & Quick Commerce Records: ${warehouseQuickCommData?.length || 0}`);
        console.log(`📈 Total Records Synced: ${
          (zeptoData?.length || 0) + 
          (blinkitData?.length || 0) + 
          (swiggyData?.length || 0) + 
          (channelData?.length || 0) + 
          (inventoryData?.length || 0) + 
          (b2bOrderData?.length || 0) + 
          (blinkitMarketplaceData?.length || 0) + 
          (warehouseQuickCommData?.length || 0)
        }`);
        console.log('=====================================\n');
        
      } catch (error) {
        console.error('❌ History sync failed:', error);
        console.error('Error stack:', error.stack);
      }
    },
    {
      timezone: 'Asia/Kolkata',
    }
  );

  console.log('✅ Cron job scheduled: Runs every 15 minutes in Asia/Kolkata timezone');
}

module.exports = startHistorySyncJob;