const express = require('express');
const router = express.Router();

// imports
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



// ==================== CHANNEL DRR SYNC ENDPOINT ====================
router.get('/sync-channel-drr', async (req, res) => {
  try {
    console.log('\n📊 Starting Channel DRR sync from operations_db...');

    const channelData = await getChannelDRRData();
    console.log(`📦 Channel DRR rows fetched: ${channelData.length}`);

    if (channelData && channelData.length > 0) {
      await writeOperationsChannelDRR(channelData);
      console.log('✅ Channel DRR sync completed');

      return res.status(200).json({
        success: true,
        message: 'Channel DRR sync executed successfully',
        rowsFetched: channelData.length
      });
    } else {
      console.log('ℹ️ No Channel DRR data found, skipping write');

      return res.status(200).json({
        success: true,
        message: 'No Channel DRR data found to sync',
        rowsFetched: 0
      });
    }

  } catch (error) {
    console.error('❌ Channel DRR sync failed:', error.message);
    console.error('Full error:', error);

    return res.status(500).json({
      success: false,
      message: 'Channel DRR sync failed',
      error: error.message
    });
  }
});


// ==================== INVENTORY DETAILS SYNC ENDPOINT ====================
router.get('/2217', async (req, res) => {
  try {
    console.log('\n📦 Starting Inventory Details sync from operations_db...');

    const inventoryData = await getInventoryDetailsData();
    console.log(`📦 Inventory Details rows fetched: ${inventoryData.length}`); // ✅ Fixed: was channelData

    if (inventoryData && inventoryData.length > 0) {
      await writeOperationsInventoryDetails(inventoryData);
      console.log('✅ Inventory Details sync completed');

      return res.status(200).json({
        success: true,
        message: 'Inventory Details sync executed successfully',
        rowsFetched: inventoryData.length // ✅ Fixed: was channelData
      });
    } else {
      console.log('ℹ️ No Inventory Details data found, skipping write');

      return res.status(200).json({
        success: true,
        message: 'No Inventory Details data found to sync',
        rowsFetched: 0
      });
    }

  } catch (error) {
    console.error('❌ Inventory Details sync failed:', error.message);
    console.error('Full error:', error);

    return res.status(500).json({
      success: false,
      message: 'Inventory Details sync failed',
      error: error.message
    });
  }
});


// ==================== COMBINED SYNC ENDPOINT ====================
router.get('/sync-all-operations', async (req, res) => {
  try {
    const results = {
      channelDRR: { success: false, rowsFetched: 0, error: null },
      inventoryDetails: { success: false, rowsFetched: 0, error: null }
    };

    // Sync Channel DRR
    try {
      console.log('\n📊 Starting Channel DRR sync from operations_db...');
      const channelData = await getChannelDRRData();
      console.log(`📦 Channel DRR rows fetched: ${channelData.length}`);

      if (channelData && channelData.length > 0) {
        await writeOperationsChannelDRR(channelData);
        console.log('✅ Channel DRR sync completed');
        results.channelDRR = { success: true, rowsFetched: channelData.length, error: null };
      } else {
        console.log('ℹ️ No Channel DRR data found');
        results.channelDRR = { success: true, rowsFetched: 0, error: 'No data found' };
      }
    } catch (error) {
      console.error('❌ Channel DRR sync failed:', error.message);
      results.channelDRR.error = error.message;
    }

    // Sync Inventory Details
    try {
      console.log('\n📦 Starting Inventory Details sync from operations_db...');
      const inventoryData = await getInventoryDetailsData();
      console.log(`📦 Inventory Details rows fetched: ${inventoryData.length}`);

      if (inventoryData && inventoryData.length > 0) {
        await writeOperationsInventoryDetails(inventoryData);
        console.log('✅ Inventory Details sync completed');
        results.inventoryDetails = { success: true, rowsFetched: inventoryData.length, error: null };
      } else {
        console.log('ℹ️ No Inventory Details data found');
        results.inventoryDetails = { success: true, rowsFetched: 0, error: 'No data found' };
      }
    } catch (error) {
      console.error('❌ Inventory Details sync failed:', error.message);
      results.inventoryDetails.error = error.message;
    }

    console.log('\n🎉 All operations sync completed');

    // Determine overall success
    const overallSuccess = results.channelDRR.success || results.inventoryDetails.success;

    return res.status(overallSuccess ? 200 : 500).json({
      success: overallSuccess,
      message: 'Operations sync completed',
      results: results
    });

  } catch (error) {
    console.error('❌ Overall sync failed:', error.message);

    return res.status(500).json({
      success: false,
      message: 'Operations sync failed',
      error: error.message
    });
  }
});


// ==================== B2B ORDER SYNC ENDPOINT (NEW) ====================
router.get('/734', async (req, res) => {
  try {
    console.log('\n📦 Starting B2B Order sync from operations_db...');

    const b2bOrderData = await getB2BOrderItemLevelData();
    console.log(`📦 B2B Order rows fetched: ${b2bOrderData.length}`);

    if (b2bOrderData && b2bOrderData.length > 0) {
      await writeB2BOrderData(b2bOrderData);
      console.log('✅ B2B Order sync completed');

      return res.status(200).json({
        success: true,
        message: 'B2B Order sync executed successfully',
        rowsFetched: b2bOrderData.length
      });
    } else {
      console.log('ℹ️ No B2B Order data found, skipping write');

      return res.status(200).json({
        success: true,
        message: 'No B2B Order data found to sync',
        rowsFetched: 0
      });
    }

  } catch (error) {
    console.error('❌ B2B Order sync failed:', error.message);
    console.error('Full error:', error);

    return res.status(500).json({
      success: false,
      message: 'B2B Order sync failed',
      error: error.message
    });
  }
});


// ==================== BLINKIT MARKETPLACE SYNC ENDPOINT (NEW) ====================
router.get('/blinkit-marketplace', async (req, res) => {
  try {
    console.log('\n🛒 Starting Blinkit Marketplace sync from operations_db...');

    const blinkitData = await getBlinkitMarketplaceData();
    console.log(`📦 Blinkit Marketplace rows fetched: ${blinkitData.length}`);

    if (blinkitData && blinkitData.length > 0) {
      await writeBlinkitMarketplaceData(blinkitData);
      console.log('✅ Blinkit Marketplace sync completed');

      return res.status(200).json({
        success: true,
        message: 'Blinkit Marketplace sync executed successfully',
        rowsFetched: blinkitData.length
      });
    } else {
      console.log('ℹ️ No Blinkit Marketplace data found, skipping write');

      return res.status(200).json({
        success: true,
        message: 'No Blinkit Marketplace data found to sync',
        rowsFetched: 0
      });
    }

  } catch (error) {
    console.error('❌ Blinkit Marketplace sync failed:', error.message);
    console.error('Full error:', error);

    return res.status(500).json({
      success: false,
      message: 'Blinkit Marketplace sync failed',
      error: error.message
    });
  }
});


// ==================== WAREHOUSE & QUICK COMMERCE SYNC ENDPOINT (NEW) ====================
router.get('/warehouse-quickcomm', async (req, res) => {
  try {
    console.log('\n🏭 Starting Warehouse & Quick Commerce calculations...');

    const warehouseQuickCommData = await getWarehouseQuickCommData();
    console.log(`📦 Warehouse & Quick Commerce rows fetched: ${warehouseQuickCommData.length}`);

    if (warehouseQuickCommData && warehouseQuickCommData.length > 0) {
      await writeWarehouseQuickCommData(warehouseQuickCommData);
      console.log('✅ Warehouse & Quick Commerce calculations completed');

      return res.status(200).json({
        success: true,
        message: 'Warehouse & Quick Commerce calculations executed successfully',
        rowsFetched: warehouseQuickCommData.length
      });
    } else {
      console.log('ℹ️ No Warehouse & Quick Commerce data found, skipping calculations');

      return res.status(200).json({
        success: true,
        message: 'No Warehouse & Quick Commerce data found to process',
        rowsFetched: 0
      });
    }

  } catch (error) {
    console.error('❌ Warehouse & Quick Commerce calculations failed:', error.message);
    console.error('Full error:', error);

    return res.status(500).json({
      success: false,
      message: 'Warehouse & Quick Commerce calculations failed',
      error: error.message
    });
  }
});


// ==================== UPCOMING STOCKS SNAPSHOT SYNC (NEW) ====================
router.get('/upcoming-stocks', async (req, res) => {
  try {
    console.log('\n📈 Starting Upcoming Stocks snapshot insert...');

    const result = await writeUpcomingStocksSnapshot();

    console.log(
      `✅ Upcoming Stocks snapshot completed. Rows inserted: ${result.affectedRows}`
    );

    return res.status(200).json({
      success: true,
      message: 'Upcoming Stocks snapshot executed successfully',
      rowsInserted: result.affectedRows
    });

  } catch (error) {
    console.error('❌ Upcoming Stocks snapshot failed:', error.message);
    console.error('Full error:', error);

    return res.status(500).json({
      success: false,
      message: 'Upcoming Stocks snapshot failed',
      error: error.message
    });
  }
});

// ==================== INVENTORY PLANNING SNAPSHOT (TEST ONLY) ====================
router.get('/inventory-planning-test', async (req, res) => {
  try {
    console.log('\n🧪 TEST: Starting Inventory Planning snapshot...');

    const result = await writeInventoryPlanningSnapshot();

    console.log('✅ TEST: Inventory Planning snapshot completed');

    return res.status(200).json({
      success: true,
      message: 'Inventory Planning snapshot test executed successfully',
      result: result || null
    });

  } catch (error) {
    console.error('❌ TEST: Inventory Planning snapshot failed:', error.message);
    console.error('Full error:', error);

    return res.status(500).json({
      success: false,
      message: 'Inventory Planning snapshot test failed',
      error: error.message
    });
  }
});




module.exports = router;