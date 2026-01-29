const cron = require('node-cron');

const { getZeptoInventoryDRR } = require('../services/ReadHistoryOperationsZepto');
const {
  writeHistoryOperationZepto,
} = require('../services/writeHistoryOperationZepto');

const { getBlinkitInventoryDRR } = require('../services/Readhistoryoperationsblinkit');
const {
  writeHistoryOperationBlinkit,
} = require('../services/Writehistoryoperationblinkit');

function startHistorySyncJob() {
  cron.schedule(
    '* * * * *', // every 1 minute
    async () => {
      console.log('🕒 History sync job started');

      try {
        // ==================== ZEPTO SYNC ====================
        console.log('\n📱 Starting Zepto sync...');
        const zeptoData = await getZeptoInventoryDRR();
        console.log(`📦 Zepto rows fetched: ${zeptoData.length}`);

        if (zeptoData && zeptoData.length > 0) {
          await writeHistoryOperationZepto(zeptoData);
          console.log('✅ Zepto sync completed');
        } else {
          console.log('ℹ️ No Zepto data found, skipping write');
        }

        // ==================== BLINKIT SYNC ====================
        console.log('\n🛒 Starting Blinkit sync...');
        const blinkitData = await getBlinkitInventoryDRR();
        console.log(`📦 Blinkit rows fetched: ${blinkitData.length}`);

        if (blinkitData && blinkitData.length > 0) {
          await writeHistoryOperationBlinkit(blinkitData);
          console.log('✅ Blinkit sync completed');
        } else {
          console.log('ℹ️ No Blinkit data found, skipping write');
        }

        console.log('\n🎉 All history sync completed successfully');
      } catch (error) {
        console.error('❌ History sync failed:', error);
      }
    },
    {
      timezone: 'Asia/Kolkata',
    }
  );
}

module.exports = startHistorySyncJob;