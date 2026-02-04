const express = require('express');
const router = express.Router();

const historyDb = require('../db/historyDb');

router.get('/info', async (req, res) => {
    try {
        const { ean } = req.query;
        if (!ean) {
            return res.status(400).json({ error: 'ean is required' });
        }

        // 1️⃣ Latest inventory snapshot
        const [[inventory]] = await historyDb.query(`
        SELECT
            ean_code,
            total_stock,
            lead_time_vendor_lt,
            COALESCE(quickcomm_speed_30_days, 0)
            + COALESCE(warehouse_speed_30_days, 0) AS drr_30d
        FROM sku_inventory_report
        WHERE ean_code = ?
        ORDER BY created_at DESC
        LIMIT 1
        `, [ean]);


        if (!inventory) {
            return res.status(404).json({ error: 'EAN not found' });
        }

        // const { drr_30d, total_stock, lead_time_vendor_lt } = inventory;
        // ✅ Explicit casting (VERY IMPORTANT)
        const drr_30d = Number(inventory.drr_30d) || 0;
        const total_stock = Number(inventory.total_stock) || 0;
        const lead_time_vendor_lt = inventory.lead_time_vendor_lt;

        // 2️⃣ Upcoming + InTransit stock
        const [[stockData]] = await historyDb.query(`
      SELECT
        GREATEST(
          SUM(CASE WHEN external_order_code LIKE '%main%' THEN order_quantity ELSE 0 END)
          -
          SUM(CASE WHEN external_order_code LIKE '%sub%' THEN order_quantity ELSE 0 END),
          0
        ) AS upcoming_stock,
        SUM(\`In Transit Quantity\`) AS in_transit_stock
      FROM history_operations_db.upcoming_stocks
      WHERE ean = ?
    `, [ean]);

        const upcomingStock = stockData?.upcoming_stock || 0;
        const inTransitStock = stockData?.in_transit_stock || 0;

        // 3️⃣ Calculations
        const reorderLevel = drr_30d * (lead_time_vendor_lt + 40);

        let stockStatus = 'OK';
        if (total_stock + inTransitStock + upcomingStock <= reorderLevel) {
            stockStatus = 'PO_REQUIRED';
        } else if (total_stock + inTransitStock <= reorderLevel) {
            stockStatus = 'LOW_STOCK';
        } else if (total_stock + inTransitStock >= reorderLevel) {
            stockStatus = 'OVER_STOCK';
        }

        const daysOfCoverNoPO =
            drr_30d > 0 ? (total_stock + inTransitStock) / drr_30d : null;

        const daysOfCoverWithPO =
            drr_30d > 0
                ? (total_stock + inTransitStock + upcomingStock) / drr_30d
                : null;

        // 4️⃣ Response
        res.json({
            ean,
            drr_30d,
            current_stock: total_stock,
            in_transit_stock: inTransitStock,
            upcoming_stock: upcomingStock,
            lead_time_days: lead_time_vendor_lt,
            safety_stock_days: 40,
            reorder_level: reorderLevel,
            stock_status: stockStatus,
            days_of_cover_without_po: daysOfCoverNoPO,
            days_of_cover_with_po: daysOfCoverWithPO,
        });

    } catch (error) {
        console.error('❌ /info API error:', error);
        res.status(500).json({ error: 'Internal server error' });
    }
});

module.exports = router;
