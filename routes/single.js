const express = require('express');
const router = express.Router();
const historyDb = require('../db/historyDb');

const PAGE_SIZE_DEFAULT = 15;

router.get('/planning', async (req, res) => {
  try {
    const { page = 1, limit = PAGE_SIZE_DEFAULT } = req.query;
    const offset = (page - 1) * limit;

    /* =========================================================
       1️⃣ GET LATEST SNAPSHOT DATE
    ========================================================= */
    const [[latestSnapshot]] = await historyDb.query(`
      SELECT MAX(snapshot_date) AS snapshot_date
      FROM history_operations_db.inventory_planning_snapshot
    `);

    const SNAPSHOT_DATE = latestSnapshot?.snapshot_date;

    if (!SNAPSHOT_DATE) {
      return res.json({
        message: 'No inventory snapshot data found'
      });
    }

    /* =========================================================
       2️⃣ SUMMARY (ONLY SNAPSHOT TABLE)
    ========================================================= */
    const [[summary]] = await historyDb.query(`
      SELECT
        SUM(current_stock) AS current_stock,
        SUM(in_transit_stock) AS in_transit,
        SUM(upcoming_stock) AS upcoming_stock,
        SUM(current_stock + in_transit_stock + upcoming_stock) AS total_stock,

        SUM(
          CASE WHEN inventory_status = 'OVER_STOCK'
          THEN current_stock ELSE 0 END
        ) AS over_inventory,

        SUM(inventory_status = 'LOW_STOCK') AS stock_alert,
        SUM(inventory_status = 'PO_REQUIRED') AS po_required,

        SUM(
          CASE WHEN inventory_status = 'PO_REQUIRED'
          THEN po_intent_units ELSE 0 END
        ) AS total_po_intent_units,

        ROUND(AVG(days_of_cover), 2) AS avg_days_cover
      FROM history_operations_db.inventory_planning_snapshot
      WHERE snapshot_date = ?
    `, [SNAPSHOT_DATE]);

    /* =========================================================
       3️⃣ DAYS COVER TREND
    ========================================================= */
    const [daysCoverTrend] = await historyDb.query(`
      SELECT
        snapshot_date,
        ROUND(AVG(days_of_cover), 2) AS avg_days_cover
      FROM history_operations_db.inventory_planning_snapshot
      GROUP BY snapshot_date
      ORDER BY snapshot_date DESC
      LIMIT 7
    `);

    /* =========================================================
       4️⃣ SKU INVENTORY DETAILS (NO JOIN)
    ========================================================= */
    const [sku_inventory_details] = await historyDb.query(`
      SELECT *
      FROM history_operations_db.inventory_planning_snapshot
      WHERE snapshot_date = ?
      ORDER BY
        FIELD(inventory_status, 'LOW_STOCK', 'PO_REQUIRED', 'OVER_STOCK'),
        days_of_cover ASC
      LIMIT ? OFFSET ?
    `, [SNAPSHOT_DATE, Number(limit), Number(offset)]);

    /* =========================================================
       5️⃣ RESPONSE
    ========================================================= */
    res.json({
      snapshot_date: SNAPSHOT_DATE,
      summary: {
        total_stock: Number(summary?.total_stock || 0),
        current_stock: Number(summary?.current_stock || 0),
        in_transit: Number(summary?.in_transit || 0),
        upcoming_stock: Number(summary?.upcoming_stock || 0),
        over_inventory: Number(summary?.over_inventory || 0),
        stock_alert: Number(summary?.stock_alert || 0),
        po_required: Number(summary?.po_required || 0),
        total_po_intent_units: Number(summary?.total_po_intent_units || 0),
        avg_days_cover: Number(summary?.avg_days_cover || 0),
        days_cover_trend: daysCoverTrend.reverse()
      },
      pagination: {
        page: Number(page),
        limit: Number(limit),
        returned: sku_inventory_details.length
      },
      sku_inventory_details
    });

  } catch (error) {
    console.error('❌ /planning API error:', error);
    res.status(500).json({
      message: 'Planning API failed',
      error: error.message
    });
  }
});

module.exports = router;
