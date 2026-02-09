const express = require('express');
const router = express.Router();
const historyDb = require('../db/historyDb');

const PAGE_SIZE_DEFAULT = 15;

router.get('/planning', async (req, res) => {
  try {
    const { page = 1, limit = PAGE_SIZE_DEFAULT } = req.query;
    const offset = (page - 1) * limit;

    /* =========================================================
       1️⃣ LATEST SNAPSHOT DATE
    ========================================================= */
    const [[latestSnapshot]] = await historyDb.query(`
      SELECT MAX(snapshot_date) snapshot_date
      FROM history_operations_db.inventory_planning_snapshot
    `);

    const SNAPSHOT_DATE = latestSnapshot?.snapshot_date;

    if (!SNAPSHOT_DATE) {
      return res.json({ message: 'No snapshot data found' });
    }

    /* =========================================================
       2️⃣ SUMMARY
    ========================================================= */
    const [[summary]] = await historyDb.query(`
      SELECT
        SUM(current_stock) current_stock,
        SUM(in_transit_stock) in_transit,
        SUM(upcoming_stock) upcoming_stock,
        SUM(current_stock + in_transit_stock + upcoming_stock) total_stock,

        SUM(CASE WHEN inventory_status='OVER_STOCK'
          THEN current_stock ELSE 0 END) over_inventory,

        SUM(inventory_status='LOW_STOCK') stock_alert,
        SUM(inventory_status='PO_REQUIRED') po_required,

        SUM(CASE WHEN inventory_status='PO_REQUIRED'
          THEN po_intent_units ELSE 0 END) total_po_intent_units,

        ROUND(AVG(days_of_cover),2) avg_days_cover
      FROM history_operations_db.inventory_planning_snapshot
      WHERE snapshot_date=?
    `, [SNAPSHOT_DATE]);

    /* =========================================================
       3️⃣ DAYS COVER TREND (LAST 7 CALENDAR DAYS)
    ========================================================= */
    const [daysCoverTrend] = await historyDb.query(`
      SELECT
        DATE(snapshot_date) snapshot_date,
        ROUND(AVG(days_of_cover),2) avg_days_cover
      FROM history_operations_db.inventory_planning_snapshot
      WHERE snapshot_date >= CURDATE() - INTERVAL 6 DAY
      GROUP BY DATE(snapshot_date)
      ORDER BY snapshot_date ASC
    `);

    /* =========================================================
       4️⃣ QUICK COMMERCE SPEED
    ========================================================= */
    const [[quickCommerce]] = await historyDb.query(`
      SELECT
        SUM(swiggy_speed) swiggy,
        SUM(zepto_speed) zepto,
        SUM(blinkit_b2b_speed) blinkit_b2b,
        SUM(blinkit_marketplace_speed) blinkit_b2c
      FROM (
        SELECT sir1.*
        FROM history_operations_db.sku_inventory_report sir1
        INNER JOIN (
          SELECT ean_code, MAX(created_at) max_created
          FROM history_operations_db.sku_inventory_report
          GROUP BY ean_code
        ) sir2
        ON sir1.ean_code=sir2.ean_code
        AND sir1.created_at=sir2.max_created
      ) latest
    `);

    /* =========================================================
       5️⃣ INVENTORY DISTRIBUTION
    ========================================================= */
    const [[distribution]] = await historyDb.query(`
      SELECT
        SUM(increff_units) increff,
        SUM(kvt_units) kv_traders,
        SUM(pc_units) processing,
        SUM(fba_units_gb + fba_bundled_units) fba,
        SUM(fbf_units_gb + fbf_bundled_units) fbf,
        SUM(myntra_units_gb + myntra_bundled_units) myntra,
        SUM(rk_world_stock) rk_world
      FROM (
        SELECT sir1.*
        FROM history_operations_db.sku_inventory_report sir1
        INNER JOIN (
          SELECT ean_code, MAX(created_at) max_created
          FROM history_operations_db.sku_inventory_report
          GROUP BY ean_code
        ) sir2
        ON sir1.ean_code=sir2.ean_code
        AND sir1.created_at=sir2.max_created
      ) latest
    `);

    /* =========================================================
       6️⃣ SKU DETAILS (ONLY SNAPSHOT TABLE)
    ========================================================= */
    const [sku_inventory_details] = await historyDb.query(`
      SELECT *
      FROM history_operations_db.inventory_planning_snapshot
      WHERE snapshot_date=?
      ORDER BY
        FIELD(inventory_status,'LOW_STOCK','PO_REQUIRED','OVER_STOCK'),
        days_of_cover ASC
      LIMIT ? OFFSET ?
    `, [SNAPSHOT_DATE, Number(limit), Number(offset)]);

    /* =========================================================
       7️⃣ RESPONSE
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

        quick_commerce_speed: {
          swiggy: Number(quickCommerce?.swiggy || 0),
          zepto: Number(quickCommerce?.zepto || 0),
          blinkit_b2b: Number(quickCommerce?.blinkit_b2b || 0),
          blinkit_b2c: Number(quickCommerce?.blinkit_b2c || 0)
        },

        inventory_distribution: {
          increff: Number(distribution?.increff || 0),
          kv_traders: Number(distribution?.kv_traders || 0),
          processing: Number(distribution?.processing || 0),
          fba: Number(distribution?.fba || 0),
          fbf: Number(distribution?.fbf || 0),
          myntra: Number(distribution?.myntra || 0),
          rk_world: Number(distribution?.rk_world || 0)
        },

        days_cover_trend: daysCoverTrend
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
