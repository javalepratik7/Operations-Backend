const express = require('express');
const router = express.Router();
const historyDb = require('../db/historyDb');

const PAGE_SIZE_DEFAULT = 15;

router.get('/planning', async (req, res) => {
  try {
    const { page = 1, limit = PAGE_SIZE_DEFAULT } = req.query;
    const offset = (page - 1) * limit;

    /* =========================================================
       SUMMARY (FROM inventory_planning_snapshot)
    ========================================================= */
    const summaryQuery = `
      SELECT
        SUM(current_stock) AS current_stock,
        SUM(in_transit_stock) AS in_transit,
        SUM(upcoming_stock) AS upcoming_stock,
        SUM(current_stock + in_transit_stock + upcoming_stock) AS total_stock,

        SUM(
          CASE WHEN inventory_status = 'OVER_STOCK'
          THEN current_stock ELSE 0 END
        ) AS over_inventory,

        SUM(current_stock * drr_30d) AS inventory_cogs,

        SUM(inventory_status = 'LOW_STOCK') AS stock_alert,
        SUM(inventory_status = 'PO_REQUIRED') AS po_required,

        ROUND(AVG(days_of_cover), 2) AS avg_days_cover
      FROM history_operations_db.inventory_planning_snapshot
      WHERE snapshot_date = CURDATE()
    `;
    const [[summary]] = await historyDb.query(summaryQuery);

    /* =========================================================
       DAYS COVER TREND (LAST 7 SNAPSHOTS)
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
       INVENTORY DISTRIBUTION (LATEST SKU SNAPSHOT)
    ========================================================= */
    const [[distribution]] = await historyDb.query(`
      SELECT
        SUM(increff_units) AS increff,
        SUM(kvt_units) AS kv_traders,
        SUM(pc_units) AS processing,
        SUM(fba_units_gb + fba_bundled_units) AS fba,
        SUM(fbf_units_gb + fbf_bundled_units) AS fbf,
        SUM(myntra_units_gb + myntra_bundled_units) AS myntra,
        SUM(rk_world_stock) AS rk_world
      FROM history_operations_db.sku_inventory_report
      WHERE created_at = (
        SELECT MAX(created_at)
        FROM history_operations_db.sku_inventory_report
      )
    `);

    /* =========================================================
       QUICK COMMERCE SPEED
    ========================================================= */
    const [[quickComm]] = await historyDb.query(`
      SELECT
        SUM(Instamart_B2B_drr_30d) AS instamart,
        SUM(Zepto_B2B_drr_30d) AS zepto,
        SUM(blinkit_b2b_drr_30d) AS blinkit_b2b,
        SUM(blinkit_marketplace_speed_30_days) AS blinkit_b2c
      FROM history_operations_db.sku_inventory_report
      WHERE created_at = (
        SELECT MAX(created_at)
        FROM history_operations_db.sku_inventory_report
      )
    `);

    /* =========================================================
       SKU INVENTORY DETAILS (ORDERED BY STATUS)
    ========================================================= */
    const skuQuery = `
      SELECT *
      FROM history_operations_db.inventory_planning_snapshot
      WHERE snapshot_date = CURDATE()
      ORDER BY
        FIELD(inventory_status, 'LOW_STOCK', 'PO_REQUIRED', 'OVER_STOCK'),
        days_of_cover ASC
      LIMIT ? OFFSET ?
    `;
    const [sku_inventory_details] = await historyDb.query(
      skuQuery,
      [Number(limit), Number(offset)]
    );

    /* =========================================================
       FILTER DATA
       (get all unique values for frontend filters)
    ========================================================= */
    const [brands] = await historyDb.query(
      `SELECT DISTINCT brand FROM history_operations_db.sku_inventory_report ORDER BY brand`
    );
    const [vendors] = await historyDb.query(
      `SELECT DISTINCT vendor_name AS vendor FROM history_operations_db.sku_inventory_report ORDER BY vendor_name`
    );
    const [categories] = await historyDb.query(
      `SELECT DISTINCT category FROM history_operations_db.sku_inventory_report ORDER BY category`
    );
    const [locations] = await historyDb.query(
      `SELECT DISTINCT swiggy_city AS location FROM history_operations_db.sku_inventory_report ORDER BY swiggy_city`
    );

    /* =========================================================
       RESPONSE
    ========================================================= */
    res.json({
      summary: {
        total_stock: Number(summary?.total_stock || 0),
        current_stock: Number(summary?.current_stock || 0),
        in_transit: Number(summary?.in_transit || 0),
        upcoming_stock: Number(summary?.upcoming_stock || 0),
        over_inventory: Number(summary?.over_inventory || 0),
        inventory_cogs: Number(summary?.inventory_cogs || 0),
        stock_alert: Number(summary?.stock_alert || 0),
        po_required: Number(summary?.po_required || 0),
        avg_days_cover: Number(summary?.avg_days_cover || 0),

        days_cover_trend: daysCoverTrend.reverse(),

        inventory_distribution: {
          increff: Number(distribution?.increff || 0),
          kv_traders: Number(distribution?.kv_traders || 0),
          processing: Number(distribution?.processing || 0),
          fba: Number(distribution?.fba || 0),
          fbf: Number(distribution?.fbf || 0),
          myntra: Number(distribution?.myntra || 0),
          rk_world: Number(distribution?.rk_world || 0)
        },

        quick_commerce_speed: {
          instamart: Number(quickComm?.instamart || 0),
          zepto: Number(quickComm?.zepto || 0),
          blinkit_b2b: Number(quickComm?.blinkit_b2b || 0),
          blinkit_b2c: Number(quickComm?.blinkit_b2c || 0)
        }
      },

      filters: {
        brands: brands.map(b => b.brand),
        vendors: vendors.map(v => v.vendor),
        categories: categories.map(c => c.category),
        locations: locations.map(l => l.location)
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
