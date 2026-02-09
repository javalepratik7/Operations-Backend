const express = require('express');
const router = express.Router();
const historyDb = require('../db/historyDb');

const PAGE_SIZE_DEFAULT = 15;

router.get('/join', async (req, res) => {
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
       2️⃣ SUMMARY
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
       3️⃣ DAYS COVER TREND (LAST 7 SNAPSHOTS)
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
       4️⃣ INVENTORY DISTRIBUTION (LATEST SKU PER EAN)
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
      FROM (
        SELECT sir1.*
        FROM history_operations_db.sku_inventory_report sir1
        INNER JOIN (
          SELECT ean_code, MAX(created_at) AS max_created_at
          FROM history_operations_db.sku_inventory_report
          GROUP BY ean_code
        ) sir2
          ON sir1.ean_code = sir2.ean_code
         AND sir1.created_at = sir2.max_created_at
      ) latest_sir
    `);

    /* =========================================================
       5️⃣ SKU INVENTORY DETAILS (LATEST SNAPSHOT)
    ========================================================= */
    const skuQuery = `
      SELECT
        ips.id,
        ips.ean_code,
        ips.drr_30d,
        ips.lead_time_days,
        ips.safety_stock_days,
        ips.current_stock,
        ips.in_transit_stock,
        ips.upcoming_stock,
        ips.reorder_level,
        ips.po_intent_units,
        ips.days_of_cover,
        ips.days_of_cover_with_po,
        ips.inventory_status,
        ips.snapshot_date,

        sir.brand,
        sir.gb_sku,
        sir.asin,
        sir.multiple_listing,
        sir.product_title,
        sir.category,
        sir.mrp,
        sir.selling_price,
        sir.cogs,
        sir.pack_size,
        sir.lead_time_vendor_lt,
        sir.vendor_name,
        sir.launch_date,
        sir.is_bundle,

        sir.increff_units,
        sir.kvt_units,
        sir.pc_units,
        sir.fba_units_gb,
        sir.fba_bundled_units,
        sir.fbf_units_gb,
        sir.fbf_bundled_units,
        sir.myntra_units_gb,
        sir.myntra_bundled_units,
        sir.rk_world_stock,

        sir.marketplace_total_stock,
        sir.marketplace_total_speed,
        sir.marketplace_total_days_of_cover,

        sir.total_stock,
        sir.total_speed,
        sir.total_day_cover
      FROM history_operations_db.inventory_planning_snapshot ips
      LEFT JOIN (
        SELECT sir1.*
        FROM history_operations_db.sku_inventory_report sir1
        INNER JOIN (
          SELECT ean_code, MAX(created_at) AS max_created_at
          FROM history_operations_db.sku_inventory_report
          GROUP BY ean_code
        ) sir2
          ON sir1.ean_code = sir2.ean_code
         AND sir1.created_at = sir2.max_created_at
      ) sir
        ON sir.ean_code = ips.ean_code
      WHERE ips.snapshot_date = ?
      ORDER BY
        FIELD(ips.inventory_status, 'LOW_STOCK', 'PO_REQUIRED', 'OVER_STOCK'),
        ips.days_of_cover ASC
      LIMIT ? OFFSET ?
    `;

    const [sku_inventory_details] = await historyDb.query(
      skuQuery,
      [SNAPSHOT_DATE, Number(limit), Number(offset)]
    );

    /* =========================================================
       6️⃣ FILTERS
    ========================================================= */
    const [brands] = await historyDb.query(`
      SELECT DISTINCT brand
      FROM history_operations_db.sku_inventory_report
      WHERE brand IS NOT NULL
      ORDER BY brand
    `);

    const [vendors] = await historyDb.query(`
      SELECT DISTINCT vendor_name AS vendor
      FROM history_operations_db.sku_inventory_report
      WHERE vendor_name IS NOT NULL
      ORDER BY vendor_name
    `);

    const [categories] = await historyDb.query(`
      SELECT DISTINCT category
      FROM history_operations_db.sku_inventory_report
      WHERE category IS NOT NULL
      ORDER BY category
    `);

    const [locations] = await historyDb.query(`
      SELECT DISTINCT swiggy_city AS location
      FROM history_operations_db.sku_inventory_report
      WHERE swiggy_city IS NOT NULL
      ORDER BY swiggy_city
    `);

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
        days_cover_trend: daysCoverTrend.reverse(),
        inventory_distribution: {
          increff: Number(distribution?.increff || 0),
          kv_traders: Number(distribution?.kv_traders || 0),
          processing: Number(distribution?.processing || 0),
          fba: Number(distribution?.fba || 0),
          fbf: Number(distribution?.fbf || 0),
          myntra: Number(distribution?.myntra || 0),
          rk_world: Number(distribution?.rk_world || 0)
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
