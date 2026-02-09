const express = require('express');
const router = express.Router();
const historyDb = require('../db/historyDb');

const PAGE_SIZE_DEFAULT = 15;

router.get('/planning', async (req, res) => {
  try {
    const {
      page = 1,
      limit = PAGE_SIZE_DEFAULT,
      brand,
      vendor,
      category,
      location
    } = req.query;

    const offset = (page - 1) * limit;

    /* =========================================================
       1️⃣ LATEST SNAPSHOT DATE
    ========================================================= */
    const [[latestSnapshot]] = await historyDb.query(`
      SELECT MAX(snapshot_date) AS snapshot_date
      FROM history_operations_db.inventory_planning_snapshot
    `);

    const SNAPSHOT_DATE = latestSnapshot?.snapshot_date;

    if (!SNAPSHOT_DATE) {
      return res.json({ message: 'No snapshot data found' });
    }

    /* =========================================================
       2️⃣ DYNAMIC FILTER CONDITIONS
    ========================================================= */
    const filters = [];
    const values = [SNAPSHOT_DATE];

    if (brand) {
      filters.push('sir.brand = ?');
      values.push(brand);
    }

    if (vendor) {
      filters.push('sir.vendor_name = ?');
      values.push(vendor);
    }

    if (category) {
      filters.push('sir.category = ?');
      values.push(category);
    }

    if (location) {
      filters.push('sir.swiggy_city = ?');
      values.push(location);
    }

    const FILTER_SQL = filters.length
      ? `AND ${filters.join(' AND ')}`
      : '';

    /* =========================================================
       3️⃣ SUMMARY (FILTER AWARE)
    ========================================================= */
    const [[summary]] = await historyDb.query(`
      SELECT
        SUM(ips.current_stock) AS current_stock,
        SUM(ips.in_transit_stock) AS in_transit,
        SUM(ips.upcoming_stock) AS upcoming_stock,
        SUM(ips.current_stock + ips.in_transit_stock + ips.upcoming_stock) AS total_stock,

        SUM(
          CASE WHEN ips.inventory_status = 'OVER_STOCK'
          THEN ips.current_stock ELSE 0 END
        ) AS over_inventory,

        SUM(ips.inventory_status = 'LOW_STOCK') AS stock_alert,
        SUM(ips.inventory_status = 'PO_REQUIRED') AS po_required,

        SUM(
          CASE WHEN ips.inventory_status = 'PO_REQUIRED'
          THEN ips.po_intent_units ELSE 0 END
        ) AS total_po_intent_units,

        ROUND(AVG(ips.days_of_cover), 2) AS avg_days_cover
      FROM history_operations_db.inventory_planning_snapshot ips
      JOIN (
        SELECT sir1.*
        FROM history_operations_db.sku_inventory_report sir1
        INNER JOIN (
          SELECT ean_code, MAX(created_at) AS max_created
          FROM history_operations_db.sku_inventory_report
          GROUP BY ean_code
        ) sir2
          ON sir1.ean_code = sir2.ean_code
         AND sir1.created_at = sir2.max_created
      ) sir ON sir.ean_code = ips.ean_code
      WHERE ips.snapshot_date = ?
      ${FILTER_SQL}
    `, values);

    /* =========================================================
       4️⃣ DAYS COVER TREND (LAST 7 DAYS, FILTER AWARE)
    ========================================================= */
    const [daysCoverTrend] = await historyDb.query(`
      SELECT
        ips.snapshot_date,
        ROUND(AVG(ips.days_of_cover), 2) AS avg_days_cover
      FROM history_operations_db.inventory_planning_snapshot ips
      JOIN (
        SELECT sir1.*
        FROM history_operations_db.sku_inventory_report sir1
        INNER JOIN (
          SELECT ean_code, MAX(created_at) AS max_created
          FROM history_operations_db.sku_inventory_report
          GROUP BY ean_code
        ) sir2
          ON sir1.ean_code = sir2.ean_code
         AND sir1.created_at = sir2.max_created
      ) sir ON sir.ean_code = ips.ean_code
      WHERE ips.snapshot_date >= CURDATE() - INTERVAL 6 DAY
      ${FILTER_SQL.replace('AND', 'AND')}
      GROUP BY ips.snapshot_date
      ORDER BY ips.snapshot_date ASC
    `, values.slice(1)); // exclude snapshot_date for trend

    /* =========================================================
       5️⃣ QUICK COMMERCE SPEED (FILTER AWARE)
    ========================================================= */
    const [[quickCommerce]] = await historyDb.query(`
      SELECT
        SUM(sir.swiggy_speed) AS swiggy,
        SUM(sir.zepto_speed) AS zepto,
        SUM(sir.blinkit_b2b_speed) AS blinkit_b2b,
        SUM(sir.blinkit_marketplace_speed) AS blinkit_b2c
      FROM (
        SELECT sir1.*
        FROM history_operations_db.sku_inventory_report sir1
        INNER JOIN (
          SELECT ean_code, MAX(created_at) AS max_created
          FROM history_operations_db.sku_inventory_report
          GROUP BY ean_code
        ) sir2
          ON sir1.ean_code = sir2.ean_code
         AND sir1.created_at = sir2.max_created
      ) sir
      WHERE 1 = 1
      ${FILTER_SQL.replace(/sir\./g, 'sir.')}
    `, values.slice(1));

    /* =========================================================
       6️⃣ INVENTORY DISTRIBUTION (FILTER AWARE)
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
          SELECT ean_code, MAX(created_at) AS max_created
          FROM history_operations_db.sku_inventory_report
          GROUP BY ean_code
        ) sir2
          ON sir1.ean_code = sir2.ean_code
         AND sir1.created_at = sir2.max_created
      ) sir
      WHERE 1 = 1
      ${FILTER_SQL.replace(/sir\./g, 'sir.')}
    `, values.slice(1));

    /* =========================================================
       7️⃣ SKU DETAILS (FILTER AWARE + PAGINATION)
    ========================================================= */
    const [sku_inventory_details] = await historyDb.query(`
      SELECT ips.*
      FROM history_operations_db.inventory_planning_snapshot ips
      JOIN (
        SELECT sir1.*
        FROM history_operations_db.sku_inventory_report sir1
        INNER JOIN (
          SELECT ean_code, MAX(created_at) AS max_created
          FROM history_operations_db.sku_inventory_report
          GROUP BY ean_code
        ) sir2
          ON sir1.ean_code = sir2.ean_code
         AND sir1.created_at = sir2.max_created
      ) sir ON sir.ean_code = ips.ean_code
      WHERE ips.snapshot_date = ?
      ${FILTER_SQL}
      ORDER BY
        FIELD(ips.inventory_status, 'LOW_STOCK', 'PO_REQUIRED', 'OVER_STOCK'),
        ips.days_of_cover ASC
      LIMIT ? OFFSET ?
    `, [...values, Number(limit), Number(offset)]);

    /* =========================================================
       8️⃣ FILTER OPTIONS
    ========================================================= */
    const [brands] = await historyDb.query(`
      SELECT DISTINCT brand FROM history_operations_db.sku_inventory_report
      WHERE brand IS NOT NULL ORDER BY brand
    `);

    const [vendors] = await historyDb.query(`
      SELECT DISTINCT vendor_name AS vendor
      FROM history_operations_db.sku_inventory_report
      WHERE vendor_name IS NOT NULL ORDER BY vendor_name
    `);

    const [categories] = await historyDb.query(`
      SELECT DISTINCT category
      FROM history_operations_db.sku_inventory_report
      WHERE category IS NOT NULL ORDER BY category
    `);

    const [locations] = await historyDb.query(`
      SELECT DISTINCT swiggy_city AS location
      FROM history_operations_db.sku_inventory_report
      WHERE swiggy_city IS NOT NULL ORDER BY swiggy_city
    `);

    /* =========================================================
       9️⃣ RESPONSE
    ========================================================= */
    res.json({
      snapshot_date: SNAPSHOT_DATE,

      summary: {
        ...summary,
        quick_commerce_speed: quickCommerce,
        inventory_distribution: distribution,
        days_cover_trend: daysCoverTrend
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
