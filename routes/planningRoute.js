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

        ROUND(AVG(ips.days_of_cover), 2) AS avg_days_cover,
        
        SUM(COALESCE(sir.total_cogs, 0)) AS inventory_cogs
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
       4️⃣ DAYS COVER TREND (LAST 7 DAYS, UNFILTERED - ALWAYS GLOBAL)
    ========================================================= */
    const [daysCoverTrend] = await historyDb.query(`
      SELECT
        ips.snapshot_date,
        ROUND(AVG(ips.days_of_cover), 2) AS avg_days_cover
      FROM history_operations_db.inventory_planning_snapshot ips
      WHERE ips.snapshot_date >= CURDATE() - INTERVAL 6 DAY
      GROUP BY ips.snapshot_date
      ORDER BY ips.snapshot_date ASC
    `);

    /* =========================================================
       5️⃣ QUICK COMMERCE SPEED (UNFILTERED - ALWAYS GLOBAL)
    ========================================================= */
    const [[quickCommerce]] = await historyDb.query(`
      SELECT
        ROUND(SUM(COALESCE(sir.swiggy_drr_30d, 0)), 2) AS swiggy,
        ROUND(SUM(COALESCE(sir.Zepto_B2B_drr_30d, 0)), 2) AS zepto,
        ROUND(SUM(COALESCE(sir.blinkit_b2b_drr_30d, 0)), 2) AS blinkit_b2b,
        ROUND(SUM(COALESCE(sir.blinkit_marketplace_speed_30_days, 0)), 2) AS blinkit_b2c
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
    `);

    /* =========================================================
       6️⃣ INVENTORY DISTRIBUTION (UNFILTERED - ALWAYS GLOBAL)
    ========================================================= */
    const [[distribution]] = await historyDb.query(`
      SELECT
        SUM(COALESCE(increff_units, 0)) AS increff,
        SUM(COALESCE(kvt_units, 0)) AS kv_traders,
        SUM(COALESCE(pc_units, 0)) AS processing,
        SUM(COALESCE(fba_units_gb, 0) + COALESCE(fba_bundled_units, 0)) AS fba,
        SUM(COALESCE(fbf_units_gb, 0) + COALESCE(fbf_bundled_units, 0)) AS fbf,
        SUM(COALESCE(myntra_units_gb, 0) + COALESCE(myntra_bundled_units, 0)) AS myntra,
        SUM(COALESCE(rk_world_stock, 0)) AS rk_world
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
    `);

    /* =========================================================
       7️⃣ SKU DETAILS (FILTER AWARE + PAGINATION)
    ========================================================= */
    
    // Get total count for pagination - use the same values array
    const [[countResult]] = await historyDb.query(`
      SELECT COUNT(*) as total
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
    
    const totalRecords = countResult?.total || 0;
    
    const [sku_inventory_details] = await historyDb.query(`
      SELECT 
        ips.*,
        sir.brand,
        sir.product_title,
        COALESCE(NULLIF(TRIM(sir.vendor_name), ''), 'No Vendor') AS vendor_name
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
       8️⃣ FILTER OPTIONS (DYNAMIC - BASED ON CURRENT FILTERS)
    ========================================================= */
    
    // Brands - filtered by other selected filters (excluding brand itself)
    const brandFilterConditions = [];
    const brandFilterValues = [];
    if (vendor) {
      brandFilterConditions.push('sir1.vendor_name = ?');
      brandFilterValues.push(vendor);
    }
    if (category) {
      brandFilterConditions.push('sir1.category = ?');
      brandFilterValues.push(category);
    }
    if (location) {
      brandFilterConditions.push('sir1.swiggy_city = ?');
      brandFilterValues.push(location);
    }
    const brandWhere = brandFilterConditions.length 
      ? `AND ${brandFilterConditions.join(' AND ')}`
      : '';
    
    const [brands] = await historyDb.query(`
      SELECT DISTINCT sir1.brand
      FROM history_operations_db.sku_inventory_report sir1
      INNER JOIN (
        SELECT ean_code, MAX(created_at) AS max_created
        FROM history_operations_db.sku_inventory_report
        GROUP BY ean_code
      ) sir2
        ON sir1.ean_code = sir2.ean_code
       AND sir1.created_at = sir2.max_created
      WHERE sir1.brand IS NOT NULL 
        AND sir1.brand != ''
        ${brandWhere}
      ORDER BY sir1.brand
    `, brandFilterValues);

    // Vendors - filtered by other selected filters (excluding vendor itself)
    const vendorFilterConditions = [];
    const vendorFilterValues = [];
    if (brand) {
      vendorFilterConditions.push('sir1.brand = ?');
      vendorFilterValues.push(brand);
    }
    if (category) {
      vendorFilterConditions.push('sir1.category = ?');
      vendorFilterValues.push(category);
    }
    if (location) {
      vendorFilterConditions.push('sir1.swiggy_city = ?');
      vendorFilterValues.push(location);
    }
    const vendorWhere = vendorFilterConditions.length 
      ? `AND ${vendorFilterConditions.join(' AND ')}`
      : '';
    
    const [vendors] = await historyDb.query(`
      SELECT DISTINCT COALESCE(NULLIF(TRIM(sir1.vendor_name), ''), 'No Vendor') AS vendor
      FROM history_operations_db.sku_inventory_report sir1
      INNER JOIN (
        SELECT ean_code, MAX(created_at) AS max_created
        FROM history_operations_db.sku_inventory_report
        GROUP BY ean_code
      ) sir2
        ON sir1.ean_code = sir2.ean_code
       AND sir1.created_at = sir2.max_created
      WHERE 1=1
        ${vendorWhere}
      ORDER BY vendor
    `, vendorFilterValues);

    // Categories - filtered by other selected filters (excluding category itself)
    const categoryFilterConditions = [];
    const categoryFilterValues = [];
    if (brand) {
      categoryFilterConditions.push('sir1.brand = ?');
      categoryFilterValues.push(brand);
    }
    if (vendor) {
      categoryFilterConditions.push('sir1.vendor_name = ?');
      categoryFilterValues.push(vendor);
    }
    if (location) {
      categoryFilterConditions.push('sir1.swiggy_city = ?');
      categoryFilterValues.push(location);
    }
    const categoryWhere = categoryFilterConditions.length 
      ? `AND ${categoryFilterConditions.join(' AND ')}`
      : '';
    
    const [categories] = await historyDb.query(`
      SELECT DISTINCT sir1.category
      FROM history_operations_db.sku_inventory_report sir1
      INNER JOIN (
        SELECT ean_code, MAX(created_at) AS max_created
        FROM history_operations_db.sku_inventory_report
        GROUP BY ean_code
      ) sir2
        ON sir1.ean_code = sir2.ean_code
       AND sir1.created_at = sir2.max_created
      WHERE sir1.category IS NOT NULL 
        AND sir1.category != ''
        ${categoryWhere}
      ORDER BY sir1.category
    `, categoryFilterValues);

    // Locations - filtered by other selected filters (excluding location itself)
    const locationFilterConditions = [];
    const locationFilterValues = [];
    if (brand) {
      locationFilterConditions.push('sir1.brand = ?');
      locationFilterValues.push(brand);
    }
    if (vendor) {
      locationFilterConditions.push('sir1.vendor_name = ?');
      locationFilterValues.push(vendor);
    }
    if (category) {
      locationFilterConditions.push('sir1.category = ?');
      locationFilterValues.push(category);
    }
    const locationWhere = locationFilterConditions.length 
      ? `AND ${locationFilterConditions.join(' AND ')}`
      : '';
    
    const [locations] = await historyDb.query(`
      SELECT DISTINCT sir1.swiggy_city AS location
      FROM history_operations_db.sku_inventory_report sir1
      INNER JOIN (
        SELECT ean_code, MAX(created_at) AS max_created
        FROM history_operations_db.sku_inventory_report
        GROUP BY ean_code
      ) sir2
        ON sir1.ean_code = sir2.ean_code
       AND sir1.created_at = sir2.max_created
      WHERE sir1.swiggy_city IS NOT NULL 
        AND sir1.swiggy_city != ''
        ${locationWhere}
      ORDER BY sir1.swiggy_city
    `, locationFilterValues);

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
        total: totalRecords,
        totalPages: Math.ceil(totalRecords / Number(limit)),
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