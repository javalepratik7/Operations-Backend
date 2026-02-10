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
      location,
      kpiFilter,
      sortBy = 'inventory_status',
      sortOrder = 'asc'
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

    /* ✅ WAREHOUSE LOCATION FILTER */
    if (location && location.toLowerCase() !== 'all') {
      switch (location.toLowerCase()) {
        case 'increff':
          filters.push('COALESCE(sir.increff_units,0) > 0');
          break;

        case 'kv_traders':
          filters.push('COALESCE(sir.kvt_units,0) > 0');
          break;

        case 'processing_center':
          filters.push('COALESCE(sir.pc_units,0) > 0');
          break;

        case 'amazon_fba':
          filters.push('(COALESCE(sir.fba_units_gb,0) + COALESCE(sir.fba_bundled_units,0)) > 0');
          break;

        case 'flipkart_fbf':
          filters.push('(COALESCE(sir.fbf_units_gb,0) + COALESCE(sir.fbf_bundled_units,0)) > 0');
          break;

        case 'myntra':
          filters.push('(COALESCE(sir.myntra_units_gb,0) + COALESCE(sir.myntra_bundled_units,0)) > 0');
          break;

        case 'rk_world':
          filters.push('COALESCE(sir.rk_world_stock,0) > 0');
          break;
      }
    }

    /* KPI Filters */
    if (kpiFilter) {
      switch (kpiFilter) {
        case 'stock_alert':
          filters.push('(ips.inventory_status = ? OR ips.current_stock = 0)');
          values.push('LOW_STOCK');
          break;

        case 'upcoming_stock':
          filters.push('ips.upcoming_stock > 0');
          break;

        case 'po_required':
          filters.push('ips.inventory_status = ?');
          values.push('PO_REQUIRED');
          break;

        case 'over_inventory':
          filters.push('ips.days_of_cover > 90');
          break;
      }
    }

    const FILTER_SQL = filters.length
      ? `AND ${filters.join(' AND ')}`
      : '';

    /* =========================================================
       3️⃣ SUMMARY
    ========================================================= */
    const [[summary]] = await historyDb.query(`
      SELECT
        SUM(ips.current_stock) AS current_stock,
        SUM(ips.in_transit_stock) AS in_transit,
        SUM(ips.upcoming_stock) AS upcoming_stock,
        SUM(ips.current_stock + ips.in_transit_stock + ips.upcoming_stock) AS total_stock,

        SUM(CASE WHEN ips.inventory_status='OVER_STOCK'
        THEN ips.current_stock ELSE 0 END) AS over_inventory,

        SUM(ips.inventory_status='LOW_STOCK') AS stock_alert,
        SUM(ips.inventory_status='PO_REQUIRED') AS po_required,

        SUM(CASE WHEN ips.inventory_status='PO_REQUIRED'
        THEN ips.po_intent_units ELSE 0 END) AS total_po_intent_units,

        ROUND(AVG(ips.days_of_cover),2) AS avg_days_cover,
        
        -- ✅ CORRECTED INVENTORY COGS CALCULATION
        SUM(
          (COALESCE(ips.current_stock, 0) + 
           COALESCE(ips.upcoming_stock, 0) + 
           COALESCE(ips.in_transit_stock, 0)) * 
          COALESCE(sir.cogs, 0)
        ) AS inventory_cogs

      FROM history_operations_db.inventory_planning_snapshot ips
      JOIN (
        SELECT sir1.*
        FROM history_operations_db.sku_inventory_report sir1
        INNER JOIN (
          SELECT ean_code, MAX(created_at) max_created
          FROM history_operations_db.sku_inventory_report
          GROUP BY ean_code
        ) sir2
        ON sir1.ean_code=sir2.ean_code
        AND sir1.created_at=sir2.max_created
      ) sir ON sir.ean_code=ips.ean_code
      WHERE ips.snapshot_date=?
      ${FILTER_SQL}
    `, values);

    /* =========================================================
       4️⃣ DAYS COVER TREND
    ========================================================= */
    const [daysCoverTrend] = await historyDb.query(`
      SELECT snapshot_date,
      ROUND(AVG(days_of_cover),2) avg_days_cover
      FROM history_operations_db.inventory_planning_snapshot
      WHERE snapshot_date >= CURDATE() - INTERVAL 6 DAY
      GROUP BY snapshot_date
      ORDER BY snapshot_date
    `);

    /* =========================================================
       5️⃣ QUICK COMMERCE
    ========================================================= */
    const [[quickCommerce]] = await historyDb.query(`
      SELECT
        ROUND(SUM(COALESCE(swiggy_drr_30d,0)),2) swiggy,
        ROUND(SUM(COALESCE(Zepto_B2B_drr_30d,0)),2) zepto,
        ROUND(SUM(COALESCE(blinkit_b2b_drr_30d,0)),2) blinkit_b2b,
        ROUND(SUM(COALESCE(blinkit_marketplace_speed_30_days,0)),2) blinkit_b2c
      FROM history_operations_db.sku_inventory_report sir1
      JOIN (
        SELECT ean_code,MAX(created_at) mc
        FROM history_operations_db.sku_inventory_report
        GROUP BY ean_code
      ) sir2
      ON sir1.ean_code=sir2.ean_code
      AND sir1.created_at=sir2.mc
    `);

    /* =========================================================
       6️⃣ INVENTORY DISTRIBUTION
    ========================================================= */
    const [[distribution]] = await historyDb.query(`
      SELECT
        SUM(COALESCE(increff_units,0)) increff,
        SUM(COALESCE(kvt_units,0)) kv_traders,
        SUM(COALESCE(pc_units,0)) processing,
        SUM(COALESCE(fba_units_gb,0)+COALESCE(fba_bundled_units,0)) fba,
        SUM(COALESCE(fbf_units_gb,0)+COALESCE(fbf_bundled_units,0)) fbf,
        SUM(COALESCE(myntra_units_gb,0)+COALESCE(myntra_bundled_units,0)) myntra,
        SUM(COALESCE(rk_world_stock,0)) rk_world
      FROM history_operations_db.sku_inventory_report sir1
      JOIN (
        SELECT ean_code,MAX(created_at) mc
        FROM history_operations_db.sku_inventory_report
        GROUP BY ean_code
      ) sir2
      ON sir1.ean_code=sir2.ean_code
      AND sir1.created_at=sir2.mc
    `);

    /* =========================================================
       7️⃣ SORTING + SKU DETAILS
    ========================================================= */
    const allowedSortColumns = [
      'brand','ean_code','product_title','current_stock',
      'drr_30d','days_of_cover','days_of_cover_with_po',
      'in_transit_stock','vendor_name','inventory_status',
      'po_intent_units','upcoming_stock'
    ];

    const validSortBy = allowedSortColumns.includes(sortBy)
      ? sortBy : 'inventory_status';

    const validSortOrder =
      sortOrder.toLowerCase()==='desc'?'DESC':'ASC';

    let orderByClause;

    // ✅ FIX: When kpiFilter is applied, use regular sorting (not special inventory_status logic)
    if (validSortBy === 'inventory_status' && !kpiFilter) {
      // Only use special FIELD sorting when NO kpiFilter is active
      orderByClause = `
        FIELD(ips.inventory_status,'LOW_STOCK','PO_REQUIRED','OVER_STOCK') ${validSortOrder},
        ips.days_of_cover ASC
      `;
    } else if (validSortBy === 'inventory_status' && kpiFilter) {
      // When kpiFilter is active, sort inventory_status alphabetically
      orderByClause = `ips.inventory_status ${validSortOrder}`;
    } else if (['brand','product_title','vendor_name'].includes(validSortBy)) {
      orderByClause = `sir.${validSortBy} ${validSortOrder}`;
    } else {
      orderByClause = `ips.${validSortBy} ${validSortOrder}`;
    }

    const [[countResult]] = await historyDb.query(`
      SELECT COUNT(*) total
      FROM history_operations_db.inventory_planning_snapshot ips
      JOIN (
        SELECT sir1.*
        FROM history_operations_db.sku_inventory_report sir1
        JOIN (
          SELECT ean_code,MAX(created_at) mc
          FROM history_operations_db.sku_inventory_report
          GROUP BY ean_code
        ) sir2
        ON sir1.ean_code=sir2.ean_code
        AND sir1.created_at=sir2.mc
      ) sir ON sir.ean_code=ips.ean_code
      WHERE ips.snapshot_date=?
      ${FILTER_SQL}
    `, values);

    const totalRecords = countResult.total || 0;

    const [sku_inventory_details] = await historyDb.query(`
      SELECT ips.*,sir.brand,sir.product_title,
      COALESCE(NULLIF(TRIM(sir.vendor_name),''),'No Vendor') vendor_name
      FROM history_operations_db.inventory_planning_snapshot ips
      JOIN (
        SELECT sir1.*
        FROM history_operations_db.sku_inventory_report sir1
        JOIN (
          SELECT ean_code,MAX(created_at) mc
          FROM history_operations_db.sku_inventory_report
          GROUP BY ean_code
        ) sir2
        ON sir1.ean_code=sir2.ean_code
        AND sir1.created_at=sir2.mc
      ) sir ON sir.ean_code=ips.ean_code
      WHERE ips.snapshot_date=?
      ${FILTER_SQL}
      ORDER BY ${orderByClause}
      LIMIT ? OFFSET ?
    `,[...values,Number(limit),Number(offset)]);

    /* =========================================================
       8️⃣ STATIC WAREHOUSE LOCATIONS
    ========================================================= */
    const locations=[
      'increff','kv_traders','processing_center',
      'amazon_fba','flipkart_fbf','myntra','rk_world'
    ];

    /* =========================================================
      FETCH FILTER DROPDOWN VALUES
    ========================================================= */

    const [brands] = await historyDb.query(`
      SELECT DISTINCT brand 
      FROM history_operations_db.sku_inventory_report
      WHERE brand IS NOT NULL AND brand!=''
      ORDER BY brand
    `);

    const [vendors] = await historyDb.query(`
      SELECT DISTINCT vendor_name 
      FROM history_operations_db.sku_inventory_report
      WHERE vendor_name IS NOT NULL AND vendor_name!=''
      ORDER BY vendor_name
    `);

    const [categories] = await historyDb.query(`
      SELECT DISTINCT category 
      FROM history_operations_db.sku_inventory_report
      WHERE category IS NOT NULL AND category!=''
      ORDER BY category
    `);


    /* =========================================================
       RESPONSE
    ========================================================= */
    res.json({
      snapshot_date:SNAPSHOT_DATE,
      summary:{
        ...summary,
        quick_commerce_speed:quickCommerce,
        inventory_distribution:distribution,
        days_cover_trend:daysCoverTrend
      },
      
      filters: {
        brands: brands.map(b => b.brand),
        vendors: vendors.map(v => v.vendor_name),
        categories: categories.map(c => c.category),
        locations
      },
      pagination:{
        page:Number(page),
        limit:Number(limit),
        total:totalRecords,
        totalPages:Math.ceil(totalRecords/limit),
        returned:sku_inventory_details.length
      },
      sorting:{sortBy:validSortBy,sortOrder:validSortOrder},
      sku_inventory_details
    });

  } catch(error){
    console.error('❌ /planning API error:',error);
    res.status(500).json({
      message:'Planning API failed',
      error:error.message
    });
  }
});

module.exports = router;
