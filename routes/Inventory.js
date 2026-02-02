const express = require('express');
const router = express.Router();
const db = require('../db/historyDb');

/**
 * GET /api/inventory/last-day
 * Returns SKU inventory reports with filters, pagination & date range
 */
router.get('/last-day', async (req, res) => {
  try {
    console.log('📊 Fetching SKU inventory reports...');

    const {
      page = 1,
      limit = 10,
      search = '',
      brand = '',
      sortBy = 'created_at',
      sortOrder = 'DESC',
      minStock = 0,
      maxStock = null,
      minDrr = 0,
      maxDrr = null,
      category = '',
      startDate = '',
      endDate = ''
    } = req.query;

    const pageNum = parseInt(page);
    const limitNum = parseInt(limit);
    const offset = (pageNum - 1) * limitNum;

    const conditions = [];
    const params = [];

    /* ---------------- DATE FILTER ---------------- */
    if (startDate && endDate) {
      conditions.push('created_at BETWEEN ? AND ?');
      params.push(
        `${startDate} 00:00:00`,
        `${endDate} 23:59:59`
      );
    } else if (startDate) {
      conditions.push('created_at >= ?');
      params.push(`${startDate} 00:00:00`);
    } else if (endDate) {
      conditions.push('created_at <= ?');
      params.push(`${endDate} 23:59:59`);
    } else {
      // Default: last 24 hours
      conditions.push('created_at >= DATE_SUB(NOW(), INTERVAL 1 DAY)');
    }

    /* ---------------- SEARCH ---------------- */
    if (search) {
      conditions.push(`
        (
          brand LIKE ? OR
          product_title LIKE ? OR
          gb_sku LIKE ? OR
          asin LIKE ? OR
          ean_code LIKE ?
        )
      `);
      const searchTerm = `%${search}%`;
      params.push(
        searchTerm,
        searchTerm,
        searchTerm,
        searchTerm,
        searchTerm
      );
    }

    /* ---------------- FILTERS ---------------- */
    if (brand && brand !== 'all') {
      conditions.push('brand = ?');
      params.push(brand);
    }

    if (category) {
      conditions.push('category LIKE ?');
      params.push(`%${category}%`);
    }

    if (minStock > 0) {
      conditions.push('(fba_units_gb >= ? OR fbf_units_gb >= ?)');
      params.push(minStock, minStock);
    }

    if (maxStock !== null && maxStock !== '') {
      conditions.push('(fba_units_gb <= ? OR fbf_units_gb <= ?)');
      params.push(maxStock, maxStock);
    }

    if (minDrr > 0) {
      conditions.push('(fba_drr >= ? OR fbf_drr >= ?)');
      params.push(minDrr, minDrr);
    }

    if (maxDrr !== null && maxDrr !== '') {
      conditions.push('(fba_drr <= ? OR fbf_drr <= ?)');
      params.push(maxDrr, maxDrr);
    }

    const whereClause =
      conditions.length > 0 ? `WHERE ${conditions.join(' AND ')}` : '';

    /* ---------------- COUNT ---------------- */
    const countQuery = `
      SELECT COUNT(*) AS total
      FROM sku_inventory_report
      ${whereClause}
    `;
    const [countResult] = await db.query(countQuery, params);
    const total = countResult[0]?.total || 0;
    const totalPages = Math.ceil(total / limitNum);

    /* ---------------- SORT ---------------- */
    const validSortColumns = [
      'created_at', 'brand', 'product_title', 'gb_sku', 'asin',
      'mrp', 'selling_price', 'cogs',
      'fba_units_gb', 'fbf_units_gb',
      'fba_drr', 'fbf_drr'
    ];
    const sortColumn = validSortColumns.includes(sortBy)
      ? sortBy
      : 'created_at';
    const order = sortOrder.toUpperCase() === 'ASC' ? 'ASC' : 'DESC';

    /* ---------------- MAIN DATA ---------------- */
    const mainQuery = `
      SELECT *
      FROM sku_inventory_report
      ${whereClause}
      ORDER BY ${sortColumn} ${order}
      LIMIT ? OFFSET ?
    `;
    const [results] = await db.query(
      mainQuery,
      [...params, limitNum, offset]
    );

    /* ---------------- BRANDS ---------------- */
    const brandsQuery = `
      SELECT DISTINCT brand
      FROM sku_inventory_report
      ${whereClause}
      AND brand IS NOT NULL AND brand != ''
      ORDER BY brand ASC
      LIMIT 50
    `;
    const [brandsResult] = await db.query(brandsQuery, params);
    const availableBrands = brandsResult.map(r => r.brand);

    /* ---------------- CATEGORIES ---------------- */
    const categoriesQuery = `
      SELECT DISTINCT category
      FROM sku_inventory_report
      ${whereClause}
      AND category IS NOT NULL AND category != ''
      ORDER BY category ASC
      LIMIT 30
    `;
    const [categoriesResult] = await db.query(categoriesQuery, params);
    const availableCategories = categoriesResult.map(r => r.category);

    /* ---------------- STATS ---------------- */
    const statsQuery = `
      SELECT
        COUNT(*) AS total_products,
        SUM(IF(fba_drr > 0 OR fbf_drr > 0, 1, 0)) AS active_products,
        SUM(fba_units_gb) AS total_fba_stock,
        SUM(fbf_units_gb) AS total_fbf_stock,
        AVG(fba_drr) AS avg_fba_drr,
        AVG(fbf_drr) AS avg_fbf_drr
      FROM sku_inventory_report
      ${whereClause}
    `;
    const [statsResult] = await db.query(statsQuery, params);
    const stats = statsResult[0];

    res.status(200).json({
      success: true,
      count: total,
      currentPage: pageNum,
      totalPages,
      itemsPerPage: limitNum,
      data: results,
      filters: {
        availableBrands,
        availableCategories,
        appliedFilters: {
          search,
          brand,
          category,
          minStock,
          maxStock,
          minDrr,
          maxDrr,
          startDate,
          endDate
        }
      },
      stats: {
        totalProducts: stats.total_products,
        activeProducts: stats.active_products,
        totalFbaStock: stats.total_fba_stock || 0,
        totalFbfStock: stats.total_fbf_stock || 0,
        avgFbaDrr: Number(stats.avg_fba_drr || 0).toFixed(2),
        avgFbfDrr: Number(stats.avg_fbf_drr || 0).toFixed(2)
      },
      pagination: {
        hasPrev: pageNum > 1,
        hasNext: pageNum < totalPages,
        prevPage: pageNum > 1 ? pageNum - 1 : null,
        nextPage: pageNum < totalPages ? pageNum + 1 : null
      }
    });

  } catch (error) {
    console.error('❌ Error:', error);
    res.status(500).json({
      success: false,
      message: 'Failed to fetch inventory reports',
      error: error.message
    });
  }
});

module.exports = router;
