const historyDb = require('../db/historyDb');

async function writeInventoryPlanningSnapshot(testEan = null) {
  console.log('📊 Inventory Planning snapshot started...');

  try {
    /**
     * 1️⃣ Get latest row per EAN from sku_inventory_report
     *    (IMPORTANT: same EAN has multiple rows historically)
     */
    const [eanRows] = await historyDb.query(`
      SELECT s.*
      FROM history_operations_db.sku_inventory_report s
      INNER JOIN (
        SELECT ean_code, MAX(created_at) AS max_created
        FROM history_operations_db.sku_inventory_report
        GROUP BY ean_code
      ) t
        ON s.ean_code = t.ean_code
       AND s.created_at = t.max_created
      ${testEan ? 'WHERE s.ean_code = ?' : ''}
    `, testEan ? [testEan] : []);

    let inserted = 0;

    for (const row of eanRows) {
      const eanCode = row.ean_code;

      /**
       * 2️⃣ DRR (30 days)
       * DRR = quick commerce 30d + warehouse 30d
       */
      const drr30 =
        (Number(row.quickcomm_speed_30_days) || 0) +
        (Number(row.warehouse_speed_30_days) || 0);

      /**
       * 3️⃣ Current stock
       */
      const currentStock = Number(row.total_stock) || 0;

      /**
       * 4️⃣ Lead time & safety stock
       */
      const leadTimeDays = Number(row.lead_time_vendor_lt) || 0;
      const safetyStockDays = 40;

      /**
       * 5️⃣ Upcoming stock (MAIN - SUB)
       */
      const [[upcomingRow]] = await historyDb.query(
        `
        SELECT
          GREATEST(
            COALESCE(SUM(
              CASE
                WHEN external_order_code LIKE '%main%' THEN order_quantity
                ELSE 0
              END
            ), 0)
            -
            COALESCE(SUM(
              CASE
                WHEN external_order_code LIKE '%sub%' THEN order_quantity
                ELSE 0
              END
            ), 0),
            0
          ) AS upcoming_stock
        FROM history_operations_db.upcoming_stocks
        WHERE ean = ?
        `,
        [eanCode]
      );

      const upcomingStock = Number(upcomingRow?.upcoming_stock) || 0;

      /**
       * 6️⃣ In-transit stock
       */
      const [[transitRow]] = await historyDb.query(
        `
        SELECT COALESCE(SUM(\`In Transit Quantity\`), 0) AS in_transit_stock
        FROM history_operations_db.upcoming_stocks
        WHERE ean = ?
        `,
        [eanCode]
      );

      const inTransitStock = Number(transitRow?.in_transit_stock) || 0;

      /**
       * 7️⃣ Reorder level
       */
      const reorderLevel = drr30 * (leadTimeDays + safetyStockDays);

      /**
       * 8️⃣ Days of cover
       * (NO DIVISION BY ZERO)
       */
      const daysOfCover =
        drr30 > 0 ? (currentStock + inTransitStock) / drr30 : 0;

      const daysOfCoverWithPO =
        drr30 > 0
          ? (currentStock + inTransitStock + upcomingStock) / drr30
          : 0;

      /**
       * 9️⃣ Inventory status
       */
      let inventoryStatus = 'OK';

      if (currentStock + inTransitStock + upcomingStock <= reorderLevel) {
        inventoryStatus = 'PO_REQUIRED';
      } else if (currentStock + inTransitStock <= reorderLevel) {
        inventoryStatus = 'LOW_STOCK';
      } else if (currentStock + inTransitStock >= reorderLevel) {
        inventoryStatus = 'OVER_STOCK';
      }

      /**
       * 🔟 Insert snapshot (1 row per EAN per day)
       */
      await historyDb.query(
        `
        INSERT INTO history_operations_db.inventory_planning_snapshot (
          ean_code,
          drr_30d,
          lead_time_days,
          safety_stock_days,
          current_stock,
          in_transit_stock,
          upcoming_stock,
          reorder_level,
          days_of_cover,
          days_of_cover_with_po,
          inventory_status,
          snapshot_date
        )
        VALUES (
          ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURDATE()
        )
        ON DUPLICATE KEY UPDATE
          drr_30d = VALUES(drr_30d),
          current_stock = VALUES(current_stock),
          in_transit_stock = VALUES(in_transit_stock),
          upcoming_stock = VALUES(upcoming_stock),
          reorder_level = VALUES(reorder_level),
          days_of_cover = VALUES(days_of_cover),
          days_of_cover_with_po = VALUES(days_of_cover_with_po),
          inventory_status = VALUES(inventory_status),
          created_at = NOW()
        `,
        [
          eanCode,
          drr30,
          leadTimeDays,
          safetyStockDays,
          currentStock,
          inTransitStock,
          upcomingStock,
          reorderLevel,
          Number(daysOfCover.toFixed(2)),
          Number(daysOfCoverWithPO.toFixed(2)),
          inventoryStatus
        ]
      );

      inserted++;
    }

    console.log(`✅ Inventory Planning snapshot completed. Rows processed: ${inserted}`);
    return { inserted };

  } catch (error) {
    console.error('❌ Inventory Planning snapshot failed:', error.message);
    throw error;
  }
}

module.exports = {
  writeInventoryPlanningSnapshot
};
