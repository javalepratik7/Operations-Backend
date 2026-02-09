const historyDb = require('../db/historyDb');

/**
 * Safely convert DB values (DECIMAL / NULL / string) to numbers
 */
const n = (v) => Number(v) || 0;

async function writeInventoryPlanningSnapshot(testEan = null) {
  console.log('📊 Inventory Planning snapshot started...');

  try {
    /**
     * 1️⃣ Get latest row per EAN from sku_inventory_report
     */
    const [eanRows] = await historyDb.query(
      `
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
      `,
      testEan ? [testEan] : []
    );

    let inserted = 0;

    for (const row of eanRows) {
      const eanCode = row.ean_code;

      /* 2️⃣ DRR (30 days) */
      const drr30 =
        n(row.quick_comm_total_speed) +
        n(row.warehouse_total_speed);

      /* 3️⃣ Current stock */
      const currentStock = n(row.total_stock);

      /* 4️⃣ Lead time & safety stock */
      const leadTimeDays = n(row.lead_time_vendor_lt);
      const safetyStockDays = 40;
      const poBufferDays = 15;

      /* 5️⃣ Upcoming stock (ONLY latest day) */
      const [[upcomingRow]] = await historyDb.query(
        `
        SELECT
          GREATEST(
            COALESCE(SUM(
              CASE
                WHEN external_order_code LIKE '%main%' THEN order_quantity
                ELSE 0
              END
            ),0)
            -
            COALESCE(SUM(
              CASE
                WHEN external_order_code LIKE '%sub%' THEN order_quantity
                ELSE 0
              END
            ),0),
            0
          ) AS upcoming_stock
        FROM history_operations_db.upcoming_stocks us
        WHERE us.ean = ?
          AND DATE(us.created_at) = (
            SELECT DATE(MAX(created_at))
            FROM history_operations_db.upcoming_stocks
            WHERE ean = ?
          )
        `,
        [eanCode, eanCode]
      );

      const upcomingStock = n(upcomingRow?.upcoming_stock);

      /* 6️⃣ In-transit stock */
      const inTransitStock =
        n(row.vendor_increff) +
        n(row.vendor_to_pc) +
        n(row.vendor_to_fba) +
        n(row.vendor_to_fbf) +
        n(row.vendor_to_kv) +
        n(row.pc_to_fba) +
        n(row.pc_to_fbf) +
        n(row.pc_to_increff) +
        n(row.kv_to_fba) +
        n(row.kv_to_fbf);

      /* 7️⃣ Reorder level */
      const reorderLevel =
        drr30 * (leadTimeDays + safetyStockDays);

      /* 8️⃣ PO Intent Units */
      const poIntentUnits =
        drr30 * (leadTimeDays + poBufferDays);

      /* 9️⃣ Days of cover */
      const daysOfCover =
        drr30 > 0
          ? (currentStock + inTransitStock) / drr30
          : 0;

      const daysOfCoverWithPO =
        drr30 > 0
          ? (currentStock + inTransitStock + upcomingStock) / drr30
          : 0;

      /* 🔟 Inventory status (FIXED LOGIC) */
      const availableNow = currentStock + inTransitStock;
      const availableWithPO = availableNow + upcomingStock;

      let inventoryStatus = 'OK';

      if (availableWithPO <= reorderLevel) {
        inventoryStatus = 'PO_REQUIRED';
      } else if (availableNow <= reorderLevel) {
        inventoryStatus = 'LOW_STOCK';
      } else {
        inventoryStatus = 'OVER_STOCK';
      }

      /* 1️⃣1️⃣ Insert / Update snapshot */
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
          po_intent_units,
          days_of_cover,
          days_of_cover_with_po,
          inventory_status,
          snapshot_date
        )
        VALUES (
          ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURDATE()
        )
        ON DUPLICATE KEY UPDATE
          drr_30d = VALUES(drr_30d),
          current_stock = VALUES(current_stock),
          in_transit_stock = VALUES(in_transit_stock),
          upcoming_stock = VALUES(upcoming_stock),
          reorder_level = VALUES(reorder_level),
          po_intent_units = VALUES(po_intent_units),
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
          Math.ceil(poIntentUnits),
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
