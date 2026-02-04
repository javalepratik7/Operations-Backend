const historyDb = require('../db/historyDb');

async function writeUpcomingStocksSnapshot() {
  const [result] = await historyDb.query(`
    INSERT INTO history_operations_db.upcoming_stocks
    SELECT 
        t.*,
        CURDATE() AS snapshot_date,
        NOW() AS created_at
    FROM operations_db.replica_b2b_order_itemlevel t
    WHERE NOT EXISTS (
        SELECT 1 
        FROM history_operations_db.upcoming_stocks h 
        WHERE 
            h.ean = t.ean
            AND h._airbyte_ab_id = t._airbyte_ab_id
    )
  `);

  return result;
}

module.exports = {
  writeUpcomingStocksSnapshot,
};
