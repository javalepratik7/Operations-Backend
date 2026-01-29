const operationsDb = require('../db/operationsDb');
const historyDb = require('../db/historyDb');

async function consolidateHistoryData() {
  // 1️⃣ Fetch Excel (history) data
  const [excelRows] = await historyDb.query(`
    SELECT *
    FROM excel_orders eo
    WHERE eo.processed = 0
  `);

  if (excelRows.length === 0) {
    console.log('ℹ️ No new history data to process');
    return;
  }

  // 2️⃣ Fetch reference data from operations_db
  const [users] = await operationsDb.query(`
    SELECT id, name, email
    FROM users
  `);

  const userMap = new Map();
  users.forEach(u => userMap.set(u.id, u));

  // 3️⃣ Prepare consolidated rows
  const consolidatedRows = excelRows.map(row => {
    const user = userMap.get(row.user_id);

    return [
      row.order_id,
      row.user_id,
      user?.name || null,
      row.amount,
      row.created_at,
      new Date()
    ];
  });

  // 4️⃣ Insert into main consolidated table
  await operationsDb.query(
    `
    INSERT INTO consolidated_operations
      (order_id, user_id, user_name, amount, order_date, synced_at)
    VALUES ?
    ON DUPLICATE KEY UPDATE
      amount = VALUES(amount),
      synced_at = VALUES(synced_at)
    `,
    [consolidatedRows]
  );

  // 5️⃣ Mark history rows as processed
  const ids = excelRows.map(r => r.id);

  await historyDb.query(
    `UPDATE excel_orders SET processed = 1 WHERE id IN (?)`,
    [ids]
  );

  console.log(`✅ Consolidated ${excelRows.length} rows`);
}

module.exports = {
  consolidateHistoryData,
};
