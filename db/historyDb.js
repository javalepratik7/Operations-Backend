// const mysql = require('mysql2/promise');

// const historyDb = mysql.createPool({
//   host: 'ug-application-db-server.catvfpilxwys.ap-south-1.rds.amazonaws.com',
//   user: 'admin',
//   password: 'urban#gabru$123',
//   database: 'history_operations_db',
//   port: 3306,
//   waitForConnections: true,
//   connectionLimit: 10,
//   queueLimit: 0,
//   connectTimeout: 20000,
// });

// module.exports = historyDb;


const mysql = require('mysql2/promise');

const historyDb = mysql.createPool({
  host: process.env.HIST_OPERATIONS_DB_HOST,
  user: process.env.HIST_OPERATIONS_DB_USER,
  password: process.env.HIST_OPERATIONS_DB_PASS, // ❌ never log this
  database: process.env.HIST_OPERATIONS_DB_DB_NAME,
  port: Number(process.env.HIST_OPERATIONS_DB_PORT),

  waitForConnections: true,
  connectionLimit: Number(process.env.HIST_OPERATIONS_DB_CONN_LIMIT),
  queueLimit: Number(process.env.HIST_OPERATIONS_DB_QUE_LIMIT),
  connectTimeout: Number(process.env.HIST_OPERATIONS_DB_CONN_TIME),
});

(async () => {
  try {
    const conn = await historyDb.getConnection();
    console.log('✅ History Operations DB connected successfully');
    conn.release();
  } catch (err) {
    console.error('❌ History Operations DB connection failed:', {
      message: err.message,
      code: err.code,
      errno: err.errno,
      sqlState: err.sqlState,
    });
  }
})();

module.exports = historyDb;
