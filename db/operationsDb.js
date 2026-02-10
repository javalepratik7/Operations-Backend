// const mysql = require('mysql2/promise');

// const operationsDb = mysql.createPool({
//   host: 'ug-application-db-server.catvfpilxwys.ap-south-1.rds.amazonaws.com',
//   user: 'admin',
//   password: 'urban#gabru$123',
//   database: 'operations_db',
//   port: 3306,
//   waitForConnections: true,
//   connectionLimit: 10,
//   queueLimit: 0,
//   connectTimeout: 20000,
// });

// module.exports = operationsDb;


const mysql = require('mysql2/promise');

const operationsDb = mysql.createPool({
  host: process.env.OPERATIONS_DB_HOST,
  user: process.env.OPERATIONS_DB_USER,
  password: process.env.OPERATIONS_DB_PASS, // ❌ never log
  database: process.env.OPERATIONS_DB_DB_NAME,
  port: Number(process.env.OPERATIONS_DB_PORT),

  waitForConnections: process.env.OPERATIONS_DB_WAIT === 'true',
  connectionLimit: Number(process.env.OPERATIONS_DB_CONN_LIMIT),
  queueLimit: Number(process.env.OPERATIONS_DB_QUE_LIMIT),
  connectTimeout: Number(process.env.OPERATIONS_DB_CONN_TIME),
});

(async () => {
  try {
    const conn = await operationsDb.getConnection();
    console.log('✅ Operations DB connected successfully');
    conn.release();
  } catch (err) {
    console.error('❌ Operations DB connection failed:', {
      message: err.message,
      code: err.code,
      errno: err.errno,
      sqlState: err.sqlState,
    });
  }
})();

module.exports = operationsDb;

