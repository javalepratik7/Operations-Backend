const mysql = require('mysql2/promise');

const operationsDb = mysql.createPool({
  host: process.env.OP_DB_HOST,
  user: process.env.OP_DB_USER,
  password: process.env.OP_DB_PASS,
  database: 'operations_db',
  waitForConnections: true,
  connectionLimit: 10,
});

module.exports = operationsDb;
