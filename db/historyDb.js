const mysql = require('mysql2/promise');

const historyDb = mysql.createPool({
  host: 'ug-application-db-server.catvfpilxwys.ap-south-1.rds.amazonaws.com',
  user: 'admin',
  password: 'urban#gabru$123',
  database: 'history_operations_db',
  port: 3306,
  waitForConnections: true,
  connectionLimit: 10,
  queueLimit: 0,
  connectTimeout: 20000,
});

module.exports = historyDb;
