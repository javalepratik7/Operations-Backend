const { Sequelize, DataTypes } = require('sequelize');
require('dotenv').config();

const sequelize = new Sequelize(
  process.env.DB_NAME,
  process.env.DB_USER,
  process.env.DB_PASS,
  {
    host: process.env.DB_HOST,
    dialect: process.env.DB_DIALECT || 'mysql',
    logging: false,
  }
);

const db = {};

db.Sequelize = Sequelize;
db.sequelize = sequelize;

/* ===== MODELS ===== */
db.User = require('./user')(sequelize, DataTypes);

/* ===== ASSOCIATIONS ===== */
// ❌ Removed invalid associations
// Add associations ONLY when models exist

(async () => {
  try {
    await sequelize.authenticate();
    console.log('✅ Database connected');

    await sequelize.sync();
    console.log('✅ Database synced');
  } catch (err) {
    console.error('❌ DB connection failed:', err);
  }
})();

module.exports = db;
