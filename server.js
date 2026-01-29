const express = require('express');
const bodyParser = require('body-parser');
require('dotenv').config();

const userRoutes = require('./routes/userRoutes');
const startHistorySyncJob = require('./jobs/historySync.job');

const app = express();
app.use(bodyParser.json());

// Routes
app.use('/api/users', userRoutes);

// Start cron jobs
startHistorySyncJob();

const PORT = process.env.PORT || 5000;
app.listen(PORT, () => {
  console.log(`🚀 Server running on port ${PORT}`);
});