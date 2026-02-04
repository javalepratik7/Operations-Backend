const express = require('express');
const bodyParser = require('body-parser');
const cors = require('cors');
require('dotenv').config();

const userRoutes = require('./routes/userRoutes');
const channelDrrSyncRoute = require('./routes/channelDrrSync');
const inventoryRoute = require('./routes/Inventory.js');
const infoRoute = require('./routes/infoRoute.js');
const startHistorySyncJob = require('./jobs/historySync.job');

const app = express();

app.use(cors()); // <-- this line is enough for public access
app.use(bodyParser.json());

// Routes
app.use('/api/users', userRoutes);
app.use('/api', channelDrrSyncRoute);
app.use('/api', infoRoute);
app.use('/api/inventory', inventoryRoute);

// Start cron jobs
startHistorySyncJob();

const PORT = process.env.PORT || 5000;
app.listen(PORT, () => {
  console.log(`🚀 Server running on port ${PORT}`);
});