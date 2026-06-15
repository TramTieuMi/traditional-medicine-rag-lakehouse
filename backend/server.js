require('dotenv').config();
const express = require('express');
const mongoose = require('mongoose');
const redis = require('redis');
const cors = require('cors');

const app = express();
const PORT = process.env.PORT || 5000;

// Middleware
app.use(cors({ origin: '*' })); // Cho phép tất cả các nguồn truy cập trong phát triển
app.use(express.json());

// Routes
const authRoutes = require('./routes/auth');
const chatRoutes = require('./routes/chat');
const analyticsRoutes = require('./routes/analytics');

app.use('/api/auth', authRoutes);
app.use('/api/chat', chatRoutes);
app.use('/api/analytics', analyticsRoutes);

// Root Endpoint
app.get('/health', (req, res) => {
  res.json({
    status: 'healthy',
    time: new Date(),
    mongodb: mongoose.connection.readyState === 1 ? 'connected' : 'disconnected'
  });
});

// Database Connections & Server Startup
const startServer = async () => {
  try {
    // 1. Kết nối MongoDB
    const mongoUri = process.env.MONGO_URI || 'mongodb://mongodb:27017/yhct_db';
    console.log('Connecting to MongoDB...');
    await mongoose.connect(mongoUri);
    console.log('MongoDB Connected Successfully!');

    // 2. Kết nối Redis (chủ động bắt lỗi, không cho sập server nếu lỗi Redis)
    const redisUrl = process.env.REDIS_URL || 'redis://redis:6379';
    try {
      console.log('Connecting to Redis...');
      const redisClient = redis.createClient({ url: redisUrl });
      redisClient.on('error', (err) => console.error('Redis connection warning:', err));
      await redisClient.connect();
      console.log('Redis Connected Successfully!');
      app.locals.redis = redisClient; // Gắn client vào app.locals để dùng sau
    } catch (redisErr) {
      console.warn('Warning: Redis connection failed. Rate limiting will fall back to in-memory.', redisErr.message);
    }

    // 3. Khởi chạy HTTP Server
    app.listen(PORT, () => {
      console.log(`Express server is running on port ${PORT}`);
    });

  } catch (err) {
    console.error('Database connection failed:', err);
    process.exit(1);
  }
};

startServer();
