const express = require('express');
const router = express.Router();
const requestIp = require('request-ip');
const useragent = require('useragent');
const AnalyticsEvent = require('../models/AnalyticsEvent');
const Conversation = require('../models/Conversation');
const jwt = require('jsonwebtoken');

// Middleware optional auth: đọc user token nếu có (để gắn user_id vào analytics event)
const optionalAuth = (req, res, next) => {
  const authHeader = req.header('Authorization');
  if (authHeader) {
    const token = authHeader.split(' ')[1];
    if (token) {
      try {
        const decoded = jwt.verify(token, process.env.JWT_SECRET || 'super_secret_jwt_key_yhct_2026');
        req.user = decoded;
      } catch (err) {
        // Bỏ qua lỗi verify token vì đây là optional auth cho tracking
      }
    }
  }
  next();
};

// Log Analytics Event
router.post('/event', optionalAuth, async (req, res) => {
  try {
    const payload = req.body;
    const {
      session_id,
      event_type,
      referrer_url,
      route,
      button_name,
      search_keywords,
      utm_source,
      utm_medium,
      device_type,
      browser,
      os,
      bounce,
      first_visit_at,
      session_duration_sec
    } = payload;

    if (!session_id || !event_type) {
      return res.status(400).json({ message: 'Thiếu session_id hoặc event_type.' });
    }

    // Capture IP Address & User Agent server-side
    const clientIp = requestIp.getClientIp(req) || req.ip;
    const agent = useragent.parse(req.headers['user-agent']);
    
    const finalBrowser = browser || agent.toAgent();
    const finalOs = os || agent.os.toString();

    // Mock GeoIP (thực tế sẽ dùng thư viện geoip-lite hoặc maxmind)
    let country = 'Vietnam';
    let city = 'Hanoi';
    if (clientIp && clientIp !== '127.0.0.1' && clientIp !== '::1' && clientIp !== '::ffff:127.0.0.1') {
      // Phân tích IP thật ở đây nếu cần, tạm thời gán mặc định cho localhost/local network
      city = 'Ho Chi Minh City';
    }

    const userId = req.user ? req.user.id : null;

    const event = new AnalyticsEvent({
      session_id,
      user_id: userId,
      event_type,
      timestamp: new Date(),
      device_type: device_type || 'desktop',
      browser: finalBrowser,
      os: finalOs,
      ip_address: clientIp,
      country,
      city,
      referrer_url,
      route,
      button_name,
      search_keywords,
      bounce: !!bounce,
      first_visit_at: first_visit_at ? new Date(first_visit_at) : new Date(),
      utm_source,
      utm_medium
    });

    await event.save();

    // Nếu có session_duration_sec gửi kèm (từ heartbeat hoặc page views), cập nhật vào Conversation tương ứng
    if (session_duration_sec && session_id) {
      await Conversation.updateOne(
        { session_id },
        { $set: { session_duration_sec: Math.round(session_duration_sec) } }
      );
    }

    res.status(201).json({ status: 'success', event_id: event._id });
  } catch (err) {
    console.error(err);
    res.status(500).json({ message: 'Lỗi server khi lưu analytics event.' });
  }
});

module.exports = router;
