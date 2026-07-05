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

    // GeoIP lookup based on real IP address
    let country = 'Vietnam';
    let city = 'Ho Chi Minh City';

    const isPrivateIp = (ip) => {
      if (!ip) return true;
      const clean = ip.trim();
      return clean === '127.0.0.1' || 
             clean === '::1' || 
             clean.startsWith('10.') || 
             clean.startsWith('192.168.') || 
             clean.startsWith('172.') || 
             clean.startsWith('::ffff:172.') ||
             clean.startsWith('::ffff:192.168.') ||
             clean.startsWith('::ffff:10.');
    };

    const queryIp = clientIp ? clientIp.split(',')[0].trim() : '';

    if (queryIp && !isPrivateIp(queryIp)) {
      try {
        // Fetch location details from ip-api.com with 3-second timeout
        const controller = new AbortController();
        const timeoutId = setTimeout(() => controller.abort(), 3000);
        
        const geoResponse = await fetch(`http://ip-api.com/json/${queryIp}?fields=status,country,city`, {
          signal: controller.signal
        });
        clearTimeout(timeoutId);

        if (geoResponse.ok) {
          const geoData = await geoResponse.json();
          if (geoData.status === 'success') {
            country = geoData.country || 'Vietnam';
            city = geoData.city || 'Ho Chi Minh City';
          }
        }
      } catch (err) {
        console.error('[GeoIP Error] Failed to resolve IP:', queryIp, err.message);
      }
    } else {
      // Localhost/Private Network fallback
      city = 'Hanoi';
    }

    console.log('[DEBUG GEOIP]', {
      clientIp,
      queryIp,
      isPrivate: isPrivateIp(queryIp),
      country,
      city
    });

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
