const mongoose = require('mongoose');

const AnalyticsEventSchema = new mongoose.Schema({
  session_id: {
    type: String,
    required: true,
    index: true
  },
  user_id: {
    type: mongoose.Schema.Types.ObjectId,
    ref: 'User',
    default: null,
    index: true
  },
  event_type: {
    type: String,
    enum: ['page_view', 'click', 'session_heartbeat'],
    required: true
  },
  timestamp: {
    type: Date,
    default: Date.now
  },
  device_type: {
    type: String,
    enum: ['mobile', 'tablet', 'desktop'],
    default: 'desktop'
  },
  browser: String,
  os: String,
  ip_address: String,
  country: {
    type: String,
    default: 'Vietnam'
  },
  city: {
    type: String,
    default: 'Unknown'
  },
  referrer_url: String,
  route: String,
  button_name: String,
  search_keywords: [String],
  bounce: {
    type: Boolean,
    default: false
  },
  first_visit_at: Date,
  utm_source: String,
  utm_medium: String
});

module.exports = mongoose.model('AnalyticsEvent', AnalyticsEventSchema);
