const mongoose = require('mongoose');

const MessageSchema = new mongoose.Schema({
  message_content: {
    type: String,
    required: true
  },
  ai_response: {
    type: String,
    required: true
  },
  timestamp: {
    type: Date,
    default: Date.now
  },
  elapsed_ms: {
    type: Number,
    required: true
  },
  is_zero: {
    type: Boolean,
    default: false
  },
  sources: [String],
  sims: [Number],
  metadatas: [mongoose.Schema.Types.Mixed]
});

const ConversationSchema = new mongoose.Schema({
  session_id: {
    type: String,
    required: true,
    unique: true
  },
  user_id: {
    type: mongoose.Schema.Types.ObjectId,
    ref: 'User',
    required: true
  },
  start_time: {
    type: Date,
    default: Date.now
  },
  total_messages: {
    type: Number,
    default: 0
  },
  session_duration_sec: {
    type: Number,
    default: 0
  },
  feedback_rating: {
    type: Number,
    min: 1,
    max: 5,
    default: null
  },
  messages: [MessageSchema]
});

module.exports = mongoose.model('Conversation', ConversationSchema);
