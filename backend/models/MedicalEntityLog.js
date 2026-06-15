const mongoose = require('mongoose');

const MedicalEntityLogSchema = new mongoose.Schema({
  session_id: {
    type: String,
    required: true,
    index: true
  },
  user_id: {
    type: mongoose.Schema.Types.ObjectId,
    ref: 'User',
    required: true,
    index: true
  },
  timestamp: {
    type: Date,
    default: Date.now
  },
  symptoms_mentioned: [String],
  diseases_mentioned: [String],
  body_parts_mentioned: [String],
  herbs_queried: [String]
});

module.exports = mongoose.model('MedicalEntityLog', MedicalEntityLogSchema);
