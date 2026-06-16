const express = require('express');
const router = express.Router();
const { v4: uuidv4 } = require('uuid');
const auth = require('../middleware/auth');
const Conversation = require('../models/Conversation');
const MedicalEntityLog = require('../models/MedicalEntityLog');
const User = require('../models/User');

const AI_SERVICE_URL = process.env.AI_SERVICE_URL || 'http://ai-service:8000/api/chat';

// Send chat message
router.post('/message', auth, async (req, res) => {
  try {
    const { session_id, message_content } = req.body;
    const user_id = req.user.id;

    if (!message_content) {
      return res.status(400).json({ message: 'Nội dung tin nhắn không được để trống.' });
    }

    let activeSessionId = session_id;
    let conversation = null;

    if (activeSessionId) {
      conversation = await Conversation.findOne({ session_id: activeSessionId, user_id });
    }

    if (!conversation) {
      activeSessionId = activeSessionId || uuidv4();
      conversation = new Conversation({
        session_id: activeSessionId,
        user_id,
        messages: [],
        start_time: new Date()
      });
    }

    // Xây dựng history để gửi cho FastAPI AI Service
    const historyPayload = conversation.messages.map(msg => ({
      role: msg.message_content ? 'user' : 'assistant', // Mongoose subdocs
      content: msg.message_content || msg.ai_response
    }));

    // Cần làm sạch format history: user và assistant đan xen
    const cleanedHistory = [];
    conversation.messages.forEach(msg => {
      cleanedHistory.push({ role: 'user', content: msg.message_content });
      cleanedHistory.push({ role: 'assistant', content: msg.ai_response });
    });

    console.log(`[Chat API] session_id: ${activeSessionId}, history length: ${cleanedHistory.length}`);

    // Gọi FastAPI AI Service
    let aiResponse;
    try {
      const user = await User.findById(user_id);
      const response = await fetch(AI_SERVICE_URL, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          query: message_content,
          history: cleanedHistory,
          user_name: user ? user.full_name : null,
          user_age: user ? user.age : null,
          user_gender: user ? user.gender : null
        })
      });

      if (!response.ok) {
        throw new Error(`AI service returned error: ${response.statusText}`);
      }

      aiResponse = await response.json();
    } catch (apiErr) {
      console.error('FastAPI Connection Error:', apiErr);
      return res.status(500).json({ message: 'Không thể kết nối tới máy chủ AI.' });
    }

    const { answer, sources, sims, metadatas, elapsed, is_zero, extracted_entities } = aiResponse;

    // Lưu tin nhắn mới vào cuộc hội thoại
    conversation.messages.push({
      message_content,
      ai_response: answer,
      elapsed_ms: elapsed,
      is_zero,
      sources,
      sims,
      metadatas,
      timestamp: new Date()
    });

    conversation.total_messages = conversation.messages.length;
    await conversation.save();

    // Lưu Log thực thể y tế trích xuất
    if (extracted_entities && (
      extracted_entities.symptoms.length > 0 ||
      extracted_entities.diseases.length > 0 ||
      extracted_entities.body_parts.length > 0 ||
      extracted_entities.herbs.length > 0
    )) {
      const entityLog = new MedicalEntityLog({
        session_id: activeSessionId,
        user_id,
        symptoms_mentioned: extracted_entities.symptoms,
        diseases_mentioned: extracted_entities.diseases,
        body_parts_mentioned: extracted_entities.body_parts,
        herbs_queried: extracted_entities.herbs,
        timestamp: new Date()
      });
      await entityLog.save();
    }

    res.json({
      session_id: activeSessionId,
      answer,
      sources,
      sims,
      metadatas,
      elapsed,
      is_zero,
      extracted_entities
    });

  } catch (err) {
    console.error(err);
    res.status(500).json({ message: 'Lỗi server khi gửi tin nhắn.' });
  }
});

// Get all chat sessions of current user
router.get('/sessions', auth, async (req, res) => {
  try {
    const user_id = req.user.id;
    const sessions = await Conversation.find({ user_id })
      .select('session_id start_time total_messages feedback_rating')
      .sort({ start_time: -1 });
    res.json(sessions);
  } catch (err) {
    console.error(err);
    res.status(500).json({ message: 'Lỗi server khi lấy danh sách phiên.' });
  }
});

// Get details of a single chat session
router.get('/session/:session_id', auth, async (req, res) => {
  try {
    const { session_id } = req.params;
    const user_id = req.user.id;

    const conversation = await Conversation.findOne({ session_id, user_id });
    if (!conversation) {
      return res.status(404).json({ message: 'Không tìm thấy phiên hội thoại.' });
    }

    res.json(conversation);
  } catch (err) {
    console.error(err);
    res.status(500).json({ message: 'Lỗi server khi lấy chi tiết phiên.' });
  }
});

// Update feedback rating
router.post('/feedback', auth, async (req, res) => {
  try {
    const { session_id, rating } = req.body;
    const user_id = req.user.id;

    if (!session_id || !rating) {
      return res.status(400).json({ message: 'Thiếu session_id hoặc rating.' });
    }

    const conversation = await Conversation.findOne({ session_id, user_id });
    if (!conversation) {
      return res.status(404).json({ message: 'Không tìm thấy phiên hội thoại.' });
    }

    conversation.feedback_rating = rating;
    await conversation.save();

    res.json({ message: 'Cảm ơn bạn đã phản hồi!', rating });
  } catch (err) {
    console.error(err);
    res.status(500).json({ message: 'Lỗi server khi lưu đánh giá.' });
  }
});

module.exports = router;
