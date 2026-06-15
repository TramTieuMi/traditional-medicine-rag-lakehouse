const express = require('express');
const router = express.Router();
const bcrypt = require('bcryptjs');
const jwt = require('jsonwebtoken');
const User = require('../models/User');

const JWT_SECRET = process.env.JWT_SECRET || 'super_secret_jwt_key_yhct_2026';
const JWT_REFRESH_SECRET = process.env.JWT_REFRESH_SECRET || 'super_secret_refresh_key_yhct_2026';

// Register
router.post('/register', async (req, res) => {
  try {
    const { full_name, email, password, age, gender } = req.body;

    if (!full_name || !email || !password || !age || !gender) {
      return res.status(400).json({ message: 'Vui lòng điền đầy đủ thông tin.' });
    }

    let user = await User.findOne({ email });
    if (user) {
      return res.status(400).json({ message: 'Email đã được đăng ký sử dụng.' });
    }

    const salt = await bcrypt.genSalt(10);
    const password_hash = await bcrypt.hash(password, salt);

    user = new User({
      full_name,
      email,
      password_hash,
      age,
      gender
    });

    await user.save();

    const payload = { id: user._id, email: user.email };
    const accessToken = jwt.sign(payload, JWT_SECRET, { expiresIn: '1h' });
    const refreshToken = jwt.sign(payload, JWT_REFRESH_SECRET, { expiresIn: '7d' });

    res.status(201).json({
      accessToken,
      refreshToken,
      user: {
        id: user._id,
        full_name: user.full_name,
        email: user.email,
        age: user.age,
        gender: user.gender
      }
    });

  } catch (err) {
    console.error(err);
    res.status(500).json({ message: 'Lỗi server khi đăng ký.' });
  }
});

// Login
router.post('/login', async (req, res) => {
  try {
    const { email, password } = req.body;

    if (!email || !password) {
      return res.status(400).json({ message: 'Vui lòng cung cấp email và mật khẩu.' });
    }

    const user = await User.findOne({ email });
    if (!user) {
      return res.status(400).json({ message: 'Thông tin tài khoản hoặc mật khẩu không chính xác.' });
    }

    const isMatch = await bcrypt.compare(password, user.password_hash);
    if (!isMatch) {
      return res.status(400).json({ message: 'Thông tin tài khoản hoặc mật khẩu không chính xác.' });
    }

    user.last_login_at = new Date();
    await user.save();

    const payload = { id: user._id, email: user.email };
    const accessToken = jwt.sign(payload, JWT_SECRET, { expiresIn: '1h' });
    const refreshToken = jwt.sign(payload, JWT_REFRESH_SECRET, { expiresIn: '7d' });

    res.json({
      accessToken,
      refreshToken,
      user: {
        id: user._id,
        full_name: user.full_name,
        email: user.email,
        age: user.age,
        gender: user.gender
      }
    });

  } catch (err) {
    console.error(err);
    res.status(500).json({ message: 'Lỗi server khi đăng nhập.' });
  }
});

// Refresh Token
router.post('/refresh', async (req, res) => {
  const { refreshToken } = req.body;
  if (!refreshToken) {
    return res.status(401).json({ message: 'No refresh token provided.' });
  }

  try {
    const decoded = jwt.verify(refreshToken, JWT_REFRESH_SECRET);
    const payload = { id: decoded.id, email: decoded.email };
    const accessToken = jwt.sign(payload, JWT_SECRET, { expiresIn: '1h' });

    res.json({ accessToken });
  } catch (err) {
    res.status(403).json({ message: 'Invalid or expired refresh token.' });
  }
});

module.exports = router;
