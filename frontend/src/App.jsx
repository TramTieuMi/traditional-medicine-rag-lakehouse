// frontend/src/App.jsx

import React, { useState, useEffect, useRef } from 'react';
import tracker from './tracker';
import './App.css';
import { 
  MessageSquare, User, LogOut, Send, Plus, 
  Sparkles, Shield, Mail, Lock, FileText, ChevronRight, Star,
  Menu, ChevronLeft
} from 'lucide-react';

const BACKEND_URL = import.meta.env.VITE_BACKEND_URL || 'http://localhost:5000';
const MINIO_PUBLIC_URL = import.meta.env.VITE_MINIO_PUBLIC_URL || 'http://localhost:9000';

function App() {
  // Navigation & Authentication states
  const [user, setUser] = useState(null);
  const [page, setPage] = useState('login'); // 'login' | 'register' | 'chat' | 'profile'
  
  // Auth Form States
  const [email, setEmail] = useState('');
  const [password, setPassword] = useState('');
  const [fullName, setFullName] = useState('');
  const [age, setAge] = useState('');
  const [gender, setGender] = useState('nam');
  const [authError, setAuthError] = useState('');

  // Chat States
  const [sessions, setSessions] = useState([]);
  const [activeSessionId, setActiveSessionId] = useState(null);
  const [messages, setMessages] = useState([]);
  const [inputMessage, setInputMessage] = useState('');
  const [chatLoading, setChatLoading] = useState(false);
  
  // Feedback rating states
  const [sessionRating, setSessionRating] = useState(null);

  const [sidebarOpen, setSidebarOpen] = useState(true);

  const chatEndRef = useRef(null);

  // 1. Phê duyệt & kiểm tra session lưu sẵn trên client khi khởi động
  useEffect(() => {
    const storedUser = localStorage.getItem('user');
    const token = localStorage.getItem('accessToken');
    if (storedUser && token) {
      setUser(JSON.parse(storedUser));
      setPage('chat');
      tracker.startHeartbeat();
    }
    
    return () => {
      tracker.stopHeartbeat();
    };
  }, []);

  // 2. Tự động tracker ghi nhận page view mỗi khi chuyển hướng Route (page state)
  useEffect(() => {
    tracker.trackPageView(page);
  }, [page]);

  // Cuộn xuống cuối khung chat
  useEffect(() => {
    chatEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [messages, chatLoading]);

  // Tải danh sách phiên chat cũ
  const fetchSessions = async () => {
    const token = localStorage.getItem('accessToken');
    if (!token) return;

    try {
      const res = await fetch(`${BACKEND_URL}/api/chat/sessions`, {
        headers: { 'Authorization': `Bearer ${token}` }
      });
      if (res.ok) {
        const data = await res.json();
        setSessions(data);
      }
    } catch (err) {
      console.error('Failed to fetch sessions', err);
    }
  };

  useEffect(() => {
    if (user && page === 'chat') {
      fetchSessions();
    }
  }, [user, page]);

  // Tải lịch sử tin nhắn của phiên đang chọn
  const loadSessionMessages = async (sessionId) => {
    const token = localStorage.getItem('accessToken');
    if (!token) return;

    try {
      const res = await fetch(`${BACKEND_URL}/api/chat/session/${sessionId}`, {
        headers: { 'Authorization': `Bearer ${token}` }
      });
      if (res.ok) {
        const data = await res.json();
        setActiveSessionId(sessionId);
        setMessages(data.messages || []);
        setSessionRating(data.feedback_rating);
        tracker.trackClick('select_session', { session_id: sessionId });
      }
    } catch (err) {
      console.error('Failed to load session messages', err);
    }
  };

  // Tạo phiên chat mới hoàn toàn
  const handleNewChat = () => {
    setActiveSessionId(null);
    setMessages([]);
    setSessionRating(null);
    tracker.trackClick('click_new_chat');
  };

  // Đăng nhập
  const handleLogin = async (e) => {
    e.preventDefault();
    setAuthError('');
    tracker.trackClick('submit_login');

    try {
      const res = await fetch(`${BACKEND_URL}/api/auth/login`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ email, password })
      });
      const data = await res.json();

      if (!res.ok) {
        setAuthError(data.message || 'Đăng nhập thất bại.');
        return;
      }

      localStorage.setItem('accessToken', data.accessToken);
      localStorage.setItem('refreshToken', data.refreshToken);
      localStorage.setItem('user', JSON.stringify(data.user));

      setUser(data.user);
      setPage('chat');
      tracker.startHeartbeat();
      setEmail('');
      setPassword('');
    } catch (err) {
      setAuthError('Không thể kết nối tới server.');
    }
  };

  // Đăng ký tài khoản (Onboarding)
  const handleRegister = async (e) => {
    e.preventDefault();
    setAuthError('');
    tracker.trackClick('submit_register');

    if (!age || isNaN(age) || parseInt(age) <= 0) {
      setAuthError('Tuổi không hợp lệ.');
      return;
    }

    try {
      const res = await fetch(`${BACKEND_URL}/api/auth/register`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          full_name: fullName,
          email,
          password,
          age: parseInt(age),
          gender
        })
      });
      const data = await res.json();

      if (!res.ok) {
        setAuthError(data.message || 'Đăng ký thất bại.');
        return;
      }

      localStorage.setItem('accessToken', data.accessToken);
      localStorage.setItem('refreshToken', data.refreshToken);
      localStorage.setItem('user', JSON.stringify(data.user));

      setUser(data.user);
      setPage('chat');
      tracker.startHeartbeat();
      setFullName('');
      setEmail('');
      setPassword('');
      setAge('');
    } catch (err) {
      setAuthError('Không thể kết nối tới server.');
    }
  };

  // Đăng xuất
  const handleLogout = () => {
    tracker.trackClick('click_logout');
    localStorage.removeItem('accessToken');
    localStorage.removeItem('refreshToken');
    localStorage.removeItem('user');
    setUser(null);
    setPage('login');
    setSessions([]);
    setMessages([]);
    setActiveSessionId(null);
    tracker.stopHeartbeat();
  };

  // Gửi câu hỏi chat
  const handleSendMessage = async (e) => {
    e?.preventDefault();
    if (!inputMessage.trim()) return;

    const userMessageText = inputMessage;
    setInputMessage('');
    setChatLoading(true);
    tracker.trackClick('send_message', { length: userMessageText.length });

    // Tạo nhanh tin nhắn giả định trên giao diện trước khi gọi API
    setMessages(prev => [...prev, {
      message_content: userMessageText,
      ai_response: '...',
      elapsed_ms: 0,
      sources: []
    }]);

    const token = localStorage.getItem('accessToken');
    try {
      const res = await fetch(`${BACKEND_URL}/api/chat/message`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': `Bearer ${token}`
        },
        body: JSON.stringify({
          session_id: activeSessionId,
          message_content: userMessageText
        })
      });

      if (res.ok) {
        const data = await res.json();
        // Thay thế tin nhắn nháp cuối cùng bằng kết quả thật từ API
        setMessages(prev => {
          const updated = [...prev];
          updated[updated.length - 1] = {
            message_content: userMessageText,
            ai_response: data.answer,
            elapsed_ms: data.elapsed,
            sources: data.sources,
            sims: data.sims,
            metadatas: data.metadatas
          };
          return updated;
        });

        if (!activeSessionId) {
          setActiveSessionId(data.session_id);
          fetchSessions();
        }
      } else {
        setMessages(prev => {
          const updated = [...prev];
          updated[updated.length - 1] = {
            message_content: userMessageText,
            ai_response: 'Có lỗi xảy ra khi gọi dịch vụ AI. Vui lòng thử lại.',
            elapsed_ms: 0,
            sources: []
          };
          return updated;
        });
      }
    } catch (err) {
      console.error(err);
      setMessages(prev => {
        const updated = [...prev];
        updated[updated.length - 1] = {
          message_content: userMessageText,
          ai_response: `Lỗi kết nối: ${err.message || err}. Bạn hãy mở Console trình duyệt (F12) để xem chi tiết lỗi hoặc kiểm tra xem VPN có đang chặn localhost không.`,
          elapsed_ms: 0,
          sources: []
        };
        return updated;
      });
    } finally {
      setChatLoading(false);
    }
  };

  // Đánh giá phản hồi 1-5 sao
  const handleRateSession = async (rating) => {
    if (!activeSessionId) return;
    tracker.trackClick('rate_session', { rating, session_id: activeSessionId });

    const token = localStorage.getItem('accessToken');
    try {
      const res = await fetch(`${BACKEND_URL}/api/chat/feedback`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': `Bearer ${token}`
        },
        body: JSON.stringify({
          session_id: activeSessionId,
          rating
        })
      });
      if (res.ok) {
        setSessionRating(rating);
        fetchSessions();
      }
    } catch (err) {
      console.error('Feedback rating failed', err);
    }
  };

  // ── RENDER 1: Trang Đăng Nhập ───────────────────────────────────────────────
  if (page === 'login') {
    return (
      <div className="auth-wrapper">
        <div className="auth-card">
          <div style={{ textAlign: 'center', marginBottom: 32 }}>
            <div className="brand-logo" style={{ margin: '0 auto 16px' }}>🌿</div>
            <h2 style={{ fontFamily: 'var(--font-display)', fontSize: '1.75rem', color: 'hsl(var(--text-main))' }}>🌿 YHCT Assistant</h2>
            <p style={{ color: 'hsl(var(--text-muted))', fontSize: '0.9rem', marginTop: 6 }}>Chào mừng bạn đến với hệ thống tra cứu Y học cổ truyền</p>
          </div>

          <form onSubmit={handleLogin}>
            {authError && <div style={{ color: '#e63946', fontSize: '0.85rem', marginBottom: 16, textAlign: 'center' }}>{authError}</div>}
            
            <div className="input-group">
              <label className="input-label"><Mail size={14} style={{ marginRight: 6, verticalAlign: 'middle' }} /> Email</label>
              <input 
                type="email" 
                className="input-field" 
                value={email}
                onChange={(e) => setEmail(e.target.value)}
                placeholder="ten@viethan.com" 
                required 
              />
            </div>

            <div className="input-group" style={{ marginBottom: 30 }}>
              <label className="input-label"><Lock size={14} style={{ marginRight: 6, verticalAlign: 'middle' }} /> Mật khẩu</label>
              <input 
                type="password" 
                className="input-field" 
                value={password}
                onChange={(e) => setPassword(e.target.value)}
                placeholder="••••••••" 
                required 
              />
            </div>

            <button type="submit" className="btn-primary" style={{ width: '100%' }}>
              Đăng nhập <ChevronRight size={18} />
            </button>
          </form>

          <p style={{ marginTop: 24, textAlign: 'center', fontSize: '0.85rem', color: 'hsl(var(--text-muted))' }}>
            Chưa có tài khoản? <span style={{ color: 'hsl(var(--emerald-600))', fontWeight: 6, cursor: 'pointer' }} onClick={() => setPage('register')}>Đăng ký ngay</span>
          </p>
        </div>
      </div>
    );
  }

  // ── RENDER 2: Trang Đăng Ký (Onboarding) ──────────────────────────────────
  if (page === 'register') {
    return (
      <div className="auth-wrapper">
        <div className="auth-card" style={{ maxWidth: 520 }}>
          <div style={{ textAlign: 'center', marginBottom: 24 }}>
            <div className="brand-logo" style={{ margin: '0 auto 12px' }}>🌿</div>
            <h2 style={{ fontFamily: 'var(--font-display)', fontSize: '1.6rem' }}>Tạo tài khoản mới</h2>
            <p style={{ color: 'hsl(var(--text-muted))', fontSize: '0.85rem', marginTop: 4 }}>Thu thập thông tin cá nhân ban đầu giúp chatbot tư vấn chuẩn xác hơn</p>
          </div>

          <form onSubmit={handleRegister}>
            {authError && <div style={{ color: '#e63946', fontSize: '0.85rem', marginBottom: 12, textAlign: 'center' }}>{authError}</div>}
            
            <div className="input-group">
              <label className="input-label"><User size={14} style={{ marginRight: 6, verticalAlign: 'middle' }} /> Họ và tên</label>
              <input 
                type="text" 
                className="input-field" 
                value={fullName}
                onChange={(e) => setFullName(e.target.value)}
                placeholder="Nguyễn Văn A" 
                required 
              />
            </div>

            <div className="input-group">
              <label className="input-label"><Mail size={14} style={{ marginRight: 6, verticalAlign: 'middle' }} /> Email</label>
              <input 
                type="email" 
                className="input-field" 
                value={email}
                onChange={(e) => setEmail(e.target.value)}
                placeholder="ten@domain.com" 
                required 
              />
            </div>

            <div className="input-group">
              <label className="input-label"><Lock size={14} style={{ marginRight: 6, verticalAlign: 'middle' }} /> Mật khẩu</label>
              <input 
                type="password" 
                className="input-field" 
                value={password}
                onChange={(e) => setPassword(e.target.value)}
                placeholder="Mật khẩu bảo mật" 
                required 
              />
            </div>

            <div style={{ display: 'flex', gap: 16 }}>
              <div className="input-group" style={{ flex: 1 }}>
                <label className="input-label">Tuổi</label>
                <input 
                  type="number" 
                  className="input-field" 
                  value={age}
                  onChange={(e) => setAge(e.target.value)}
                  placeholder="25" 
                  required 
                />
              </div>

              <div className="input-group" style={{ flex: 1 }}>
                <label className="input-label">Giới tính</label>
                <select 
                  className="input-field" 
                  value={gender}
                  onChange={(e) => setGender(e.target.value)}
                >
                  <option value="nam">Nam</option>
                  <option value="nữ">Nữ</option>
                  <option value="khác">Khác</option>
                </select>
              </div>
            </div>

            <button type="submit" className="btn-primary" style={{ width: '100%', marginTop: 12 }}>
              Hoàn thành Đăng ký
            </button>
          </form>

          <p style={{ marginTop: 20, textAlign: 'center', fontSize: '0.85rem', color: 'hsl(var(--text-muted))' }}>
            Đã có tài khoản? <span style={{ color: 'hsl(var(--emerald-600))', fontWeight: 6, cursor: 'pointer' }} onClick={() => setPage('login')}>Đăng nhập</span>
          </p>
        </div>
      </div>
    );
  }

  // ── RENDER 3 & 4: Trang chính (Chat / Lịch sử / Hồ Sơ) ─────────────────────
  return (
    <div className="app-container">
      {/* Sidebar trái */}
      <aside className={`sidebar ${sidebarOpen ? '' : 'collapsed'}`}>
        <div style={{ width: 272, display: 'flex', flexDirection: 'column', height: '100%', flexShrink: 0 }}>
          <div className="brand-section" style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', width: '100%' }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
              <div className="brand-logo">🌿</div>
              <span className="brand-name">YHCT AI Portal</span>
            </div>
            <button 
              onClick={() => setSidebarOpen(false)}
              title="Đóng sidebar"
              style={{ background: 'none', border: 'none', cursor: 'pointer', color: 'hsl(var(--text-muted))', display: 'flex', alignItems: 'center', padding: 4 }}
            >
              <ChevronLeft size={20} />
            </button>
          </div>

          <button className="btn-primary new-chat-btn" onClick={handleNewChat}>
            <Plus size={18} /> Cuộc hội thoại mới
          </button>

          {/* Danh sách tin nhắn cũ */}
          <div className="nav-section">
            <h4 className="nav-title">Lịch sử tư vấn</h4>
            {sessions.length === 0 ? (
              <p style={{ fontSize: '0.8rem', color: 'hsl(var(--text-muted))', paddingLeft: 8 }}>Chưa có cuộc hội thoại nào</p>
            ) : (
              sessions.map((s) => (
                <div 
                  key={s.session_id} 
                  className={`session-item ${activeSessionId === s.session_id ? 'active' : ''}`}
                  onClick={() => {
                    setPage('chat');
                    loadSessionMessages(s.session_id);
                  }}
                >
                  <MessageSquare size={16} />
                  <div style={{ overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', flex: 1 }}>
                    {s.start_time 
                      ? `${new Date(s.start_time).toLocaleDateString('vi-VN')} ${new Date(s.start_time).toLocaleTimeString('vi-VN')}` 
                      : 'Phiên chat'}
                    {s.feedback_rating && <span style={{ color: '#f59e0b', fontSize: '0.75rem', marginLeft: 6 }}>★{s.feedback_rating}</span>}
                  </div>
                </div>
              ))
            )}
          </div>

          {/* Profile Widget phía dưới */}
          <div className="profile-widget">
            <div className="profile-header">
              <div className="avatar">{user?.full_name?.charAt(0).toUpperCase()}</div>
              <div className="profile-info">
                <h4>{user?.full_name}</h4>
                <p>{user?.email}</p>
              </div>
            </div>
            <div style={{ display: 'flex', gap: 8, marginTop: 12 }}>
              <button 
                className="btn-secondary" 
                style={{ flex: 1, padding: '8px 12px', display: 'flex', alignItems: 'center', justifyContent: 'center', gap: 6, fontSize: '0.8rem' }}
                onClick={() => {
                  setPage(page === 'profile' ? 'chat' : 'profile');
                  tracker.trackClick('click_toggle_profile');
                }}
              >
                <User size={14} /> {page === 'profile' ? 'Trò chuyện' : 'Hồ sơ'}
              </button>
              <button 
                className="btn-secondary" 
                style={{ padding: '8px 12px', display: 'flex', alignItems: 'center', justifyContent: 'center' }}
                onClick={handleLogout}
                title="Đăng xuất"
              >
                <LogOut size={14} style={{ color: '#e63946' }} />
              </button>
            </div>
          </div>
        </div>
      </aside>

      {/* Vùng làm việc chính bên phải */}
      <main style={{ flex: 1, display: 'flex', flexDirection: 'column', height: '100%' }}>
        {page === 'profile' ? (
          // Màn hình 4: Hồ sơ bệnh nhân cá nhân
          <div className="patient-profile-screen">
            <div className="profile-card">
              <div style={{ display: 'flex', alignItems: 'center', gap: 16, borderBottom: '1.5px solid hsl(var(--border-light))', paddingBottom: 20 }}>
                {!sidebarOpen && (
                  <button 
                    onClick={() => setSidebarOpen(true)}
                    title="Mở sidebar"
                    style={{ background: 'none', border: 'none', cursor: 'pointer', color: 'hsl(var(--text-main))', display: 'flex', alignItems: 'center', padding: 8, borderRadius: 'var(--radius-sm)', border: '1.5px solid hsl(var(--border-light))' }}
                  >
                    <Menu size={20} />
                  </button>
                )}
                <div className="avatar" style={{ width: 64, height: 64, fontSize: '1.8rem' }}>{user?.full_name?.charAt(0).toUpperCase()}</div>
                <div>
                  <h2 style={{ fontSize: '1.8rem', color: 'hsl(var(--text-main))' }}>Hồ sơ Bệnh nhân</h2>
                  <p style={{ color: 'hsl(var(--text-muted))' }}>Mã bệnh nhân: #{user?.id?.substring(0, 8)}</p>
                </div>
              </div>

              <div className="profile-info-grid">
                <div className="profile-info-item">
                  <label>Họ và tên</label>
                  <span>{user?.full_name}</span>
                </div>
                <div className="profile-info-item">
                  <label>Email liên lạc</label>
                  <span>{user?.email}</span>
                </div>
                <div className="profile-info-item">
                  <label>Tuổi</label>
                  <span>{user?.age} tuổi</span>
                </div>
                <div className="profile-info-item">
                  <label>Giới tính</label>
                  <span style={{ textTransform: 'capitalize' }}>{user?.gender}</span>
                </div>
                <div className="profile-info-item">
                  <label>Thành viên từ</label>
                  <span>{user?.created_at ? new Date(user.created_at).toLocaleDateString('vi-VN') : 'Mới tham gia'}</span>
                </div>
                <div className="profile-info-item">
                  <label>Phiên tư vấn</label>
                  <span>{sessions.length} phiên đã ghi nhận</span>
                </div>
              </div>
            </div>
          </div>
        ) : (
          // Màn hình 3: Giao diện Chat tư vấn AI
          <div className="main-chat-area">
            <header className="chat-header">
              <div style={{ display: 'flex', alignItems: 'center', gap: 16 }}>
                {!sidebarOpen && (
                  <button 
                    onClick={() => setSidebarOpen(true)}
                    title="Mở sidebar"
                    style={{ background: 'none', border: 'none', cursor: 'pointer', color: 'hsl(var(--text-main))', display: 'flex', alignItems: 'center', padding: 8, borderRadius: 'var(--radius-sm)', border: '1.5px solid hsl(var(--border-light))' }}
                  >
                    <Menu size={20} />
                  </button>
                )}
                <div className="chat-header-title">
                  <h3>🌿 Trợ lý Y học Cổ truyền AI</h3>
                  <p>Hỏi về bài thuốc và phương pháp YHCT điều trị các căn bệnh thường gặp</p>
                </div>
              </div>
            </header>

            {/* Khung chat chứa tin nhắn */}
            <div className="chat-feed">
              {messages.length === 0 ? (
                <div className="welcome-screen">
                  <div className="welcome-icon">🌿</div>
                  <h2>Xin chào, {user?.full_name}!</h2>
                  <p>Hệ thống AI sẽ tư vấn các bài thuốc và phương pháp điều trị các căn bệnh thường gặp (tiêu hóa, mất ngủ, xương khớp, cảm mạo...) bằng Y học cổ truyền dựa trên nguồn tài liệu chuẩn y khoa. Hãy thử một số gợi ý dưới đây:</p>
                  
                  <div className="suggestion-box">
                    <div className="suggestion-card" onClick={() => { setInputMessage('Bài thuốc đông y điều trị đau dạ dày hiệu quả'); tracker.trackClick('click_suggestion', { query: 'Bài thuốc trị đau dạ dày' }); }}>
                      <h4>🌱 Đau dạ dày (Vị quản thống)</h4>
                      <p>Tìm hiểu các bài thuốc cổ phương kiện tỳ vị, giảm đau và viêm loét thượng vị.</p>
                    </div>
                    <div className="suggestion-card" onClick={() => { setInputMessage('Các phương pháp đông y trị mất ngủ và suy nhược thần kinh'); tracker.trackClick('click_suggestion', { query: 'Phương pháp trị mất ngủ' }); }}>
                      <h4>😴 Mất ngủ (Thất miên)</h4>
                      <p>Các vị thuốc an thần, dưỡng tâm bổ tỳ giúp cải thiện giấc ngủ tự nhiên.</p>
                    </div>
                    <div className="suggestion-card" onClick={() => { setInputMessage('Cách điều trị táo bón và ăn uống không tiêu bằng thảo dược'); tracker.trackClick('click_suggestion', { query: 'Điều trị táo bón khó tiêu' }); }}>
                      <h4>🏥 Táo bón khó tiêu</h4>
                      <p>Các giải pháp nhuận tràng, thông tiện, ích khí kiện tỳ từ dược liệu tự nhiên.</p>
                    </div>
                    <div className="suggestion-card" onClick={() => { setInputMessage('Bài thuốc đông y trị cảm mạo, ho khan và ho có đờm'); tracker.trackClick('click_suggestion', { query: 'Trị cảm mạo ho khan' }); }}>
                      <h4>🤧 Cảm mạo & Ho</h4>
                      <p>Tra cứu các bài thuốc tán phong hàn, thanh phong nhiệt và giảm ho thường gặp.</p>
                    </div>
                  </div>
                </div>
              ) : (
                messages.map((msg, index) => (
                  <React.Fragment key={index}>
                    {msg.message_content && (
                      <div className="message-bubble-row user">
                        <div className="bubble">
                          <p style={{ whiteSpace: 'pre-wrap' }}>{msg.message_content}</p>
                        </div>
                      </div>
                    )}
                    {msg.ai_response && msg.ai_response !== '...' && (
                      <div className="message-bubble-row assistant">
                        <div className="bubble">
                          <p style={{ whiteSpace: 'pre-wrap' }}>{msg.ai_response}</p>
                          
                          {/* Hiển thị metadata / nguồn PDF khi có phản hồi của AI */}
                          {msg.sources && msg.sources.length > 0 && (
                            <div className="message-metadata">
                              <h5 className="sources-title">Nguồn Tài liệu Tham khảo:</h5>
                              <div className="sources-container">
                                {msg.metadatas && msg.metadatas.map((meta, mIdx) => {
                                  const sourceFile = meta.source || msg.sources[mIdx];
                                  const pageNum = meta.page_num || '?';
                                  const pdfUrl = `${MINIO_PUBLIC_URL}/yhct-docs/${sourceFile}`;
                                  const bookName = sourceFile.replace('.pdf', '').replace(/_/g, ' ').toUpperCase();
                                  
                                  return (
                                    <a 
                                      key={mIdx}
                                      href={pdfUrl}
                                      target="_blank" 
                                      rel="noopener noreferrer" 
                                      className="source-badge"
                                    >
                                      📚 {bookName} (tr.{pageNum})
                                    </a>
                                  );
                                })}
                              </div>
                              {msg.elapsed_ms > 0 && (
                                <div style={{ fontSize: '0.7rem', color: 'hsl(var(--text-muted))', marginTop: 4 }}>
                                  Phản hồi được xử lý trong {msg.elapsed_ms}ms.
                                </div>
                              )}
                            </div>
                          )}
                        </div>
                      </div>
                    )}
                  </React.Fragment>
                ))
              )}

              {/* Icon Loading suy nghĩ */}
              {chatLoading && (
                <div className="message-bubble-row assistant">
                  <div className="bubble" style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                    <Sparkles size={16} className="star-icon" style={{ animation: 'spin 1.5s linear infinite' }} />
                    <span style={{ fontSize: '0.9rem', color: 'hsl(var(--text-muted))' }}>Đang tra cứu kho y học cổ truyền...</span>
                  </div>
                </div>
              )}
              
              <div ref={chatEndRef} />
            </div>

            {/* Đánh giá session ở dưới cùng khi có hội thoại */}
            {activeSessionId && messages.length > 0 && (
              <div style={{ backgroundColor: 'hsl(var(--bg-card))', padding: '10px 32px', borderTop: '1px solid hsl(var(--border-light))', display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
                <span style={{ fontSize: '0.85rem', color: 'hsl(var(--text-muted))' }}>Bạn có hài lòng với câu trả lời của Trợ lý AI?</span>
                <div className="feedback-section">
                  <div className="feedback-stars">
                    {[1, 2, 3, 4, 5].map((star) => (
                      <Star 
                        key={star}
                        size={18}
                        className="star-icon"
                        style={{
                          fill: star <= (sessionRating || 0) ? '#f59e0b' : 'none',
                          color: star <= (sessionRating || 0) ? '#f59e0b' : 'hsl(var(--text-muted))'
                        }}
                        onClick={() => handleRateSession(star)}
                      />
                    ))}
                  </div>
                  {sessionRating && <span style={{ fontSize: '0.8rem', color: '#f59e0b', fontWeight: 6 }}>Đã đánh giá {sessionRating}★</span>}
                </div>
              </div>
            )}

            {/* Input Bar */}
            <div className="input-container">
              <form onSubmit={handleSendMessage} className="input-bar">
                <input 
                  type="text" 
                  className="chat-input-field" 
                  value={inputMessage}
                  onChange={(e) => setInputMessage(e.target.value)}
                  placeholder="Hỏi về bài thuốc, dược liệu hoặc các triệu chứng..." 
                  disabled={chatLoading}
                />
                <button type="submit" className="send-message-btn" disabled={chatLoading || !inputMessage.trim()}>
                  <Send size={16} />
                </button>
              </form>
            </div>
          </div>
        )}
      </main>
    </div>
  );
}

export default App;
