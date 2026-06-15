// frontend/src/tracker.js

const BACKEND_URL = import.meta.env.VITE_BACKEND_URL || 'http://localhost:5000';

class AnalyticsTracker {
  constructor() {
    this.sessionId = this._getOrCreateSessionId();
    this.firstVisitAt = this._getOrCreateFirstVisit();
    this.totalSessions = this._incrementSessionCount();
    this.referrerUrl = document.referrer || '';
    this.utm = this._parseUtmParams();
    this.clientInfo = this._getClientInfo();
    this.pagesVisited = [];
    this.pageViews = 0;
    this.startTime = Date.now();
    this.heartbeatTimer = null;
    this.hasInteracted = false;

    // Lắng nghe sự kiện để xác định tương tác (tránh bounce)
    window.addEventListener('click', () => { this.hasInteracted = true; });
    window.addEventListener('keydown', () => { this.hasInteracted = true; });
  }

  // Khởi chạy Heartbeat để đếm thời gian phiên hội thoại
  startHeartbeat() {
    if (this.heartbeatTimer) clearInterval(this.heartbeatTimer);
    
    // Gửi sự kiện ban đầu
    this.trackPageView(window.location.pathname);

    this.heartbeatTimer = setInterval(() => {
      const durationSec = (Date.now() - this.startTime) / 1000;
      const isBounce = !this.hasInteracted && durationSec < 15;

      this._sendEvent('session_heartbeat', {
        session_duration_sec: durationSec,
        bounce: isBounce,
      });
    }, 10000); // 10 giây một lần
  }

  stopHeartbeat() {
    if (this.heartbeatTimer) {
      clearInterval(this.heartbeatTimer);
    }
  }

  // Ghi nhận Page View
  trackPageView(route) {
    this.pageViews += 1;
    if (!this.pagesVisited.includes(route)) {
      this.pagesVisited.push(route);
    }
    
    const durationSec = (Date.now() - this.startTime) / 1000;
    
    this._sendEvent('page_view', {
      route,
      page_views: this.pageViews,
      pages_visited: this.pagesVisited,
      session_duration_sec: durationSec
    });
  }

  // Ghi nhận Click Nút
  trackClick(buttonName, extra = {}) {
    this.hasInteracted = true;
    this._sendEvent('click', {
      button_name: buttonName,
      ...extra
    });
  }

  // Gửi sự kiện về Backend
  async _sendEvent(eventType, eventData = {}) {
    const token = localStorage.getItem('accessToken');
    const durationSec = (Date.now() - this.startTime) / 1000;
    const isBounce = !this.hasInteracted && durationSec < 15;

    const payload = {
      session_id: this.sessionId,
      event_type: eventType,
      referrer_url: this.referrerUrl,
      utm_source: this.utm.source,
      utm_medium: this.utm.medium,
      device_type: this.clientInfo.deviceType,
      browser: this.clientInfo.browser,
      os: this.clientInfo.os,
      bounce: isBounce,
      first_visit_at: this.firstVisitAt,
      session_duration_sec: durationSec,
      pages_visited: this.pagesVisited,
      page_views: this.pageViews,
      ...eventData
    };

    try {
      const headers = { 'Content-Type': 'application/json' };
      if (token) {
        headers['Authorization'] = `Bearer ${token}`;
      }

      await fetch(`${BACKEND_URL}/api/analytics/event`, {
        method: 'POST',
        headers,
        body: JSON.stringify(payload)
      });
    } catch (err) {
      console.warn('Analytics logging failed', err);
    }
  }

  // Helpers
  _getOrCreateSessionId() {
    let sid = sessionStorage.getItem('analytics_session_id');
    if (!sid) {
      sid = 'sid_' + Math.random().toString(36).substr(2, 9) + '_' + Date.now().toString(36);
      sessionStorage.setItem('analytics_session_id', sid);
    }
    return sid;
  }

  _getOrCreateFirstVisit() {
    let fv = localStorage.getItem('analytics_first_visit');
    if (!fv) {
      fv = new Date().toISOString();
      localStorage.setItem('analytics_first_visit', fv);
    }
    return fv;
  }

  _incrementSessionCount() {
    let sessions = parseInt(localStorage.getItem('analytics_total_sessions') || '0', 10);
    // Chỉ cộng dồn nếu là tab/session mới thực sự
    if (!sessionStorage.getItem('session_counted')) {
      sessions += 1;
      localStorage.setItem('analytics_total_sessions', sessions.toString());
      sessionStorage.setItem('session_counted', 'true');
    }
    return sessions;
  }

  _parseUtmParams() {
    const params = new URLSearchParams(window.location.search);
    return {
      source: params.get('utm_source') || null,
      medium: params.get('utm_medium') || null
    };
  }

  _getClientInfo() {
    const ua = navigator.userAgent;
    let deviceType = 'desktop';
    if (/tablet|ipad|playbook|silk/i.test(ua)) {
      deviceType = 'tablet';
    } else if (/mobile|iphone|ipod|blackberry|opera mini|iemobile/i.test(ua)) {
      deviceType = 'mobile';
    }

    // Phân tích đơn giản Browser
    let browser = 'Unknown';
    if (ua.includes('Firefox')) browser = 'Firefox';
    else if (ua.includes('SamsungBrowser')) browser = 'Samsung Browser';
    else if (ua.includes('Opera') || ua.includes('OPR')) browser = 'Opera';
    else if (ua.includes('Trident')) browser = 'Internet Explorer';
    else if (ua.includes('Edge') || ua.includes('Edg')) browser = 'Edge';
    else if (ua.includes('Chrome')) browser = 'Chrome';
    else if (ua.includes('Safari')) browser = 'Safari';

    // Phân tích đơn giản OS
    let os = 'Unknown';
    if (ua.includes('Windows NT')) os = 'Windows';
    else if (ua.includes('Mac OS X')) os = 'MacOS';
    else if (ua.includes('Android')) os = 'Android';
    else if (ua.includes('iPhone') || ua.includes('iPad')) os = 'iOS';
    else if (ua.includes('Linux')) os = 'Linux';

    return { deviceType, browser, os };
  }
}

const tracker = new AnalyticsTracker();
export default tracker;
