# streamlit_app/pages/1_user_management.py

import os
import streamlit as st
import polars as pl
from io import BytesIO
from minio import Minio
import json

st.set_page_config(page_title="YHCT User Management", page_icon="👥", layout="wide")

MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT",   "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY",  "minio")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY",  "minio123")
MINIO_PUBLIC_URL = os.getenv("MINIO_PUBLIC_URL", "http://localhost:9000")

@st.cache_data(ttl=60)
def load_parquet(bucket: str, key: str) -> pl.DataFrame:
    client = Minio(MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, secure=False)
    obj    = client.get_object(bucket, key)
    return pl.read_parquet(BytesIO(obj.read()))

# ── CSS ──────────────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;500;600;700&display=swap');

html, body, [class*="css"], .stApp {
    font-family: 'Outfit', sans-serif !important;
}

.user-title { color: #1a6b3c; font-weight: bold; margin-bottom: 20px; }
.chat-container {
    background-color: var(--background-color, #ffffff);
    border-radius: 20px;
    padding: 24px;
    border: 1.5px solid #eef4f0;
    max-height: 700px;
    overflow-y: auto;
    box-shadow: 0 10px 25px rgba(26, 107, 60, 0.03);
}
.user-msg {
    background: linear-gradient(135deg, #1a6b3c 0%, #2d9e5f 100%);
    color: white;
    padding: 12px 18px;
    border-radius: 20px 20px 4px 20px;
    margin: 8px 0 8px auto;
    width: fit-content;
    max-width: 80%;
    font-size: 14px;
    line-height: 1.5;
    text-align: left;
    box-shadow: 0 4px 10px rgba(26, 107, 60, 0.15);
}
.ai-msg {
    background-color: #ffffff;
    color: #1c3225;
    padding: 14px 18px;
    border-radius: 20px 20px 20px 4px;
    margin: 8px auto 8px 0;
    width: fit-content;
    max-width: 80%;
    font-size: 14px;
    line-height: 1.6;
    border: 1.5px solid #eef4f0;
    box-shadow: 0 4px 12px rgba(26, 107, 60, 0.02);
}
.source-badge {
    display: inline-flex;
    align-items: center;
    gap: 4px;
    background-color: rgba(45, 158, 95, 0.08);
    color: #1a6b3c;
    border: 1px solid rgba(45, 158, 95, 0.15);
    border-radius: 20px;
    padding: 4px 12px;
    font-size: 12px;
    margin: 6px 6px 0 0;
    text-decoration: none;
    font-weight: 500;
    transition: all 0.2s ease;
}
.source-badge:hover {
    background-color: rgba(45, 158, 95, 0.15) !important;
    transform: translateY(-1px);
    box-shadow: 0 2px 6px rgba(26, 107, 60, 0.1);
    color: #1a6b3c !important;
}
.metadata-line {
    font-size: 11px;
    color: #888888;
    opacity: 0.7;
    margin-top: 8px;
}

/* Dark mode overrides */
@media (prefers-color-scheme: dark) {
    .chat-container {
        background-color: #131714;
        border-color: #2d5a3c;
    }
    .ai-msg {
        background-color: #1e221f;
        color: #e2e8f0;
        border-color: #2d5a3c;
    }
    .metadata-line {
        color: #b0c2b5;
    }
}
</style>
""", unsafe_allow_html=True)

st.title("👥 Quản lý Người dùng & Lịch sử Hội thoại")
st.markdown("---")

try:
    df_users = load_parquet("yhct-silver", "silver/mongodb/silver_mongodb_users.parquet")
except Exception as e:
    st.warning("⚠ Chưa tìm thấy dữ liệu người dùng từ Lakehouse. Hãy kiểm tra hoặc chạy ETL pipeline.")
    st.stop()

if df_users.is_empty():
    st.info("Chưa có người dùng nào được đăng ký trong hệ thống.")
    st.stop()

# 3 Columns Layout
col_users, col_sessions, col_chat = st.columns([1.2, 1.2, 2.6])

with col_users:
    st.subheader("🧑 Danh sách người dùng")
    search_name = st.text_input("🔍 Tìm theo tên người dùng", placeholder="Nhập tên...")
    
    # Filter users based on search query
    filtered_users = df_users
    if search_name:
        # Case insensitive search using str.to_lowercase()
        filtered_users = df_users.filter(
            pl.col("full_name").str.to_lowercase().str.contains(search_name.lower(), literal=True)
        )
    
    if filtered_users.is_empty():
        st.info("Không tìm thấy người dùng.")
        selected_user_id = None
    else:
        user_options = {
            f"{row['full_name']} ({row['gender'].upper()}, {row['age']} tuổi)": row['user_id']
            for row in filtered_users.iter_rows(named=True)
        }
        selected_user_label = st.selectbox("Chọn người dùng", list(user_options.keys()))
        selected_user_id = user_options[selected_user_label]
        
        # Display details of selected user
        user_row = df_users.filter(pl.col("user_id") == selected_user_id).row(0, named=True)
        st.markdown(f"""
        **Chi tiết tài khoản:**
        - **User ID:** `{user_row['user_id']}`
        - **Họ tên:** `{user_row['full_name']}`
        - **Email Hashed:** `{user_row['email_hashed'][:20]}...`
        - **Độ tuổi:** {user_row['age']} tuổi
        - **Giới tính:** {user_row['gender'].upper()}
        - **Đăng ký ngày:** `{user_row['created_at'][:10]}`
        - **Hoạt động cuối:** `{user_row['last_login_at'][:19]}`
        """)

with col_sessions:
    st.subheader("💬 Danh sách phiên chat")
    if selected_user_id:
        try:
            df_convs = load_parquet("yhct-silver", "silver/mongodb/silver_mongodb_conversations.parquet")
        except Exception as e:
            st.error(f"Lỗi đọc dữ liệu hội thoại: {e}")
            st.stop()
            
        user_convs = df_convs.filter(pl.col("user_id") == selected_user_id)
        if user_convs.is_empty():
            st.info("Người dùng này chưa có cuộc hội thoại nào.")
            selected_session_id = None
        else:
            # Format label for sessions
            conv_options = {
                f"🕒 {row['start_time'][11:19]} ({row['total_messages']} tin nhắn)": row['session_id']
                for row in user_convs.sort("start_time", descending=True).iter_rows(named=True)
            }
            selected_conv_label = st.selectbox("Chọn phiên trò chuyện", list(conv_options.keys()))
            selected_session_id = conv_options[selected_conv_label]
            
            # Show details of selected session
            conv_row = user_convs.filter(pl.col("session_id") == selected_session_id).row(0, named=True)
            rating = conv_row['feedback_rating']
            rating_str = "⭐" * rating if rating and rating > 0 else "Chưa đánh giá"
            
            st.markdown(f"""
            **Tổng quan phiên chat:**
            - **Session ID:** `{conv_row['session_id']}`
            - **Bắt đầu:** `{conv_row['start_time'][:19]}`
            - **Thời gian chat:** {conv_row['session_duration_sec']:.1f} giây
            - **Đánh giá Admin:** {rating_str}
            """)
    else:
        st.info("Vui lòng chọn một người dùng.")
        selected_session_id = None

with col_chat:
    st.subheader("🕵 Lịch sử hội thoại chi tiết")
    if selected_session_id and selected_user_id:
        conv_row = user_convs.filter(pl.col("session_id") == selected_session_id).row(0, named=True)
        messages_str = conv_row.get("messages_json", "[]")
        
        try:
            messages = json.loads(messages_str)
        except Exception as e:
            messages = []
            st.error(f"Không thể giải mã dữ liệu tin nhắn: {e}")
            
        if not messages:
            st.info("Không có tin nhắn nào trong phiên này.")
        else:
            st.markdown('<div class="chat-container">', unsafe_allow_html=True)
            for msg in messages:
                # 1. User Message
                user_msg = msg.get("message_content", "")
                st.markdown(f'<div class="user-msg">{user_msg}</div>', unsafe_allow_html=True)
                
                # 2. AI Response
                ai_res = msg.get("ai_response", "")
                
                # 3. Citation sources
                sources = msg.get("sources", [])
                sims = msg.get("sims", [])
                
                sources_html = ""
                if sources and len(sources) == len(sims):
                    sources_html += '<div style="margin-top: 8px;">'
                    for src, sim in zip(sources, sims):
                        pdf_url = f"{MINIO_PUBLIC_URL}/yhct-docs/{src}"
                        sources_html += f'<a href="{pdf_url}" target="_blank" class="source-badge">📚 {src} · tr. ({sim*100:.1f}%)</a>'
                    sources_html += '</div>'
                
                elapsed = msg.get("elapsed_ms", 0)
                is_zero = msg.get("is_zero", False)
                fallback_text = '<span style="color:#d93025; font-weight:bold; margin-left:10px;">⚠️ RAG Fallback / Tin xã giao</span>' if is_zero else ""
                
                metadata_html = f'<div class="metadata-line">⏱ Phản hồi sau: {elapsed}ms {fallback_text}</div>'
                
                st.markdown(f"""<div class="ai-msg">
{ai_res}
{sources_html}
{metadata_html}
</div>""", unsafe_allow_html=True)
            st.markdown('</div>', unsafe_allow_html=True)
    else:
        st.info("Vui lòng chọn một phiên hội thoại để duyệt lịch sử chat.")
