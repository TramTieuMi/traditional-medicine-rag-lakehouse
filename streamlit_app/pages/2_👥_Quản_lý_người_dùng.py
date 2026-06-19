# streamlit_app/pages/2_user_management.py

import json
import os
from io import BytesIO

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import polars as pl
import streamlit as st
from minio import Minio

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


def _parse_json_list_column(series: pl.Series) -> list[str]:
    result = []
    for val in series.drop_nulls().to_list():
        try:
            items = json.loads(val)
            if isinstance(items, list):
                result.extend(str(i).strip() for i in items if i)
        except Exception:
            pass
    return result


# ── CSS ──────────────────────────────────────────────────────────────────────
st.markdown("""
<style>
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
.analytics-card {
    background: #f8fdf9;
    border: 1px solid #d4edda;
    border-radius: 12px;
    padding: 16px;
    margin: 8px 0;
}
@media (prefers-color-scheme: dark) {
    .chat-container { background-color: #131714; border-color: #2d5a3c; }
    .ai-msg         { background-color: #1e221f; color: #e2e8f0; border-color: #2d5a3c; }
    .metadata-line  { color: #b0c2b5; }
    .analytics-card { background: #1a2b1f; border-color: #2d5a3c; }
}
</style>
""", unsafe_allow_html=True)

# ── Header ────────────────────────────────────────────────────────────────────
st.title("👥 Quản lý Người dùng & Lịch sử Hội thoại")
st.page_link("pages/3_📊_Phân_tích_dữ_liệu.py", label="→ Xem dashboard analytics tổng thể", icon="📊")
st.markdown("---")

# ── Load silver users ─────────────────────────────────────────────────────────
try:
    df_users = load_parquet("yhct-silver", "silver/mongodb/silver_mongodb_users.parquet")
except Exception as e:
    st.warning("⚠ Chưa tìm thấy dữ liệu người dùng từ Lakehouse. Hãy kiểm tra hoặc chạy ETL pipeline.")
    st.stop()

if df_users.is_empty():
    st.info("Chưa có người dùng nào được đăng ký trong hệ thống.")
    st.stop()

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 1: Tổng quan analytics toàn bộ người dùng
# ═══════════════════════════════════════════════════════════════════════════════
st.subheader("📈 Tổng quan người dùng hệ thống")

# Load gold tables for aggregate stats
try:
    gold_chat_all = load_parquet("yhct-gold", "gold/mongodb/gold_chat_performance.parquet")
    chat_all_pd   = gold_chat_all.to_pandas() if not gold_chat_all.is_empty() else pd.DataFrame()
except Exception:
    chat_all_pd   = pd.DataFrame()

try:
    gold_med_all  = load_parquet("yhct-gold", "gold/mongodb/gold_medical_insights.parquet")
except Exception:
    gold_med_all  = pl.DataFrame()

try:
    df_convs_all  = load_parquet("yhct-silver", "silver/mongodb/silver_mongodb_conversations.parquet")
    convs_all_pd  = df_convs_all.to_pandas() if not df_convs_all.is_empty() else pd.DataFrame()
except Exception:
    convs_all_pd  = pd.DataFrame()

# ── KPI row ────────────────────────────────────────────────────────────────
total_users     = df_users.shape[0]
total_sessions  = len(chat_all_pd) if not chat_all_pd.empty else 0
avg_qs          = float(chat_all_pd["total_messages_exchanged"].mean()) if not chat_all_pd.empty else 0.0
avg_dur         = float(convs_all_pd["session_duration_sec"].mean()) if not convs_all_pd.empty else 0.0
total_qs        = int(chat_all_pd["total_messages_exchanged"].sum()) if not chat_all_pd.empty else 0
avg_rating      = float(chat_all_pd["feedback_rating"].dropna().mean()) if not chat_all_pd.empty else 0.0

k1, k2, k3, k4, k5, k6 = st.columns(6)
k1.metric("Tổng người dùng",       f"{total_users:,}")
k2.metric("Tổng phiên chat",        f"{total_sessions:,}")
k3.metric("Tổng câu hỏi",          f"{total_qs:,}")
k4.metric("TB câu hỏi/phiên",      f"{avg_qs:.1f}")
k5.metric("TB thời gian/phiên",    f"{avg_dur:.1f}s")
k6.metric("Đánh giá trung bình",   f"{avg_rating:.1f} ⭐" if avg_rating else "Chưa có")

# ── Charts row ─────────────────────────────────────────────────────────────
ov_c1, ov_c2, ov_c3 = st.columns(3)

with ov_c1:
    # User registration trend
    if "created_at" in df_users.columns:
        reg_pd = df_users.select("created_at").to_pandas()
        reg_pd["date"] = pd.to_datetime(reg_pd["created_at"], errors="coerce").dt.strftime("%Y-%m-%d")
        reg_by_day = reg_pd.groupby("date").size().reset_index(name="Người đăng ký")
        if not reg_by_day.empty:
            fig_reg = px.bar(
                reg_by_day, x="date", y="Người đăng ký",
                title="📅 Người dùng đăng ký theo ngày",
                color="Người đăng ký", color_continuous_scale="Greens",
                height=280,
            )
            fig_reg.update_layout(xaxis_title="", plot_bgcolor="rgba(0,0,0,0)",
                                  coloraxis_showscale=False)
            st.plotly_chart(fig_reg, use_container_width=True)

with ov_c2:
    # Questions per session distribution (all users)
    if not chat_all_pd.empty:
        fig_qs_dist = px.histogram(
            chat_all_pd, x="total_messages_exchanged", nbins=20,
            title="❓ Phân bố câu hỏi/phiên (toàn hệ thống)",
            color_discrete_sequence=["#E67E22"],
            labels={"total_messages_exchanged": "Số câu hỏi", "count": "Số phiên"},
            height=280,
        )
        fig_qs_dist.update_layout(plot_bgcolor="rgba(0,0,0,0)")
        st.plotly_chart(fig_qs_dist, use_container_width=True)

with ov_c3:
    # Session duration distribution
    if not convs_all_pd.empty:
        fig_dur_dist = px.histogram(
            convs_all_pd, x="session_duration_sec", nbins=20,
            title="⏱️ Phân bố thời gian ở lại (giây)",
            color_discrete_sequence=["#3498DB"],
            labels={"session_duration_sec": "Giây", "count": "Số phiên"},
            height=280,
        )
        fig_dur_dist.update_layout(plot_bgcolor="rgba(0,0,0,0)")
        st.plotly_chart(fig_dur_dist, use_container_width=True)

# ── Top active users table ──────────────────────────────────────────────────
ov_left, ov_right = st.columns([3, 2])

with ov_left:
    if not chat_all_pd.empty and "user_id" in chat_all_pd.columns:
        top_active = (
            chat_all_pd[chat_all_pd["user_id"].notna()]
            .groupby("user_id")
            .agg(
                sessions        =("session_id", "count"),
                total_questions =("total_messages_exchanged", "sum"),
                avg_latency_ms  =("average_latency_ms", "mean"),
                avg_rating      =("feedback_rating", "mean"),
            )
            .sort_values("total_questions", ascending=False)
            .head(10)
            .reset_index()
        )
        # Join user names
        users_pd = df_users.select(["user_id", "full_name", "gender", "age"]).to_pandas()
        top_active = top_active.merge(users_pd, on="user_id", how="left")
        top_active["avg_latency_ms"] = top_active["avg_latency_ms"].round(0).fillna(0).astype(int)
        top_active["avg_rating"]     = top_active["avg_rating"].round(1)
        display_cols = ["full_name", "sessions", "total_questions", "avg_latency_ms", "avg_rating"]
        display_labels = {
            "full_name":       "Người dùng",
            "sessions":        "Phiên",
            "total_questions": "Câu hỏi",
            "avg_latency_ms":  "Độ trễ TB (ms)",
            "avg_rating":      "Đánh giá TB",
        }
        st.markdown("**🏆 Top 10 người dùng hoạt động nhiều nhất**")
        st.dataframe(
            top_active[display_cols].rename(columns=display_labels),
            use_container_width=True, hide_index=True, height=300,
        )

with ov_right:
    # Top symptoms/diseases across ALL users
    if not gold_med_all.is_empty():
        all_symptoms = _parse_json_list_column(gold_med_all["symptoms_list"])
        all_diseases = _parse_json_list_column(gold_med_all["diseases_list"])
        all_topics   = all_symptoms + all_diseases
        if all_topics:
            topic_counts = pd.Series(all_topics).value_counts().head(10).reset_index()
            topic_counts.columns = ["Chủ đề", "Số lần"]
            fig_top_topics = px.bar(
                topic_counts, x="Số lần", y="Chủ đề", orientation="h",
                title="🩺 Top chủ đề bệnh/triệu chứng toàn hệ thống",
                color="Số lần", color_continuous_scale="Reds",
                height=300,
            )
            fig_top_topics.update_layout(yaxis=dict(autorange="reversed"),
                                         plot_bgcolor="rgba(0,0,0,0)",
                                         coloraxis_showscale=False)
            st.plotly_chart(fig_top_topics, use_container_width=True)

st.markdown("---")

# ═══════════════════════════════════════════════════════════════════════════════
# 3-column layout: user list | sessions | chat history
# ═══════════════════════════════════════════════════════════════════════════════
col_users, col_sessions, col_chat = st.columns([1.2, 1.2, 2.6])
selected_user_id    = None
selected_session_id = None
user_convs          = pl.DataFrame()

with col_users:
    st.subheader("🧑 Danh sách người dùng")
    search_name = st.text_input("🔍 Tìm theo tên", placeholder="Nhập tên...")

    filtered_users = df_users
    if search_name:
        filtered_users = df_users.filter(
            pl.col("full_name").str.to_lowercase().str.contains(search_name.lower(), literal=True)
        )

    if filtered_users.is_empty():
        st.info("Không tìm thấy người dùng.")
    else:
        user_options = {
            f"{row['full_name']} ({row['gender'].upper()}, {row['age']} tuổi)": row["user_id"]
            for row in filtered_users.iter_rows(named=True)
        }
        selected_user_label = st.selectbox("Chọn người dùng", list(user_options.keys()))
        selected_user_id    = user_options[selected_user_label]

        user_row = df_users.filter(pl.col("user_id") == selected_user_id).row(0, named=True)
        st.markdown(f"""
**Chi tiết tài khoản:**
- **User ID:** `{user_row['user_id']}`
- **Họ tên:** `{user_row['full_name']}`
- **Email:** `{user_row['email_hashed'][:20]}...`
- **Độ tuổi:** {user_row['age']} tuổi
- **Giới tính:** {user_row['gender'].upper()}
- **Đăng ký:** `{user_row['created_at'][:10]}`
- **Hoạt động cuối:** `{user_row['last_login_at'][:19]}`
""")

with col_sessions:
    st.subheader("💬 Danh sách phiên chat")
    if selected_user_id:
        try:
            df_convs   = load_parquet("yhct-silver", "silver/mongodb/silver_mongodb_conversations.parquet")
            user_convs = df_convs.filter(pl.col("user_id") == selected_user_id)
        except Exception as e:
            st.error(f"Lỗi đọc dữ liệu hội thoại: {e}")
            st.stop()

        if user_convs.is_empty():
            st.info("Người dùng này chưa có cuộc hội thoại nào.")
        else:
            conv_options = {
                f"🕒 {row['start_time'][11:19]} ({row['total_messages']} tin nhắn)": row["session_id"]
                for row in user_convs.sort("start_time", descending=True).iter_rows(named=True)
            }
            selected_conv_label = st.selectbox("Chọn phiên trò chuyện", list(conv_options.keys()))
            selected_session_id = conv_options[selected_conv_label]

            conv_row   = user_convs.filter(pl.col("session_id") == selected_session_id).row(0, named=True)
            rating     = conv_row["feedback_rating"]
            rating_str = "⭐" * int(rating) if rating and rating > 0 else "Chưa đánh giá"
            st.markdown(f"""
**Tổng quan phiên:**
- **Session ID:** `{conv_row['session_id']}`
- **Bắt đầu:** `{conv_row['start_time'][:19]}`
- **Thời gian chat:** {conv_row['session_duration_sec']:.1f}s
- **Đánh giá:** {rating_str}
""")
    else:
        st.info("Vui lòng chọn một người dùng.")

with col_chat:
    st.subheader("🕵 Lịch sử hội thoại chi tiết")
    if selected_session_id and selected_user_id and not user_convs.is_empty():
        conv_row     = user_convs.filter(pl.col("session_id") == selected_session_id).row(0, named=True)
        messages_str = conv_row.get("messages_json", "[]")
        try:
            messages = json.loads(messages_str)
        except Exception as e:
            messages = []
            st.error(f"Không thể giải mã tin nhắn: {e}")

        if not messages:
            st.info("Không có tin nhắn nào trong phiên này.")
        else:
            st.markdown('<div class="chat-container">', unsafe_allow_html=True)
            for msg in messages:
                user_msg = msg.get("message_content", "")
                st.markdown(f'<div class="user-msg">{user_msg}</div>', unsafe_allow_html=True)

                ai_res  = msg.get("ai_response", "")
                sources = msg.get("sources", [])
                sims    = msg.get("sims", [])

                sources_html = ""
                if sources and len(sources) == len(sims):
                    sources_html += '<div style="margin-top: 8px;">'
                    for src, sim in zip(sources, sims):
                        pdf_url       = f"{MINIO_PUBLIC_URL}/yhct-docs/{src}"
                        sources_html += (
                            f'<a href="{pdf_url}" target="_blank" class="source-badge">'
                            f'📚 {src} · ({sim*100:.1f}%)</a>'
                        )
                    sources_html += "</div>"

                elapsed     = msg.get("elapsed_ms", 0)
                is_zero     = msg.get("is_zero", False)
                fallback_txt = (
                    '<span style="color:#d93025; font-weight:bold; margin-left:10px;">'
                    "⚠️ RAG Fallback</span>"
                    if is_zero else ""
                )
                metadata_html = f'<div class="metadata-line">⏱ {elapsed}ms {fallback_txt}</div>'
                st.markdown(
                    f'<div class="ai-msg">{ai_res}{sources_html}{metadata_html}</div>',
                    unsafe_allow_html=True,
                )
            st.markdown("</div>", unsafe_allow_html=True)
    else:
        st.info("Vui lòng chọn một phiên hội thoại để duyệt lịch sử chat.")

# ═══════════════════════════════════════════════════════════════════════════════
# Per-user analytics panel — shown when a user is selected
# ═══════════════════════════════════════════════════════════════════════════════
if selected_user_id:
    st.markdown("---")
    st.subheader(f"📊 Analytics chi tiết — {selected_user_label}")
    st.caption("Dữ liệu tổng hợp từ Gold layer (ETL pipeline).")

    # Load Gold tables for this user
    try:
        gold_chat_df   = load_parquet("yhct-gold", "gold/mongodb/gold_chat_performance.parquet")
        user_chat      = gold_chat_df.filter(pl.col("user_id") == selected_user_id)
    except Exception:
        user_chat      = pl.DataFrame()

    try:
        gold_med_df    = load_parquet("yhct-gold", "gold/mongodb/gold_medical_insights.parquet")
        user_medical   = gold_med_df.filter(pl.col("user_id") == selected_user_id)
    except Exception:
        user_medical   = pl.DataFrame()

    # ── KPI metrics ────────────────────────────────────────────────────────────
    chat_pd          = user_chat.to_pandas() if not user_chat.is_empty() else pd.DataFrame()
    total_sessions   = len(chat_pd)
    total_questions  = int(chat_pd["total_messages_exchanged"].sum()) if not chat_pd.empty else 0
    avg_duration_sec = (
        float(user_convs["session_duration_sec"].mean())
        if not user_convs.is_empty() else 0.0
    )
    avg_latency      = float(chat_pd["average_latency_ms"].mean()) if not chat_pd.empty else 0.0
    avg_rating       = float(chat_pd["feedback_rating"].dropna().mean()) if not chat_pd.empty else 0.0

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Tổng phiên chat",      f"{total_sessions:,}")
    c2.metric("Tổng câu hỏi",         f"{total_questions:,}")
    c3.metric("TB thời gian/phiên",   f"{avg_duration_sec:.1f}s")
    c4.metric("Độ trễ TB",            f"{avg_latency:.0f}ms" if avg_latency else "N/A")
    c5.metric("Đánh giá TB",          f"{avg_rating:.1f} ⭐" if avg_rating else "Chưa có")

    # ── Charts ─────────────────────────────────────────────────────────────────
    col_left, col_right = st.columns(2)

    with col_left:
        # Sessions + questions over time
        if not user_convs.is_empty():
            conv_pd      = user_convs.to_pandas()
            conv_pd["date"] = pd.to_datetime(conv_pd["start_time"], errors="coerce").dt.strftime("%Y-%m-%d")
            by_day       = (
                conv_pd.groupby("date")
                .agg(sessions=("session_id", "count"), total_msgs=("total_messages", "sum"))
                .reset_index()
            )
            fig_timeline = go.Figure()
            fig_timeline.add_trace(go.Bar(
                x=by_day["date"], y=by_day["sessions"],
                name="Phiên chat", marker_color="#2E86C1",
            ))
            fig_timeline.add_trace(go.Scatter(
                x=by_day["date"], y=by_day["total_msgs"],
                name="Câu hỏi", mode="lines+markers",
                line=dict(color="#E74C3C", width=2), yaxis="y2",
            ))
            fig_timeline.update_layout(
                title="📅 Hoạt động theo ngày",
                yaxis=dict(title="Số phiên"),
                yaxis2=dict(title="Số câu hỏi", overlaying="y", side="right"),
                plot_bgcolor="rgba(0,0,0,0)",
                legend=dict(x=0, y=1.1, orientation="h"),
                xaxis_title="",
                height=300,
            )
            st.plotly_chart(fig_timeline, use_container_width=True)

        # Session duration distribution
        if not user_convs.is_empty() and user_convs.shape[0] > 1:
            dur_pd = user_convs.select("session_duration_sec").to_pandas()
            fig_dur = px.histogram(
                dur_pd, x="session_duration_sec", nbins=15,
                title="⏱️ Phân bố thời gian mỗi phiên (giây)",
                color_discrete_sequence=["#27AE60"],
                labels={"session_duration_sec": "Giây", "count": "Số phiên"},
                height=260,
            )
            fig_dur.update_layout(plot_bgcolor="rgba(0,0,0,0)")
            st.plotly_chart(fig_dur, use_container_width=True)

    with col_right:
        # Messages per session distribution
        if not chat_pd.empty and len(chat_pd) > 1:
            fig_msgs = px.histogram(
                chat_pd, x="total_messages_exchanged", nbins=15,
                title="❓ Số câu hỏi/phiên chat",
                color_discrete_sequence=["#E67E22"],
                labels={"total_messages_exchanged": "Số câu hỏi", "count": "Số phiên"},
                height=260,
            )
            fig_msgs.update_layout(plot_bgcolor="rgba(0,0,0,0)")
            st.plotly_chart(fig_msgs, use_container_width=True)

        # Top topics from medical logs
        if not user_medical.is_empty():
            symptoms  = _parse_json_list_column(user_medical["symptoms_list"])
            diseases  = _parse_json_list_column(user_medical["diseases_list"])
            herbs_q   = _parse_json_list_column(user_medical["herbs_list"])
            all_topics = symptoms + diseases + herbs_q

            if all_topics:
                topic_counts = (
                    pd.Series(all_topics).value_counts().head(10).reset_index()
                )
                topic_counts.columns = ["Chủ đề", "Số lần"]
                fig_topics = px.bar(
                    topic_counts, x="Số lần", y="Chủ đề", orientation="h",
                    title="🩺 Chủ đề người dùng hỏi nhiều nhất",
                    color="Số lần", color_continuous_scale="Greens",
                    height=300,
                )
                fig_topics.update_layout(yaxis=dict(autorange="reversed"),
                                         plot_bgcolor="rgba(0,0,0,0)")
                st.plotly_chart(fig_topics, use_container_width=True)
            else:
                st.info("Chưa có dữ liệu chủ đề cho người dùng này.")
        else:
            st.info("Chưa có dữ liệu y tế cho người dùng này.")

    # ── Raw session table ───────────────────────────────────────────────────
    with st.expander("🔍 Toàn bộ phiên chat của người dùng"):
        if not user_convs.is_empty():
            display_cols = [c for c in ["session_id","start_time","total_messages",
                                        "session_duration_sec","feedback_rating"]
                            if c in user_convs.columns]
            st.dataframe(user_convs.select(display_cols).to_pandas(),
                         use_container_width=True, hide_index=True)
        else:
            st.info("Không có dữ liệu.")

    st.page_link("pages/3_📊_Phân_tích_dữ_liệu.py",
                 label="→ Xem dashboard analytics toàn hệ thống", icon="📊")
