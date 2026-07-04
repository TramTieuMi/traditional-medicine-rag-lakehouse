import json
import os
import time
from io import BytesIO

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import polars as pl
import requests
import streamlit as st
from minio import Minio

st.set_page_config(
    page_title="Quản lý Người dùng — YHCT",
    page_icon="👥",
    layout="wide",
    initial_sidebar_state="collapsed",
)

import sys as _sys, os as _os
_sys.path.insert(0, _os.path.dirname(_os.path.dirname(__file__)))
from utils import COLORS, fc  # registers Plotly template as side-effect

MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT",   "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY",  "minio")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY",  "minio123")
MINIO_PUBLIC_URL = os.getenv("MINIO_PUBLIC_URL", "http://localhost:9000")
DAGSTER_URL      = os.getenv("DAGSTER_URL", "http://dagster:3001").rstrip("/") + "/graphql"


# ── ETL pipeline helpers ──────────────────────────────────────────────────────
def _trigger_pipeline() -> str | None:
    """Launch user_lakehouse_job on Dagster; return run_id or None on error."""
    mutation = """
    mutation {
      launchPipelineExecution(executionParams: {
        selector: {
          repositoryLocationName: "etl_pipeline",
          repositoryName: "__repository__",
          jobName: "user_lakehouse_job"
        },
        mode: "default",
        runConfigData: {}
      }) {
        __typename
        ... on LaunchRunSuccess { run { runId } }
        ... on PythonError { message }
      }
    }
    """
    try:
        resp = requests.post(DAGSTER_URL, json={"query": mutation}, timeout=10)
        result = resp.json()["data"]["launchPipelineExecution"]
        if result["__typename"] == "LaunchRunSuccess":
            return result["run"]["runId"]
    except Exception:
        pass
    return None


def _pipeline_status(run_id: str) -> str:
    """Return 'SUCCESS', 'FAILURE', 'CANCELED', or 'RUNNING'."""
    query = f'{{ runOrError(runId: "{run_id}") {{ __typename ... on Run {{ status }} }} }}'
    try:
        resp = requests.post(DAGSTER_URL, json={"query": query}, timeout=5)
        status = resp.json()["data"]["runOrError"].get("status", "RUNNING")
        if status in ("SUCCESS", "FAILURE", "CANCELED"):
            return status
    except Exception:
        pass
    return "RUNNING"


# ── Data ──────────────────────────────────────────────────────────────────────
@st.cache_data(ttl=60)
def load_parquet(bucket: str, key: str) -> pl.DataFrame:
    from minio.error import S3Error
    client = Minio(MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, secure=False)
    try:
        obj = client.get_object(bucket, key)
        return pl.read_parquet(BytesIO(obj.read()))
    except S3Error as e:
        if e.code == "NoSuchKey":
            return pl.DataFrame()
        raise


def _parse_json_list(val: str) -> list:
    try:
        return json.loads(val) if val else []
    except Exception:
        return []


def _flatten_col(series: pl.Series) -> list[str]:
    out = []
    for v in series.drop_nulls().to_list():
        items = _parse_json_list(v)
        out.extend(str(i).strip() for i in items if i)
    return out


# ── CSS ───────────────────────────────────────────────────────────────────────
st.markdown("""
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Be+Vietnam+Pro:wght@300;400;500;600;700;800&family=Inter:wght@400;500;600&display=swap" rel="stylesheet">

<style>
/* ── Reset & base ─────────────────────────────────────────────── */
html, body, [class*="css"], .stApp {
    font-family: 'Be Vietnam Pro', 'Inter', sans-serif !important;
}

.stApp { background: #F5F8F6; }

.main .block-container {
    padding: 2rem 2.5rem 3rem;
    max-width: 1440px;
}

/* ── Page header ──────────────────────────────────────────────── */
.pg-header {
    display: flex;
    align-items: flex-end;
    justify-content: space-between;
    margin-bottom: 28px;
    padding-bottom: 20px;
    border-bottom: 1px solid #D4E6DA;
}
.pg-title {
    font-size: 1.75rem;
    font-weight: 800;
    color: #1B3A2D;
    letter-spacing: -0.03em;
    margin: 0;
    line-height: 1;
}
.pg-sub {
    font-size: 0.82rem;
    color: #7A9185;
    margin: 5px 0 0;
    font-weight: 400;
}
.nav-links { display: flex; gap: 10px; }
.nav-link {
    font-size: 0.78rem;
    font-weight: 600;
    color: #2D8A57;
    background: #E8F5EE;
    padding: 6px 14px;
    border-radius: 20px;
    text-decoration: none;
    border: 1px solid #B8DECA;
    cursor: pointer;
    transition: all 0.15s;
}

/* ── KPI cards ────────────────────────────────────────────────── */
.kpi-grid {
    display: grid;
    grid-template-columns: repeat(6, 1fr);
    gap: 12px;
    margin-bottom: 28px;
}
.kpi-card {
    background: white;
    border-radius: 12px;
    padding: 18px 20px;
    border: 1px solid #D4E6DA;
    border-top: 3px solid #2D8A57;
    box-shadow: 0 1px 4px rgba(0,0,0,0.04);
}
.kpi-card.amber { border-top-color: #C17F3A; }
.kpi-val {
    font-size: 1.8rem;
    font-weight: 800;
    color: #1B3A2D;
    line-height: 1;
    letter-spacing: -0.03em;
}
.kpi-card.amber .kpi-val { color: #8B5A1F; }
.kpi-lbl {
    font-size: 0.7rem;
    font-weight: 600;
    color: #7A9185;
    text-transform: uppercase;
    letter-spacing: 0.06em;
    margin-top: 6px;
}

/* ── Tab styling ──────────────────────────────────────────────── */
.stTabs [data-baseweb="tab-list"] {
    gap: 4px;
    background: white;
    border-radius: 12px;
    padding: 5px;
    border: 1px solid #D4E6DA;
    box-shadow: 0 1px 4px rgba(0,0,0,0.04);
    margin-bottom: 20px;
}
.stTabs [data-baseweb="tab"] {
    border-radius: 8px;
    padding: 9px 22px;
    font-size: 0.85rem;
    font-weight: 600;
    color: #7A9185;
    border: none !important;
    background: transparent !important;
    transition: all 0.15s;
    font-family: 'Be Vietnam Pro', sans-serif !important;
}
.stTabs [aria-selected="true"] {
    background: #1B3A2D !important;
    color: white !important;
}
.stTabs [data-baseweb="tab-border"] { display: none !important; }
.stTabs [data-baseweb="tab-highlight"] { display: none !important; }

/* ── Section labels ───────────────────────────────────────────── */
.sec-label {
    font-size: 0.65rem;
    font-weight: 700;
    color: #9AB0A5;
    text-transform: uppercase;
    letter-spacing: 0.1em;
    margin-bottom: 10px;
    padding-bottom: 8px;
    border-bottom: 1px solid #E4EFE8;
}

/* ── Profile card ─────────────────────────────────────────────── */
.profile-card {
    background: linear-gradient(135deg, #1B3A2D 0%, #2D5A42 100%);
    border-radius: 14px;
    padding: 22px;
    color: white;
    margin-bottom: 14px;
}
.profile-avatar {
    width: 46px; height: 46px;
    border-radius: 50%;
    background: rgba(255,255,255,0.15);
    display: flex; align-items: center; justify-content: center;
    font-size: 1.1rem; font-weight: 800;
    margin-bottom: 12px;
    border: 2px solid rgba(255,255,255,0.2);
}
.profile-name { font-size: 1.05rem; font-weight: 700; margin-bottom: 2px; }
.profile-meta { font-size: 0.78rem; opacity: 0.65; margin-bottom: 16px; }
.profile-stats {
    display: grid; grid-template-columns: 1fr 1fr 1fr;
    gap: 8px;
}
.pstat {
    background: rgba(255,255,255,0.08);
    border-radius: 8px; padding: 10px 8px; text-align: center;
    border: 1px solid rgba(255,255,255,0.1);
}
.pstat-val { font-size: 1.25rem; font-weight: 800; line-height: 1; }
.pstat-lbl { font-size: 0.62rem; opacity: 0.6; margin-top: 3px; text-transform: uppercase; letter-spacing: 0.05em; }

/* ── User table ───────────────────────────────────────────────── */
.user-row {
    background: white;
    border-radius: 10px;
    padding: 12px 16px;
    border: 1px solid #D4E6DA;
    margin-bottom: 7px;
    display: flex;
    align-items: center;
    gap: 12px;
    cursor: pointer;
    transition: all 0.12s;
}
.user-row:hover { border-color: #2D8A57; box-shadow: 0 2px 10px rgba(45,138,87,0.1); }
.user-row.selected { border-color: #1B3A2D; background: #F0F8F3; }
.u-avatar {
    width: 36px; height: 36px; border-radius: 50%;
    background: #1B3A2D; color: white;
    display: flex; align-items: center; justify-content: center;
    font-weight: 700; font-size: 13px; flex-shrink: 0;
}
.u-name { font-size: 0.88rem; font-weight: 600; color: #1A1F1C; }
.u-meta { font-size: 0.75rem; color: #7A9185; margin-top: 1px; }
.u-badge {
    margin-left: auto;
    font-size: 0.7rem; font-weight: 600;
    padding: 3px 9px; border-radius: 20px;
    background: #E8F5EE; color: #1B3A2D;
    border: 1px solid #B8DECA; flex-shrink: 0;
}
.u-badge.amber { background: #FDF4E8; color: #8B5A1F; border-color: #E8C98A; }

/* ── Session item ─────────────────────────────────────────────── */
.sess-item {
    background: white;
    border-radius: 10px;
    padding: 11px 14px;
    border: 1px solid #D4E6DA;
    margin-bottom: 7px;
    cursor: pointer;
    transition: all 0.12s;
}
.sess-item:hover { border-color: #2D8A57; }
.sess-item.selected { border-color: #1B3A2D; background: #F0F8F3; }
.sess-time { font-size: 0.82rem; font-weight: 600; color: #1A1F1C; }
.sess-stats { font-size: 0.72rem; color: #7A9185; margin-top: 3px; }
.sess-rating { font-size: 0.7rem; margin-top: 4px; }

/* ── Conversation log ─────────────────────────────────────────── */
.log-wrap {
    background: white;
    border-radius: 14px;
    border: 1px solid #D4E6DA;
    overflow: hidden;
}
.log-header {
    background: #1B3A2D;
    padding: 14px 20px;
    color: white;
    display: flex;
    align-items: center;
    gap: 16px;
    font-size: 0.8rem;
}
.log-header-title { font-weight: 700; font-size: 0.9rem; }
.log-header-meta { opacity: 0.65; font-size: 0.75rem; }
.log-entry {
    border-bottom: 1px solid #E8F0EB;
    overflow: hidden;
}
.log-entry:last-child { border-bottom: none; }
.log-q {
    padding: 14px 20px 10px;
    background: #F7FAF8;
    border-bottom: 1px solid #EDF4F0;
}
.log-q-label {
    font-size: 0.65rem; font-weight: 700;
    color: #2D8A57; text-transform: uppercase;
    letter-spacing: 0.08em; margin-bottom: 5px;
}
.log-q-text { font-size: 0.9rem; font-weight: 600; color: #1A1F1C; line-height: 1.5; }
.log-a {
    padding: 12px 20px;
    background: white;
}
.log-a-label {
    font-size: 0.65rem; font-weight: 700;
    color: #7A9185; text-transform: uppercase;
    letter-spacing: 0.08em; margin-bottom: 5px;
}
.log-a-text { font-size: 0.85rem; color: #2D3830; line-height: 1.7; }
.log-footer {
    padding: 8px 20px;
    background: #FAFCFB;
    border-top: 1px solid #EDF4F0;
    display: flex; gap: 16px; align-items: center;
    flex-wrap: wrap;
}
.log-tag {
    font-size: 0.68rem; font-weight: 500;
    padding: 2px 8px; border-radius: 4px;
    background: #E8F5EE; color: #1B3A2D;
}
.log-tag.warn { background: #FDF4E8; color: #8B5A1F; }
.log-tag.time { background: #F0F4FF; color: #3A5A99; }
.log-tag.zero { background: #FEE; color: #C0392B; }
.log-src {
    font-size: 0.68rem; color: #2D8A57; font-weight: 500;
    text-decoration: none; margin-left: auto;
}

/* ── Info box ─────────────────────────────────────────────────── */
.info-box {
    background: white;
    border-radius: 12px;
    padding: 28px;
    border: 1px solid #D4E6DA;
    text-align: center;
    color: #7A9185;
    font-size: 0.88rem;
}
.info-box-icon { font-size: 2rem; margin-bottom: 10px; }
.info-box-title { font-weight: 700; color: #1B3A2D; margin-bottom: 5px; }

/* ── Metric override ──────────────────────────────────────────── */
[data-testid="stMetric"] {
    background: white;
    border-radius: 12px;
    padding: 16px 18px;
    border: 1px solid #D4E6DA;
    box-shadow: 0 1px 4px rgba(0,0,0,0.04);
}
[data-testid="stMetricValue"] {
    font-family: 'Be Vietnam Pro', sans-serif !important;
    font-weight: 800 !important;
    color: #1B3A2D !important;
}
[data-testid="stMetricLabel"] { color: #7A9185 !important; }

/* ── Plotly dark ──────────────────────────────────────────────── */
.js-plotly-plot .plotly .main-svg { border-radius: 10px; }

/* ── Refresh bar ──────────────────────────────────────────────── */
.refresh-bar {
    display: flex; align-items: center; gap: 16px;
    background: #FFFFFF; border: 1px solid #D4E6DA;
    border-radius: 12px; padding: 13px 20px;
    margin-bottom: 16px;
    box-shadow: 0 1px 4px rgba(0,0,0,0.05);
}
.refresh-note { font-size: 0.78rem; color: #5A7A6A; line-height: 1.5; }
.refresh-note strong { color: #1B3A2D; }

/* ── Input fields ─────────────────────────────────────────────── */
[data-baseweb="input"] {
    border-radius: 8px !important;
    border-color: #D4E6DA !important;
    background: white !important;
}
[data-baseweb="select"] { border-radius: 8px !important; }

/* ── Scrollable container ─────────────────────────────────────── */
.scroll-list {
    max-height: 460px;
    overflow-y: auto;
    padding-right: 4px;
}
.scroll-list::-webkit-scrollbar { width: 4px; }
.scroll-list::-webkit-scrollbar-track { background: transparent; }
.scroll-list::-webkit-scrollbar-thumb { background: #B8DECA; border-radius: 2px; }

.log-scroll {
    max-height: 600px;
    overflow-y: auto;
    padding-right: 2px;
}
</style>
""", unsafe_allow_html=True)


# ── Load data ─────────────────────────────────────────────────────────────────
df_users     = load_parquet("yhct-silver", "silver/mongodb/silver_mongodb_users.parquet")
df_convs_all = load_parquet("yhct-silver", "silver/mongodb/silver_mongodb_conversations.parquet")
gold_chat    = load_parquet("yhct-gold",   "gold/mongodb/gold_chat_performance.parquet")
gold_med     = load_parquet("yhct-gold",   "gold/mongodb/gold_medical_insights.parquet")

# ── Computed globals ──────────────────────────────────────────────────────────
total_users   = df_users.shape[0]
total_sess    = df_convs_all.shape[0]
total_qs      = int(df_convs_all["total_messages"].sum()) if not df_convs_all.is_empty() and "total_messages" in df_convs_all.columns else 0
avg_dur       = float(df_convs_all["session_duration_sec"].mean()) if not df_convs_all.is_empty() else 0.0
avg_msgs      = float(gold_chat["total_messages_exchanged"].mean()) if not gold_chat.is_empty() else 0.0
avg_rating    = (lambda v: float(v) if v is not None else 0.0)(gold_chat["feedback_rating"].drop_nulls().mean() if not gold_chat.is_empty() else None)

# ── Session state ─────────────────────────────────────────────────────────────
if "sel_uid" not in st.session_state:
    st.session_state.sel_uid = None
if "sel_sid" not in st.session_state:
    st.session_state.sel_sid = None

# ── Page header ───────────────────────────────────────────────────────────────
st.markdown("""
<div class="pg-header">
  <div>
    <p class="pg-title">Quản lý Người dùng</p>
    <p class="pg-sub">Hệ thống Y học Cổ truyền Việt Nam · Dữ liệu theo thời gian thực từ Lakehouse</p>
  </div>
</div>
""", unsafe_allow_html=True)

# ── KPI strip ─────────────────────────────────────────────────────────────────
st.markdown(f"""
<div class="kpi-grid">
  <div class="kpi-card">
    <div class="kpi-val">{total_users:,}</div>
    <div class="kpi-lbl">Người dùng</div>
  </div>
  <div class="kpi-card">
    <div class="kpi-val">{total_sess:,}</div>
    <div class="kpi-lbl">Phiên chat</div>
  </div>
  <div class="kpi-card">
    <div class="kpi-val">{total_qs:,}</div>
    <div class="kpi-lbl">Tổng câu hỏi</div>
  </div>
  <div class="kpi-card">
    <div class="kpi-val">{avg_msgs:.1f}</div>
    <div class="kpi-lbl">TB câu/phiên</div>
  </div>
  <div class="kpi-card">
    <div class="kpi-val">{avg_dur:.0f}s</div>
    <div class="kpi-lbl">TB thời gian/phiên</div>
  </div>
  <div class="kpi-card amber">
    <div class="kpi-val">{"%.1f ★" % avg_rating if avg_rating else "—"}</div>
    <div class="kpi-lbl">Đánh giá TB</div>
  </div>
</div>
""", unsafe_allow_html=True)

# ── Refresh bar ───────────────────────────────────────────────────────────────
_rf_col, _note_col = st.columns([1, 4])
with _rf_col:
    _do_refresh = st.button(
        "🔄  Làm mới dữ liệu",
        use_container_width=True,
        type="primary",
        help="Chạy ETL pipeline để đồng bộ dữ liệu mới nhất từ MongoDB vào Lakehouse",
    )
with _note_col:
    st.markdown(
        '<div class="refresh-bar">'
        '<div class="refresh-note">'
        '<strong>Dữ liệu tự động cập nhật mỗi 15 phút.</strong><br>'
        'Bấm "Làm mới dữ liệu" để đồng bộ ngay lập tức — '
        '⏳ <strong>vui lòng chờ khoảng 30–60 giây</strong> để pipeline hoàn tất, trang sẽ tự tải lại.'
        '</div></div>',
        unsafe_allow_html=True,
    )

if _do_refresh:
    _status_slot = st.empty()
    _bar_slot    = st.empty()

    _status_slot.info("Đang khởi động pipeline ETL...")
    run_id = _trigger_pipeline()

    if run_id is None:
        _status_slot.error("Không kết nối được tới Dagster. Kiểm tra service dagster đang chạy.")
    else:
        _steps = [
            "⚙️  Đang đọc dữ liệu từ MongoDB (Bronze)...",
            "🔄  Đang làm sạch & ẩn danh dữ liệu (Silver)...",
            "📊  Đang tổng hợp metrics & insights (Gold)...",
            "✅  Hoàn tất! Đang tải lại trang...",
        ]
        _progress = _bar_slot.progress(0, text="Khởi động pipeline...")
        _elapsed  = 0
        _step_idx = 0

        while True:
            _pipeline_stat = _pipeline_status(run_id)
            if _pipeline_stat != "RUNNING":
                break

            # Advance fake progress (moves through steps every ~12s)
            _pct = min(90, _elapsed * 2)
            _step_idx = min(len(_steps) - 2, _elapsed // 12)
            _progress.progress(_pct, text=_steps[_step_idx])
            time.sleep(3)
            _elapsed += 3

        if _pipeline_stat == "SUCCESS":
            _progress.progress(100, text=_steps[-1])
            _status_slot.success(
                f"Pipeline hoàn tất sau ~{_elapsed}s. "
                "Dữ liệu đã được cập nhật từ MongoDB vào Lakehouse."
            )
            time.sleep(1)
            st.cache_data.clear()
            st.rerun()
        else:
            _progress.empty()
            _status_slot.error(
                f"Pipeline kết thúc với trạng thái: **{_pipeline_stat}**. "
                "Kiểm tra Dagster UI tại http://localhost:3001 để xem chi tiết lỗi."
            )

# Navigation
nav_col1, nav_col2, _ = st.columns([1, 1, 5])
with nav_col1:
    st.page_link("pages/3_📊_Phân_tích_dữ_liệu.py", label="📊 Analytics tổng thể")
with nav_col2:
    st.page_link("pages/4_🩺_Log_Y_Tế.py", label="🩺 Log Y Tế")

st.markdown("<div style='margin-bottom:4px'></div>", unsafe_allow_html=True)

# ═══════════════════════════════════════════════════════════════════════════════
# TABS
# ═══════════════════════════════════════════════════════════════════════════════
tab_overview, tab_users, tab_log = st.tabs([
    "📊  Tổng quan",
    "👤  Danh sách người dùng",
    "💬  Nhật ký hội thoại",
])


# ╔═══════════════════════════════════════════════════════════════════════════╗
# ║  TAB 1 — TỔNG QUAN                                                       ║
# ╚═══════════════════════════════════════════════════════════════════════════╝
with tab_overview:
    if df_users.is_empty():
        st.markdown('<div class="info-box"><div class="info-box-icon">🗄️</div><div class="info-box-title">Chưa có dữ liệu</div>Hãy chạy ETL pipeline để nạp dữ liệu người dùng.</div>', unsafe_allow_html=True)
    else:
        ov_c1, ov_c2, ov_c3 = st.columns(3)

        # Đăng ký theo ngày
        with ov_c1:
            if "created_at" in df_users.columns:
                reg_pd = df_users.select("created_at").to_pandas()
                reg_pd["date"] = pd.to_datetime(reg_pd["created_at"], errors="coerce").dt.strftime("%Y-%m-%d")
                reg_day = reg_pd.groupby("date").size().reset_index(name="count")
                fig = px.bar(
                    reg_day, x="date", y="count",
                    title="Người dùng đăng ký theo ngày",
                    color_discrete_sequence=[COLORS[0]],
                )
                fig.update_layout(xaxis_title="", yaxis_title="")
                st.plotly_chart(fc(fig, 240), use_container_width=True)

        # Phân bố câu hỏi/phiên
        with ov_c2:
            if not gold_chat.is_empty():
                chat_pd = gold_chat.to_pandas()
                fig2 = px.histogram(
                    chat_pd, x="total_messages_exchanged", nbins=20,
                    title="Số câu hỏi / phiên chat",
                    color_discrete_sequence=[COLORS[0]],
                    labels={"total_messages_exchanged": "Số câu hỏi"},
                )
                fig2.update_layout(yaxis_title="Số phiên", bargap=0.08)
                st.plotly_chart(fc(fig2, 240), use_container_width=True)

        # Thời gian ở lại
        with ov_c3:
            if not df_convs_all.is_empty():
                dur_pd = df_convs_all.select("session_duration_sec").to_pandas()
                fig3 = px.histogram(
                    dur_pd, x="session_duration_sec", nbins=20,
                    title="Thời gian ở lại (giây)",
                    color_discrete_sequence=[COLORS[1]],
                    labels={"session_duration_sec": "Giây"},
                )
                fig3.update_layout(yaxis_title="Số phiên", bargap=0.08)
                st.plotly_chart(fc(fig3, 240), use_container_width=True)

        # Top người dùng + top chủ đề
        ov_l, ov_r = st.columns([3, 2])

        with ov_l:
            st.markdown('<div class="sec-label">Top người dùng hoạt động nhất</div>', unsafe_allow_html=True)
            if not gold_chat.is_empty() and "user_id" in gold_chat.columns:
                chat_pd2   = gold_chat.to_pandas()
                users_pd   = df_users.select(["user_id", "full_name", "gender", "age"]).to_pandas()
                top_active = (
                    chat_pd2[chat_pd2["user_id"].notna()]
                    .groupby("user_id")
                    .agg(
                        phien       = ("session_id", "count"),
                        cau_hoi     = ("total_messages_exchanged", "sum"),
                        danh_gia    = ("feedback_rating", "mean"),
                    )
                    .sort_values("cau_hoi", ascending=False)
                    .head(10)
                    .reset_index()
                    .merge(users_pd, on="user_id", how="left")
                )
                top_active["danh_gia"] = top_active["danh_gia"].round(1)
                top_active["cau_hoi"]  = top_active["cau_hoi"].astype(int)
                st.dataframe(
                    top_active[["full_name", "phien", "cau_hoi", "danh_gia"]].rename(columns={
                        "full_name": "Người dùng",
                        "phien":     "Phiên",
                        "cau_hoi":  "Câu hỏi",
                        "danh_gia": "Đánh giá",
                    }),
                    use_container_width=True, hide_index=True, height=300,
                )

        with ov_r:
            st.markdown('<div class="sec-label">Chủ đề y tế phổ biến nhất</div>', unsafe_allow_html=True)
            if not gold_med.is_empty():
                all_sym = _flatten_col(gold_med["symptoms_list"])
                all_dis = _flatten_col(gold_med["diseases_list"])
                topics  = all_sym + all_dis
                if topics:
                    tc = pd.Series(topics).value_counts().head(10).reset_index()
                    tc.columns = ["Chủ đề", "Số lần"]
                    fig_t = px.bar(
                        tc, x="Số lần", y="Chủ đề", orientation="h",
                        color_discrete_sequence=[COLORS[0]],
                    )
                    fig_t.update_layout(yaxis=dict(autorange="reversed"), xaxis_title="")
                    st.plotly_chart(fc(fig_t, 300), use_container_width=True)
                else:
                    st.info("Chưa có dữ liệu chủ đề.")
            else:
                st.info("Chưa có dữ liệu y tế. Hãy chạy pipeline.")


# ╔═══════════════════════════════════════════════════════════════════════════╗
# ║  TAB 2 — DANH SÁCH NGƯỜI DÙNG                                            ║
# ╚═══════════════════════════════════════════════════════════════════════════╝
with tab_users:
    if df_users.is_empty():
        st.markdown('<div class="info-box"><div class="info-box-icon">👤</div><div class="info-box-title">Chưa có người dùng</div>Chưa có tài khoản nào được đồng bộ từ MongoDB.</div>', unsafe_allow_html=True)
    else:
        ul_left, ul_right = st.columns([2, 3])

        with ul_left:
            st.markdown('<div class="sec-label">Tìm kiếm & Chọn người dùng</div>', unsafe_allow_html=True)
            search = st.text_input("", placeholder="🔍  Tìm theo tên...", label_visibility="collapsed")

            filtered = df_users
            if search:
                filtered = df_users.filter(
                    pl.col("full_name").str.to_lowercase().str.contains(search.lower(), literal=True)
                )

            st.markdown('<div class="scroll-list">', unsafe_allow_html=True)
            for row in filtered.iter_rows(named=True):
                uid   = row["user_id"]
                name  = row.get("full_name", "—")
                gender = row.get("gender", "")
                age   = row.get("age", "")
                initial = name[0].upper() if name else "?"
                is_sel  = (uid == st.session_state.sel_uid)

                # Count sessions for this user
                n_sess = 0
                if not df_convs_all.is_empty() and "user_id" in df_convs_all.columns:
                    n_sess = df_convs_all.filter(pl.col("user_id") == uid).shape[0]

                sel_cls = "selected" if is_sel else ""
                badge   = f'<span class="u-badge">{n_sess} phiên</span>'

                html = f"""
                <div class="user-row {sel_cls}" onclick="">
                  <div class="u-avatar">{initial}</div>
                  <div>
                    <div class="u-name">{name}</div>
                    <div class="u-meta">{gender.upper()} · {age} tuổi</div>
                  </div>
                  {badge}
                </div>"""
                st.markdown(html, unsafe_allow_html=True)

                if st.button(f"Xem chi tiết", key=f"sel_{uid}", help=name,
                             use_container_width=True):
                    st.session_state.sel_uid = uid
                    st.session_state.sel_sid = None
                    st.rerun()
            st.markdown("</div>", unsafe_allow_html=True)

        with ul_right:
            if st.session_state.sel_uid:
                uid  = st.session_state.sel_uid
                urow = df_users.filter(pl.col("user_id") == uid).row(0, named=True)
                name = urow.get("full_name", "—")
                initial = name[0].upper() if name else "?"

                # Count stats
                u_convs = df_convs_all.filter(pl.col("user_id") == uid) if not df_convs_all.is_empty() else pl.DataFrame()
                n_sess  = u_convs.shape[0]
                n_qs    = int(u_convs["total_messages"].sum()) if not u_convs.is_empty() and "total_messages" in u_convs.columns else 0
                avg_d   = float(u_convs["session_duration_sec"].mean()) if not u_convs.is_empty() else 0.0

                st.markdown(f"""
                <div class="profile-card">
                  <div class="profile-avatar">{initial}</div>
                  <div class="profile-name">{name}</div>
                  <div class="profile-meta">
                    {urow.get('gender','').upper()} · {urow.get('age','')} tuổi ·
                    Đăng ký {urow.get('created_at','')[:10]}
                  </div>
                  <div class="profile-stats">
                    <div class="pstat">
                      <div class="pstat-val">{n_sess}</div>
                      <div class="pstat-lbl">Phiên</div>
                    </div>
                    <div class="pstat">
                      <div class="pstat-val">{n_qs}</div>
                      <div class="pstat-lbl">Câu hỏi</div>
                    </div>
                    <div class="pstat">
                      <div class="pstat-val">{avg_d:.0f}s</div>
                      <div class="pstat-lbl">TB/phiên</div>
                    </div>
                  </div>
                </div>
                """, unsafe_allow_html=True)

                # Per-user topic chart
                if not gold_med.is_empty() and "user_id" in gold_med.columns:
                    u_med = gold_med.filter(pl.col("user_id") == uid)
                    if not u_med.is_empty():
                        u_sym = _flatten_col(u_med["symptoms_list"])
                        u_dis = _flatten_col(u_med["diseases_list"])
                        u_top = u_sym + u_dis
                        if u_top:
                            tc = pd.Series(u_top).value_counts().head(8).reset_index()
                            tc.columns = ["Chủ đề", "Số lần"]
                            fig_u = px.bar(
                                tc, x="Số lần", y="Chủ đề", orientation="h",
                                title="Chủ đề người dùng hỏi nhiều nhất",
                                color_discrete_sequence=[COLORS[1]],
                            )
                            fig_u.update_layout(yaxis=dict(autorange="reversed"), xaxis_title="")
                            st.plotly_chart(fc(fig_u, 260), use_container_width=True)

                # Sessions table
                if not u_convs.is_empty():
                    st.markdown('<div class="sec-label" style="margin-top:8px">Lịch sử phiên chat</div>', unsafe_allow_html=True)
                    show_cols = [c for c in ["session_id", "start_time", "total_messages",
                                             "session_duration_sec", "feedback_rating"]
                                 if c in u_convs.columns]
                    disp = u_convs.select(show_cols).to_pandas().sort_values(
                        "start_time", ascending=False
                    ).rename(columns={
                        "session_id":           "Session",
                        "start_time":           "Bắt đầu",
                        "total_messages":       "Tin nhắn",
                        "session_duration_sec": "Thời gian (s)",
                        "feedback_rating":      "Đánh giá",
                    })
                    if "Session" in disp.columns:
                        disp["Session"] = disp["Session"].str[:12] + "..."
                    st.dataframe(disp, use_container_width=True, hide_index=True, height=220)

                # Quick link to conversation log
                if st.button("💬 Xem nhật ký hội thoại đầy đủ →",
                             use_container_width=True, type="primary"):
                    st.session_state.sel_uid = uid
                    st.rerun()
            else:
                st.markdown("""
                <div class="info-box">
                  <div class="info-box-icon">👈</div>
                  <div class="info-box-title">Chọn người dùng</div>
                  Chọn một người dùng ở cột bên trái để xem thông tin chi tiết.
                </div>""", unsafe_allow_html=True)


# ╔═══════════════════════════════════════════════════════════════════════════╗
# ║  TAB 3 — NHẬT KÝ HỘI THOẠI                                              ║
# ╚═══════════════════════════════════════════════════════════════════════════╝
with tab_log:
    log_left, log_right = st.columns([1, 2.8])

    with log_left:
        # User selector
        st.markdown('<div class="sec-label">Người dùng</div>', unsafe_allow_html=True)
        if not df_users.is_empty():
            user_map = {
                row["full_name"]: row["user_id"]
                for row in df_users.iter_rows(named=True)
            }
            sel_name = st.selectbox(
                "",
                options=["— Chọn người dùng —"] + list(user_map.keys()),
                label_visibility="collapsed",
                key="log_user_select",
            )
            if sel_name != "— Chọn người dùng —":
                log_uid = user_map[sel_name]
                st.session_state.sel_uid = log_uid
            else:
                log_uid = None
        else:
            log_uid = None
            st.info("Chưa có dữ liệu người dùng.")

        # Session selector
        if log_uid and not df_convs_all.is_empty():
            st.markdown('<div class="sec-label" style="margin-top:14px">Phiên hội thoại</div>', unsafe_allow_html=True)
            u_convs = df_convs_all.filter(pl.col("user_id") == log_uid)
            if u_convs.is_empty():
                st.info("Người dùng này chưa có phiên nào.")
                log_sid = None
            else:
                u_convs_sorted = u_convs.sort("start_time", descending=True)
                st.markdown('<div class="scroll-list">', unsafe_allow_html=True)
                for row in u_convs_sorted.iter_rows(named=True):
                    sid   = row["session_id"]
                    stime = row.get("start_time", "")[:16]
                    nmsg  = row.get("total_messages", 0)
                    dur   = row.get("session_duration_sec", 0)
                    rat   = row.get("feedback_rating", None)
                    rating_str = "⭐" * int(rat) if rat and rat > 0 else "Chưa đánh giá"
                    is_s  = (sid == st.session_state.sel_sid)
                    sel_c = "selected" if is_s else ""

                    st.markdown(f"""
                    <div class="sess-item {sel_c}">
                      <div class="sess-time">🕒 {stime}</div>
                      <div class="sess-stats">{nmsg} tin nhắn · {dur:.0f}s</div>
                      <div class="sess-rating">{rating_str}</div>
                    </div>""", unsafe_allow_html=True)

                    if st.button("Xem", key=f"sess_{sid}", use_container_width=True):
                        st.session_state.sel_sid = sid
                        st.rerun()
                st.markdown("</div>", unsafe_allow_html=True)
                log_sid = st.session_state.sel_sid
        else:
            log_sid = None

    with log_right:
        filtered_conv = df_convs_all.filter((pl.col("user_id") == log_uid) & (pl.col("session_id") == log_sid)) if log_uid and log_sid and not df_convs_all.is_empty() else pl.DataFrame()
        if not filtered_conv.is_empty():
            conv_row = filtered_conv.row(0, named=True)
            try:
                messages = json.loads(conv_row.get("messages_json", "[]"))
            except Exception:
                messages = []

            _udf  = df_users.filter(pl.col("user_id") == log_uid)
            urow  = _udf.row(0, named=True) if not _udf.is_empty() else {}
            uname = urow.get("full_name", "—")
            stime = conv_row.get("start_time", "")[:16]
            nmsg  = conv_row.get("total_messages", 0)
            dur   = conv_row.get("session_duration_sec", 0)
            rat   = conv_row.get("feedback_rating", None)
            rating_str = ("⭐" * int(rat)) if rat and rat > 0 else "Chưa đánh giá"

            # Log header
            st.markdown(f"""
            <div class="log-wrap">
              <div class="log-header">
                <div>
                  <div class="log-header-title">📋 Nhật ký hội thoại — {uname}</div>
                  <div class="log-header-meta">{stime} · {nmsg} tin nhắn · {dur:.0f}s · {rating_str}</div>
                </div>
              </div>
            """, unsafe_allow_html=True)

            if not messages:
                st.markdown('<div style="padding:24px;color:#7A9185;text-align:center">Không có nội dung trong phiên này.</div>', unsafe_allow_html=True)
            else:
                st.markdown('<div class="log-scroll">', unsafe_allow_html=True)
                for i, msg in enumerate(messages, 1):
                    q       = msg.get("message_content", "")
                    a       = msg.get("ai_response", "")
                    elapsed = msg.get("elapsed_ms", 0)
                    is_zero = msg.get("is_zero", False)
                    sources = msg.get("sources", [])
                    sims    = msg.get("sims", [])
                    ts      = msg.get("timestamp", "")
                    if ts:
                        ts = str(ts)[:16]

                    zero_tag  = '<span class="log-tag zero">⚠ RAG fallback</span>' if is_zero else ""
                    time_tag  = f'<span class="log-tag time">⏱ {elapsed}ms</span>' if elapsed else ""
                    idx_tag   = f'<span class="log-tag">#{i}</span>'

                    src_html = ""
                    if sources:
                        srcs = []
                        for s, sim in zip(sources, sims or [0]*len(sources)):
                            pdf_url = f"{MINIO_PUBLIC_URL}/yhct-docs/{s}"
                            srcs.append(f'<a href="{pdf_url}" target="_blank" class="log-src">📄 {s} ({sim*100:.0f}%)</a>')
                        src_html = " ".join(srcs)

                    # Escape HTML in content
                    q_safe = q.replace("<", "&lt;").replace(">", "&gt;")
                    a_safe = a.replace("<", "&lt;").replace(">", "&gt;")

                    st.markdown(f"""
                    <div class="log-entry">
                      <div class="log-q">
                        <div class="log-q-label">Người dùng hỏi</div>
                        <div class="log-q-text">{q_safe}</div>
                      </div>
                      <div class="log-a">
                        <div class="log-a-label">Trả lời YHCT AI</div>
                        <div class="log-a-text">{a_safe}</div>
                      </div>
                      <div class="log-footer">
                        {idx_tag}{time_tag}{zero_tag}
                        {src_html}
                      </div>
                    </div>""", unsafe_allow_html=True)
                st.markdown("</div>", unsafe_allow_html=True)

            st.markdown("</div>", unsafe_allow_html=True)  # close log-wrap

        elif log_uid and not log_sid:
            st.markdown("""
            <div class="info-box" style="margin-top:48px">
              <div class="info-box-icon">💬</div>
              <div class="info-box-title">Chọn phiên hội thoại</div>
              Chọn một phiên ở cột bên trái để xem nhật ký.
            </div>""", unsafe_allow_html=True)
        else:
            st.markdown("""
            <div class="info-box" style="margin-top:48px">
              <div class="info-box-icon">👤</div>
              <div class="info-box-title">Chọn người dùng</div>
              Chọn người dùng và phiên hội thoại để đọc nhật ký.
            </div>""", unsafe_allow_html=True)
