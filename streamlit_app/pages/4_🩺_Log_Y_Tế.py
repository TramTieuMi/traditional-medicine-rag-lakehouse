# streamlit_app/pages/4_medical_log.py
# Dashboard phân tích y tế cộng đồng — insight triệu chứng/bệnh/dược liệu từ câu hỏi người dùng

import json
import os
from collections import Counter
from io import BytesIO
from itertools import combinations

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import polars as pl
import streamlit as st
from minio import Minio

st.set_page_config(page_title="YHCT Log Y Tế", page_icon="🩺", layout="wide")

import sys as _sys, os as _os
_sys.path.insert(0, _os.path.dirname(_os.path.dirname(__file__)))
from utils import inject_css, kpis, section, fc, COLORS
inject_css()

MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT",   "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY",  "minio")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY",  "minio123")


@st.cache_data(ttl=120)
def load_parquet(bucket: str, key: str) -> pl.DataFrame | None:
    from minio.error import S3Error
    client = Minio(MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, secure=False)
    try:
        obj = client.get_object(bucket, key)
        return pl.read_parquet(BytesIO(obj.read()))
    except S3Error as e:
        if e.code == "NoSuchKey":
            return None
        raise


def _flatten(series: pl.Series) -> list[str]:
    out = []
    for val in series.drop_nulls().to_list():
        try:
            items = json.loads(val)
            if isinstance(items, list):
                out.extend(s.strip().lower() for s in items if s and str(s).strip())
        except Exception:
            pass
    return out


def _flatten_per_row(series: pl.Series) -> list[list[str]]:
    """Return list of lists — one per row (for co-occurrence)."""
    out = []
    for val in series.to_list():
        try:
            items = json.loads(val) if val else []
            out.append([s.strip().lower() for s in items if s and str(s).strip()])
        except Exception:
            out.append([])
    return out


def _short_id(uid: str) -> str:
    return f"#{str(uid)[-8:].upper()}" if uid else "—"


# ── Header ────────────────────────────────────────────────────────────────────
st.title("🩺 Phân tích Y tế Cộng đồng")
st.caption(
    "Insights từ câu hỏi người dùng: triệu chứng, bệnh, dược liệu thường gặp nhất — "
    "trích xuất bằng AI. Dữ liệu được tổng hợp ở cấp cộng đồng, không định danh cá nhân."
)
col_nav1, col_nav2 = st.columns(2)
with col_nav1:
    st.page_link("pages/2_👥_Quản_lý_người_dùng.py", label="→ Quản lý người dùng", icon="👥")
with col_nav2:
    st.page_link("pages/3_📊_Phân_tích_dữ_liệu.py", label="→ Analytics tổng thể", icon="📊")
st.markdown("---")

# ── Load data ─────────────────────────────────────────────────────────────────
try:
    med_df   = load_parquet("yhct-gold", "gold/mongodb/gold_medical_insights.parquet")
    users_df = load_parquet("yhct-silver", "silver/mongodb/silver_mongodb_users.parquet")
except Exception as e:
    st.error(f"Lỗi kết nối MinIO: {e}")
    st.stop()

if med_df is None or med_df.is_empty():
    st.info(
        "⏳ Chưa có dữ liệu log y tế.\n\n"
        "Dữ liệu sẽ xuất hiện sau khi người dùng hỏi chatbot và ETL pipeline được chạy."
    )
    st.stop()

# Flatten entity columns
symptoms_all   = _flatten(med_df["symptoms_list"])
diseases_all   = _flatten(med_df["diseases_list"])
herbs_all      = _flatten(med_df["herbs_list"])
body_parts_all = _flatten(med_df["body_parts_list"])

# Per-row lists (for co-occurrence)
symptoms_rows   = _flatten_per_row(med_df["symptoms_list"])
diseases_rows   = _flatten_per_row(med_df["diseases_list"])

# Pandas for time-based analysis
med_pd = med_df.to_pandas()
if "timestamp" in med_pd.columns:
    med_pd["dt"]       = pd.to_datetime(med_pd["timestamp"], errors="coerce")
    med_pd["date"]     = med_pd["dt"].dt.strftime("%Y-%m-%d")
    med_pd["hour"]     = med_pd["dt"].dt.hour
    med_pd["weekday"]  = med_pd["dt"].dt.day_name()
    med_pd["week"]     = med_pd["dt"].dt.strftime("%Y-W%U")

# Merge with user demographics (gender, age) — no name
if users_df is not None and not users_df.is_empty():
    user_cols = [c for c in ["user_id", "user_uuid", "gender", "age"] if c in users_df.columns]
    users_demo = users_df.select(user_cols).to_pandas()
    med_pd = med_pd.merge(users_demo, on="user_id", how="left")
else:
    med_pd["gender"] = None
    med_pd["age"]    = None

# Age bins
if "age" in med_pd.columns:
    med_pd["age_group"] = pd.cut(
        med_pd["age"].fillna(0).astype(float),
        bins=[0, 25, 35, 45, 60, 200],
        labels=["≤25", "26–35", "36–45", "46–60", "60+"],
    )

# ── KPIs ──────────────────────────────────────────────────────────────────────
total_logs      = med_df.shape[0]
unique_users    = med_df["user_id"].drop_nulls().n_unique()
unique_symptoms = len(set(symptoms_all))
unique_diseases = len(set(diseases_all))
unique_herbs    = len(set(herbs_all))

kpis([
    {"label": "Tổng lượt hỏi được log",    "value": f"{total_logs:,}"},
    {"label": "Người dùng có log",          "value": f"{unique_users:,}",    "accent": "blue"},
    {"label": "Loại triệu chứng",           "value": f"{unique_symptoms}",   "accent": "red"},
    {"label": "Loại bệnh ghi nhận",         "value": f"{unique_diseases}",   "accent": "amber"},
    {"label": "Dược liệu được hỏi",         "value": f"{unique_herbs}",      "accent": "teal"},
])
st.markdown('<div style="margin:4px 0 12px"></div>', unsafe_allow_html=True)

# ═══════════════════════════════════════════════════════════════════════════════
# 1. TOP ENTITIES
# ═══════════════════════════════════════════════════════════════════════════════
st.subheader("📊 Những gì người dùng hỏi nhiều nhất")

col1, col2 = st.columns(2)
with col1:
    if symptoms_all:
        sym_counts = pd.Series(symptoms_all).value_counts().head(15).reset_index()
        sym_counts.columns = ["Triệu chứng", "Số lần"]
        fig = px.bar(sym_counts, x="Số lần", y="Triệu chứng", orientation="h",
                     title="Top 15 triệu chứng mô tả nhiều nhất",
                     color_discrete_sequence=[COLORS[4]])
        fig.update_layout(yaxis=dict(autorange="reversed"), showlegend=False)
        st.plotly_chart(fc(fig, 440), use_container_width=True)

    if herbs_all:
        herb_counts = pd.Series(herbs_all).value_counts().head(15).reset_index()
        herb_counts.columns = ["Dược liệu", "Số lần"]
        fig_herb = px.bar(herb_counts, x="Số lần", y="Dược liệu", orientation="h",
                          title="Top 15 dược liệu được hỏi nhiều nhất",
                          color_discrete_sequence=[COLORS[0]])
        fig_herb.update_layout(yaxis=dict(autorange="reversed"), showlegend=False)
        st.plotly_chart(fc(fig_herb, 440), use_container_width=True)

with col2:
    if diseases_all:
        dis_counts = pd.Series(diseases_all).value_counts().head(15).reset_index()
        dis_counts.columns = ["Bệnh / Hội chứng", "Số lần"]
        fig_dis = px.bar(dis_counts, x="Số lần", y="Bệnh / Hội chứng", orientation="h",
                         title="Top 15 bệnh / hội chứng được đề cập",
                         color_discrete_sequence=[COLORS[1]])
        fig_dis.update_layout(yaxis=dict(autorange="reversed"), showlegend=False)
        st.plotly_chart(fc(fig_dis, 440), use_container_width=True)

    if body_parts_all:
        bp_counts = pd.Series(body_parts_all).value_counts().head(10).reset_index()
        bp_counts.columns = ["Bộ phận", "Số lần"]
        fig_bp = px.bar(bp_counts, x="Số lần", y="Bộ phận", orientation="h",
                        title="Bộ phận cơ thể được đề cập nhiều nhất",
                        color_discrete_sequence=[COLORS[2]])
        fig_bp.update_layout(yaxis=dict(autorange="reversed"), showlegend=False)
        st.plotly_chart(fc(fig_bp, 340), use_container_width=True)

st.markdown("---")

# ═══════════════════════════════════════════════════════════════════════════════
# 2. PATTERN THỜI GIAN
# ═══════════════════════════════════════════════════════════════════════════════
st.subheader("⏰ Khi nào người dùng hỏi nhiều nhất?")

time_col1, time_col2 = st.columns(2)

with time_col1:
    # Giờ cao điểm
    if "hour" in med_pd.columns and med_pd["hour"].notna().any():
        hour_counts = med_pd.groupby("hour").size().reset_index(name="Số lượt")
        fig_hour = px.bar(
            hour_counts, x="hour", y="Số lượt",
            title="Lượt hỏi theo giờ trong ngày",
            labels={"hour": "Giờ", "Số lượt": "Lượt hỏi"},
            color_discrete_sequence=[COLORS[2]],
        )
        fig_hour.update_layout(xaxis=dict(tickmode="linear", tick0=0, dtick=2), showlegend=False)
        st.plotly_chart(fc(fig_hour, 340), use_container_width=True)

with time_col2:
    # Ngày trong tuần
    if "weekday" in med_pd.columns and med_pd["weekday"].notna().any():
        day_order = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]
        day_vi    = {"Monday":"Thứ 2","Tuesday":"Thứ 3","Wednesday":"Thứ 4",
                     "Thursday":"Thứ 5","Friday":"Thứ 6","Saturday":"Thứ 7","Sunday":"Chủ nhật"}
        day_counts = (
            med_pd.groupby("weekday").size().reset_index(name="Số lượt")
            .assign(weekday=lambda d: d["weekday"].map(day_vi))
        )
        ordered = [day_vi[d] for d in day_order if day_vi[d] in day_counts["weekday"].values]
        fig_day = px.bar(
            day_counts, x="weekday", y="Số lượt",
            title="Lượt hỏi theo ngày trong tuần",
            labels={"weekday": "Ngày", "Số lượt": "Lượt hỏi"},
            color_discrete_sequence=[COLORS[5]],
            category_orders={"weekday": ordered},
        )
        fig_day.update_layout(showlegend=False)
        st.plotly_chart(fc(fig_day, 340), use_container_width=True)

# Xu hướng theo ngày (area) + Top 5 symptom trend
if "date" in med_pd.columns and med_pd["date"].notna().any():
    daily_logs = med_pd.groupby("date").size().reset_index(name="Số lượt hỏi")
    fig_trend = px.area(
        daily_logs, x="date", y="Số lượt hỏi",
        title="Lượt câu hỏi y tế theo ngày",
        color_discrete_sequence=[COLORS[0]], line_shape="spline",
    )
    fig_trend.update_layout(xaxis_title="")
    st.plotly_chart(fc(fig_trend, 300), use_container_width=True)

    if symptoms_all:
        top5_sym = pd.Series(symptoms_all).value_counts().head(5).index.tolist()
        rows_trend = []
        for _, row in med_pd.dropna(subset=["date"]).iterrows():
            try:
                syms = json.loads(row.get("symptoms_list", "[]"))
            except Exception:
                syms = []
            for s in syms:
                if s and s.lower() in top5_sym:
                    rows_trend.append({"date": row["date"], "symptom": s.lower()})
        if rows_trend:
            trend_pd = pd.DataFrame(rows_trend)
            trend_counts = trend_pd.groupby(["date","symptom"]).size().reset_index(name="count")
            fig_sym_trend = px.line(
                trend_counts, x="date", y="count", color="symptom",
                title="Xu hướng Top 5 triệu chứng theo ngày",
                markers=True,
                labels={"count": "Số lần", "symptom": "Triệu chứng", "date": ""},
                color_discrete_sequence=COLORS,
            )
            st.plotly_chart(fc(fig_sym_trend, 320), use_container_width=True)

st.markdown("---")

# ═══════════════════════════════════════════════════════════════════════════════
# 3. TRIỆU CHỨNG ĐỒNG XUẤT HIỆN (Co-occurrence)
# ═══════════════════════════════════════════════════════════════════════════════
st.subheader("🔗 Triệu chứng thường xuất hiện cùng nhau")
st.caption("Những cặp triệu chứng nào hay được người dùng đề cập trong cùng một câu hỏi — gợi ý nhóm bệnh lý liên quan.")

cooc_counter: Counter = Counter()
for row_syms in symptoms_rows:
    if len(row_syms) >= 2:
        for a, b in combinations(sorted(set(row_syms)), 2):
            cooc_counter[(a, b)] += 1

if cooc_counter:
    top_pairs = cooc_counter.most_common(12)
    cooc_df = pd.DataFrame([
        {"Cặp triệu chứng": f"{a}  ↔  {b}", "Số lần cùng xuất hiện": cnt}
        for (a, b), cnt in top_pairs
    ])
    fig_cooc = px.bar(
        cooc_df, x="Số lần cùng xuất hiện", y="Cặp triệu chứng", orientation="h",
        title="Top 12 cặp triệu chứng đồng xuất hiện nhiều nhất",
        color_discrete_sequence=[COLORS[3]],
    )
    fig_cooc.update_layout(yaxis=dict(autorange="reversed"), showlegend=False)
    st.plotly_chart(fc(fig_cooc, 420), use_container_width=True)
else:
    st.info("Cần ít nhất một số session có từ 2 triệu chứng trở lên để tính co-occurrence.")

st.markdown("---")

# ═══════════════════════════════════════════════════════════════════════════════
# 4. NHÂN KHẨU HỌC × Y TẾ
# ═══════════════════════════════════════════════════════════════════════════════
st.subheader("👥 Nhân khẩu học & Sức khỏe cộng đồng")

demo_col1, demo_col2 = st.columns(2)

with demo_col1:
    # Giới tính × triệu chứng
    if "gender" in med_pd.columns and med_pd["gender"].notna().any() and symptoms_all:
        top8_sym = pd.Series(symptoms_all).value_counts().head(8).index.tolist()
        gender_sym_rows = []
        for _, row in med_pd.iterrows():
            gender = row.get("gender")
            if not gender or str(gender) in ("None", "nan"):
                continue
            try:
                syms = json.loads(row.get("symptoms_list", "[]"))
            except Exception:
                syms = []
            for s in syms:
                if s and s.lower() in top8_sym:
                    gender_sym_rows.append({"Giới tính": str(gender), "Triệu chứng": s.lower()})

        if gender_sym_rows:
            gs_df = pd.DataFrame(gender_sym_rows)
            gs_counts = gs_df.groupby(["Triệu chứng","Giới tính"]).size().reset_index(name="Số lần")
            fig_gs = px.bar(
                gs_counts, x="Số lần", y="Triệu chứng", color="Giới tính",
                orientation="h", barmode="group",
                title="Triệu chứng phổ biến theo giới tính",
                color_discrete_map={"nam": COLORS[2], "nữ": COLORS[3], "khác": COLORS[6]},
            )
            fig_gs.update_layout(yaxis=dict(autorange="reversed"))
            st.plotly_chart(fc(fig_gs, 380), use_container_width=True)

with demo_col2:
    # Nhóm tuổi × bệnh
    if "age_group" in med_pd.columns and med_pd["age_group"].notna().any() and diseases_all:
        top6_dis = pd.Series(diseases_all).value_counts().head(6).index.tolist()
        age_dis_rows = []
        for _, row in med_pd.iterrows():
            ag = str(row.get("age_group",""))
            if not ag or ag in ("nan","None","<NA>"):
                continue
            try:
                dis = json.loads(row.get("diseases_list","[]"))
            except Exception:
                dis = []
            for d in dis:
                if d and d.lower() in top6_dis:
                    age_dis_rows.append({"Nhóm tuổi": ag, "Bệnh": d.lower()})

        if age_dis_rows:
            ad_df = pd.DataFrame(age_dis_rows)
            ad_counts = ad_df.groupby(["Nhóm tuổi","Bệnh"]).size().reset_index(name="Số lần")
            fig_ad = px.bar(
                ad_counts, x="Nhóm tuổi", y="Số lần", color="Bệnh",
                barmode="stack",
                title="Nhóm tuổi × Bệnh được hỏi",
                category_orders={"Nhóm tuổi": ["≤25","26–35","36–45","46–60","60+"]},
                color_discrete_sequence=COLORS,
            )
            st.plotly_chart(fc(fig_ad, 380), use_container_width=True)

# Phân bố giới tính + tuổi
stat_col1, stat_col2 = st.columns(2)
with stat_col1:
    if "gender" in med_pd.columns and med_pd["gender"].notna().any():
        g_counts = med_pd["gender"].dropna().astype(str)
        g_counts = g_counts[g_counts.isin(["nam","nữ","khác"])].value_counts().reset_index()
        g_counts.columns = ["Giới tính", "Số người dùng"]
        fig_g = px.pie(g_counts, names="Giới tính", values="Số người dùng",
                       title="Phân bố giới tính người dùng", hole=0.4,
                       color_discrete_map={"nam": COLORS[2], "nữ": COLORS[3], "khác": COLORS[6]})
        st.plotly_chart(fc(fig_g, 320), use_container_width=True)

with stat_col2:
    if "age_group" in med_pd.columns and med_pd["age_group"].notna().any():
        ag_counts = (
            med_pd["age_group"].dropna().astype(str)
            .value_counts().reindex(["≤25","26–35","36–45","46–60","60+"], fill_value=0)
            .reset_index()
        )
        ag_counts.columns = ["Nhóm tuổi", "Số người dùng"]
        fig_ag = px.bar(ag_counts, x="Nhóm tuổi", y="Số người dùng",
                        title="Phân bố nhóm tuổi người dùng",
                        color_discrete_sequence=[COLORS[1]])
        fig_ag.update_layout(showlegend=False)
        st.plotly_chart(fc(fig_ag, 320), use_container_width=True)

st.markdown("---")

# ═══════════════════════════════════════════════════════════════════════════════
# 5. BẢNG LOG CHI TIẾT — anonymized
# ═══════════════════════════════════════════════════════════════════════════════
st.subheader("🔍 Bảng log chi tiết")
st.caption("Mỗi dòng là một phiên hỏi bệnh. ID người dùng được rút gọn để bảo vệ danh tính.")

def _parse_to_str(val) -> str:
    try:
        items = json.loads(val)
        return ", ".join(items) if items else "—"
    except Exception:
        return "—"

log_display = med_pd.copy()
log_display["user_short"] = log_display["user_id"].fillna("").apply(_short_id)
for col in ["symptoms_list","diseases_list","herbs_list","body_parts_list"]:
    if col in log_display.columns:
        log_display[col] = log_display[col].fillna("[]").apply(_parse_to_str)

# Filters
flt1, flt2, flt3 = st.columns(3)
with flt1:
    search_sym = st.text_input("🦠 Lọc theo triệu chứng", placeholder="vd: đau đầu")
with flt2:
    search_dis = st.text_input("🏥 Lọc theo bệnh",        placeholder="vd: tiểu đường")
with flt3:
    search_herb = st.text_input("🌿 Lọc theo dược liệu",  placeholder="vd: gừng")

filtered = log_display.copy()
if search_sym:
    filtered = filtered[filtered["symptoms_list"].str.lower().str.contains(search_sym.lower())]
if search_dis:
    filtered = filtered[filtered["diseases_list"].str.lower().str.contains(search_dis.lower())]
if search_herb:
    filtered = filtered[filtered["herbs_list"].str.lower().str.contains(search_herb.lower())]

show_cols = {
    "user_short":       "User ID",
    "gender":           "Giới tính",
    "age":              "Tuổi",
    "symptoms_list":    "Triệu chứng",
    "diseases_list":    "Bệnh / Hội chứng",
    "herbs_list":       "Dược liệu hỏi",
    "body_parts_list":  "Bộ phận cơ thể",
    "timestamp":        "Thời gian",
}
avail = [c for c in show_cols if c in filtered.columns]
st.dataframe(
    filtered[avail].rename(columns=show_cols).sort_values("Thời gian", ascending=False),
    use_container_width=True, hide_index=True, height=380,
)
st.caption(f"Hiển thị {len(filtered):,} / {len(log_display):,} bản ghi")
