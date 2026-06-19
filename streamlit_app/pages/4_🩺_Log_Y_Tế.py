# streamlit_app/pages/4_medical_log.py
# Dashboard phân tích log y tế — insight triệu chứng/bệnh từ câu hỏi người dùng

import json
import os
from io import BytesIO

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import polars as pl
import streamlit as st
from minio import Minio

st.set_page_config(page_title="YHCT Log Y Tế", page_icon="🩺", layout="wide")

MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT",   "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY",  "minio")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY",  "minio123")


@st.cache_data(ttl=120)
def load_parquet(bucket: str, key: str) -> pl.DataFrame | None:
    """None = file chưa tồn tại (chưa chạy pipeline). Exception = lỗi thật."""
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
    """Flatten JSON-list column → Python list of strings."""
    out = []
    for val in series.drop_nulls().to_list():
        try:
            items = json.loads(val)
            if isinstance(items, list):
                out.extend(s.strip().lower() for s in items if s and str(s).strip())
        except Exception:
            pass
    return out


# ── Header ────────────────────────────────────────────────────────────────────
st.title("🩺 Log Y Tế — Phân tích câu hỏi người dùng")
st.caption(
    "Insight tự động từ câu hỏi người dùng: triệu chứng, bệnh, dược liệu thường được nhắc đến nhất. "
    "Dữ liệu được trích xuất bằng AI từ nội dung hội thoại."
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

if med_df is None or users_df is None or med_df.is_empty():
    st.info(
        "⏳ Chưa có dữ liệu log y tế.\n\n"
        "Dữ liệu sẽ xuất hiện sau khi:\n"
        "1. Người dùng hỏi chatbot (tạo ra `MedicalEntityLog` trong MongoDB)\n"
        "2. ETL pipeline được chạy để đẩy dữ liệu lên Gold layer\n\n"
        "**Cách chạy pipeline:** vào trang 📊 Phân tích dữ liệu → tab Thêm tài liệu → hoặc chạy job `user_lakehouse_job` trong Dagster."
    )
    st.page_link("pages/3_📊_Phân_tích_dữ_liệu.py", label="→ Đến trang Phân tích để chạy pipeline", icon="📊")
    st.stop()

# Pre-flatten entity columns
symptoms_all   = _flatten(med_df["symptoms_list"])
diseases_all   = _flatten(med_df["diseases_list"])
herbs_all      = _flatten(med_df["herbs_list"])
body_parts_all = _flatten(med_df["body_parts_list"])

# ── KPI ───────────────────────────────────────────────────────────────────────
total_logs       = med_df.shape[0]
unique_users     = med_df["user_id"].drop_nulls().n_unique()
unique_symptoms  = len(set(symptoms_all))
unique_diseases  = len(set(diseases_all))
unique_herbs     = len(set(herbs_all))

k1, k2, k3, k4, k5 = st.columns(5)
k1.metric("Tổng lượt hỏi được log",  f"{total_logs:,}")
k2.metric("Người dùng có log",        f"{unique_users:,}")
k3.metric("Loại triệu chứng ghi nhận", f"{unique_symptoms}")
k4.metric("Loại bệnh ghi nhận",       f"{unique_diseases}")
k5.metric("Dược liệu được hỏi",       f"{unique_herbs}")

st.markdown("---")

# ═══════════════════════════════════════════════════════════════════════════════
# TOP ENTITIES — 4 biểu đồ chính
# ═══════════════════════════════════════════════════════════════════════════════
st.subheader("📊 Những gì người dùng hỏi nhiều nhất")

col1, col2 = st.columns(2)

with col1:
    # Triệu chứng
    if symptoms_all:
        sym_counts = pd.Series(symptoms_all).value_counts().head(15).reset_index()
        sym_counts.columns = ["Triệu chứng", "Số lần"]
        fig = px.bar(
            sym_counts, x="Số lần", y="Triệu chứng", orientation="h",
            title="🦠 Top 15 triệu chứng người dùng mô tả nhiều nhất",
            color="Số lần", color_continuous_scale="Reds",
        )
        fig.update_layout(yaxis=dict(autorange="reversed"),
                          plot_bgcolor="rgba(0,0,0,0)", coloraxis_showscale=False)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Chưa có dữ liệu triệu chứng.")

    # Dược liệu
    if herbs_all:
        herb_counts = pd.Series(herbs_all).value_counts().head(15).reset_index()
        herb_counts.columns = ["Dược liệu", "Số lần"]
        fig_herb = px.bar(
            herb_counts, x="Số lần", y="Dược liệu", orientation="h",
            title="🌿 Top 15 dược liệu người dùng hỏi nhiều nhất",
            color="Số lần", color_continuous_scale="Greens",
        )
        fig_herb.update_layout(yaxis=dict(autorange="reversed"),
                               plot_bgcolor="rgba(0,0,0,0)", coloraxis_showscale=False)
        st.plotly_chart(fig_herb, use_container_width=True)

with col2:
    # Bệnh / hội chứng
    if diseases_all:
        dis_counts = pd.Series(diseases_all).value_counts().head(15).reset_index()
        dis_counts.columns = ["Bệnh / Hội chứng", "Số lần"]
        fig_dis = px.bar(
            dis_counts, x="Số lần", y="Bệnh / Hội chứng", orientation="h",
            title="🏥 Top 15 bệnh / hội chứng được đề cập nhiều nhất",
            color="Số lần", color_continuous_scale="Oranges",
        )
        fig_dis.update_layout(yaxis=dict(autorange="reversed"),
                              plot_bgcolor="rgba(0,0,0,0)", coloraxis_showscale=False)
        st.plotly_chart(fig_dis, use_container_width=True)
    else:
        st.info("Chưa có dữ liệu bệnh lý.")

    # Bộ phận cơ thể
    if body_parts_all:
        bp_counts = pd.Series(body_parts_all).value_counts().head(15).reset_index()
        bp_counts.columns = ["Bộ phận", "Số lần"]
        fig_bp = px.bar(
            bp_counts, x="Số lần", y="Bộ phận", orientation="h",
            title="🫁 Top 15 bộ phận cơ thể được đề cập",
            color="Số lần", color_continuous_scale="Blues",
        )
        fig_bp.update_layout(yaxis=dict(autorange="reversed"),
                             plot_bgcolor="rgba(0,0,0,0)", coloraxis_showscale=False)
        st.plotly_chart(fig_bp, use_container_width=True)

st.markdown("---")

# ═══════════════════════════════════════════════════════════════════════════════
# TREND THEO THỜI GIAN
# ═══════════════════════════════════════════════════════════════════════════════
st.subheader("📅 Xu hướng câu hỏi theo thời gian")

med_pd = med_df.to_pandas()
if "timestamp" in med_pd.columns:
    med_pd["date"] = pd.to_datetime(med_pd["timestamp"], errors="coerce").dt.strftime("%Y-%m-%d")
    daily_logs = med_pd.groupby("date").size().reset_index(name="Số lượt hỏi")

    if not daily_logs.empty:
        fig_trend = px.area(
            daily_logs, x="date", y="Số lượt hỏi",
            title="Lượt câu hỏi y tế theo ngày",
            color_discrete_sequence=["#E74C3C"], line_shape="spline",
        )
        fig_trend.update_layout(plot_bgcolor="rgba(0,0,0,0)", xaxis_title="")
        st.plotly_chart(fig_trend, use_container_width=True)

        # Top symptom trend over time (chọn top 5)
        if symptoms_all:
            top5_sym = pd.Series(symptoms_all).value_counts().head(5).index.tolist()

            # Build daily count per symptom
            rows = []
            for _, row in med_pd.dropna(subset=["date"]).iterrows():
                try:
                    syms = json.loads(row.get("symptoms_list", "[]"))
                except Exception:
                    syms = []
                for s in syms:
                    if s and s.lower() in top5_sym:
                        rows.append({"date": row["date"], "symptom": s.lower()})

            if rows:
                trend_pd = pd.DataFrame(rows)
                trend_counts = (
                    trend_pd.groupby(["date", "symptom"])
                    .size().reset_index(name="count")
                )
                fig_sym_trend = px.line(
                    trend_counts, x="date", y="count", color="symptom",
                    title="📈 Xu hướng Top 5 triệu chứng theo ngày",
                    markers=True,
                    labels={"count": "Số lần", "symptom": "Triệu chứng", "date": ""},
                )
                fig_sym_trend.update_layout(plot_bgcolor="rgba(0,0,0,0)")
                st.plotly_chart(fig_sym_trend, use_container_width=True)

st.markdown("---")

# ═══════════════════════════════════════════════════════════════════════════════
# BẢNG LOG CHI TIẾT — có filter
# ═══════════════════════════════════════════════════════════════════════════════
st.subheader("🔍 Bảng log chi tiết")

# Merge với tên người dùng
users_pd = users_df.select(["user_id", "full_name", "gender", "age"]).to_pandas()
log_display = med_pd.merge(users_pd, on="user_id", how="left")

# Parse entities thành chuỗi đọc được
def _parse_to_str(val: str) -> str:
    try:
        items = json.loads(val)
        return ", ".join(items) if items else "—"
    except Exception:
        return "—"

for col in ["symptoms_list", "diseases_list", "herbs_list", "body_parts_list"]:
    if col in log_display.columns:
        log_display[col] = log_display[col].fillna("[]").apply(_parse_to_str)

# Filters
filter_col1, filter_col2, filter_col3 = st.columns(3)
with filter_col1:
    search_user = st.text_input("🔍 Tìm theo tên người dùng", placeholder="Nhập tên...")
with filter_col2:
    search_sym  = st.text_input("🦠 Tìm theo triệu chứng", placeholder="Nhập triệu chứng...")
with filter_col3:
    search_dis  = st.text_input("🏥 Tìm theo bệnh", placeholder="Nhập tên bệnh...")

filtered = log_display.copy()
if search_user:
    filtered = filtered[filtered["full_name"].fillna("").str.lower().str.contains(search_user.lower())]
if search_sym:
    filtered = filtered[filtered["symptoms_list"].str.lower().str.contains(search_sym.lower())]
if search_dis:
    filtered = filtered[filtered["diseases_list"].str.lower().str.contains(search_dis.lower())]

display_cols = {
    "full_name":      "Người dùng",
    "gender":         "Giới tính",
    "age":            "Tuổi",
    "symptoms_list":  "Triệu chứng",
    "diseases_list":  "Bệnh / Hội chứng",
    "herbs_list":     "Dược liệu hỏi",
    "body_parts_list": "Bộ phận cơ thể",
    "timestamp":      "Thời gian",
}
avail_cols = [c for c in display_cols if c in filtered.columns]
st.dataframe(
    filtered[avail_cols].rename(columns=display_cols).sort_values("Thời gian", ascending=False),
    use_container_width=True,
    hide_index=True,
    height=400,
)
st.caption(f"Hiển thị {len(filtered):,} / {len(log_display):,} bản ghi")

st.markdown("---")

# ═══════════════════════════════════════════════════════════════════════════════
# PER-USER BREAKDOWN — ai hỏi gì nhiều nhất
# ═══════════════════════════════════════════════════════════════════════════════
st.subheader("👤 Breakdown theo từng người dùng")

if not log_display.empty and "full_name" in log_display.columns:
    # Count rows per user
    user_log_counts = (
        log_display.groupby(["user_id", "full_name", "gender", "age"])
        .size().reset_index(name="Số lượt log")
        .sort_values("Số lượt log", ascending=False)
        .head(20)
    )

    fig_users = px.bar(
        user_log_counts, x="Số lượt log", y="full_name", orientation="h",
        title="🏆 Top 20 người dùng có nhiều câu hỏi y tế nhất",
        color="Số lượt log", color_continuous_scale="Purples",
        labels={"full_name": "Người dùng"},
    )
    fig_users.update_layout(yaxis=dict(autorange="reversed"),
                            plot_bgcolor="rgba(0,0,0,0)", coloraxis_showscale=False)
    st.plotly_chart(fig_users, use_container_width=True)

    # Chi tiết từng user khi chọn
    st.markdown("#### Chi tiết người dùng")
    selected_name = st.selectbox(
        "Chọn người dùng để xem chi tiết",
        options=[""] + user_log_counts["full_name"].dropna().tolist(),
    )
    if selected_name:
        user_logs = log_display[log_display["full_name"] == selected_name]
        u_symptoms  = []
        u_diseases  = []
        u_herbs     = []

        for col_name, target in [("symptoms_list", u_symptoms),
                                  ("diseases_list", u_diseases),
                                  ("herbs_list",    u_herbs)]:
            for val in user_logs[col_name].tolist():
                try:
                    items = json.loads(val) if val != "—" else []
                    target.extend(i.strip() for i in items if i)
                except Exception:
                    pass

        uc1, uc2, uc3 = st.columns(3)
        with uc1:
            if u_symptoms:
                s_cnt = pd.Series(u_symptoms).value_counts().reset_index()
                s_cnt.columns = ["Triệu chứng", "Số lần"]
                st.plotly_chart(px.bar(
                    s_cnt.head(8), x="Số lần", y="Triệu chứng", orientation="h",
                    title=f"Triệu chứng của {selected_name}",
                    color_discrete_sequence=["#E74C3C"], height=300,
                ), use_container_width=True)
            else:
                st.info("Không có dữ liệu triệu chứng.")
        with uc2:
            if u_diseases:
                d_cnt = pd.Series(u_diseases).value_counts().reset_index()
                d_cnt.columns = ["Bệnh", "Số lần"]
                st.plotly_chart(px.bar(
                    d_cnt.head(8), x="Số lần", y="Bệnh", orientation="h",
                    title=f"Bệnh đề cập bởi {selected_name}",
                    color_discrete_sequence=["#E67E22"], height=300,
                ), use_container_width=True)
            else:
                st.info("Không có dữ liệu bệnh.")
        with uc3:
            if u_herbs:
                h_cnt = pd.Series(u_herbs).value_counts().reset_index()
                h_cnt.columns = ["Dược liệu", "Số lần"]
                st.plotly_chart(px.bar(
                    h_cnt.head(8), x="Số lần", y="Dược liệu", orientation="h",
                    title=f"Dược liệu hỏi bởi {selected_name}",
                    color_discrete_sequence=["#27AE60"], height=300,
                ), use_container_width=True)
            else:
                st.info("Không có dữ liệu dược liệu.")

        st.page_link("pages/2_👥_Quản_lý_người_dùng.py",
                     label="→ Xem lịch sử hội thoại của người này", icon="💬")
