# streamlit_app/pages/3_analytics.py

import json
import os
from io import BytesIO

import httpx
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import polars as pl
import streamlit as st
from minio import Minio

st.set_page_config(page_title="YHCT Analytics", page_icon="📊", layout="wide")

MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT",   "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY",  "minio")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY",  "minio123")
DAGSTER_URL      = os.getenv("DAGSTER_URL",        "http://dagster:3001")
RAW_DATA_DIR     = os.getenv("RAW_DATA_DIR",       "/app/data/raw")


@st.cache_data(ttl=300)
def load_parquet(bucket: str, key: str) -> pl.DataFrame:
    client = Minio(MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, secure=False)
    obj    = client.get_object(bucket, key)
    return pl.read_parquet(BytesIO(obj.read()))


def _parse_json_list_column(series: pl.Series) -> list[str]:
    """Flatten a Polars column of JSON-encoded list strings into a Python list."""
    result = []
    for val in series.drop_nulls().to_list():
        try:
            items = json.loads(val)
            if isinstance(items, list):
                result.extend(str(i).strip() for i in items if i)
        except Exception:
            pass
    return result


# ── Dagster helpers ───────────────────────────────────────────────────────────
def _discover_repo_info() -> tuple[str, str, str] | None:
    query = """
    query {
      repositoriesOrError {
        ... on RepositoryConnection {
          nodes { name location { name } pipelines { name } }
        }
        ... on PythonError { message }
      }
    }
    """
    try:
        r     = httpx.post(f"{DAGSTER_URL}/graphql", json={"query": query}, timeout=10)
        nodes = r.json()["data"]["repositoriesOrError"].get("nodes", [])
        if not nodes:
            return None
        repo  = nodes[0]
        pipes = [p["name"] for p in repo.get("pipelines", [])]
        job   = "all_assets_job" if "all_assets_job" in pipes else (pipes[0] if pipes else "all_assets_job")
        return repo["location"]["name"], repo["name"], job
    except Exception:
        return None


def _launch_pipeline(loc: str, repo: str, job: str) -> tuple[str | None, str | None]:
    mutation = """
    mutation LaunchRun($executionParams: ExecutionParams!) {
      launchRun(executionParams: $executionParams) {
        ... on LaunchRunSuccess  { run { runId status } }
        ... on PipelineNotFoundError { message }
        ... on InvalidSubsetError    { message }
        ... on PythonError           { message }
      }
    }
    """
    try:
        r = httpx.post(
            f"{DAGSTER_URL}/graphql",
            json={"query": mutation, "variables": {
                "executionParams": {
                    "selector": {"repositoryLocationName": loc, "repositoryName": repo, "jobName": job},
                    "executionMetadata": {}, "runConfigData": "{}",
                }
            }}, timeout=30,
        )
        result = r.json()["data"]["launchRun"]
        if "run" in result:
            return result["run"]["runId"], None
        return None, result.get("message", "Unknown error")
    except Exception as e:
        return None, str(e)


def _get_recent_runs(limit: int = 5) -> list[dict]:
    q = """
    query RecentRuns($limit: Int!) {
      runsOrError(limit: $limit) {
        ... on Runs { results { runId status startTime endTime tags { key value } } }
      }
    }
    """
    try:
        r = httpx.post(f"{DAGSTER_URL}/graphql", json={"query": q, "variables": {"limit": limit}}, timeout=10)
        return r.json()["data"]["runsOrError"].get("results", [])
    except Exception:
        return []


def _upload_to_minio(filename: str, data: bytes) -> None:
    client = Minio(MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, secure=False)
    if not client.bucket_exists("yhct-docs"):
        client.make_bucket("yhct-docs")
    client.put_object("yhct-docs", filename, BytesIO(data), length=len(data), content_type="application/pdf")


# ── Header + navigation ───────────────────────────────────────────────────────
st.title("📊 YHCT Analytics Dashboard")
st.page_link("pages/2_👥_Quản_lý_người_dùng.py", label="→ Xem chi tiết từng người dùng", icon="👥")
st.markdown("---")

tab1, tab2, tab3, tab4, tab5, tab6, tab7, tab8 = st.tabs([
    "🌿 Dược liệu",
    "🫀 Tạng phủ",
    "📄 Chunks",
    "📚 Nguồn tài liệu",
    "📥 Thêm tài liệu",
    "👥 Tương tác Người dùng",
    "💬 Hiệu năng Chatbot",
    "🩺 Xu hướng Dịch tễ",
])

# ── Tab 1: Herb mentions ──────────────────────────────────────────────────────
with tab1:
    st.subheader("Top dược liệu xuất hiện nhiều nhất trong tài liệu")
    try:
        herb_df = load_parquet("yhct-gold", "gold/herbs/gold_herb_mentions.parquet")
        top_herbs = (
            herb_df.group_by("herb_name")
            .agg(pl.sum("count_in_chunk").alias("total"))
            .sort("total", descending=True)
            .head(20)
        )
        fig = px.bar(
            top_herbs.to_pandas(), x="total", y="herb_name", orientation="h",
            color="total", color_continuous_scale="Greens",
            title="Top 20 dược liệu trong tài liệu YHCT",
            labels={"total": "Số lần xuất hiện", "herb_name": "Dược liệu"},
        )
        fig.update_layout(height=600, yaxis=dict(autorange="reversed"))
        st.plotly_chart(fig, use_container_width=True)
        with st.expander("Bảng chi tiết"):
            st.dataframe(top_herbs.to_pandas(), use_container_width=True)
    except Exception as e:
        st.error(f"Lỗi load dữ liệu: {e}")

# ── Tab 2: Tạng phủ ───────────────────────────────────────────────────────────
with tab2:
    st.subheader("Phân bố tạng phủ trong tài liệu")
    try:
        tp_df = load_parquet("yhct-gold", "gold/tang_phu/gold_tang_phu_mentions.parquet")
        tp_counts = (
            tp_df.group_by("tang_phu")
            .agg(pl.len().alias("count"))
            .sort("count", descending=True)
        )
        TANG_PHU_LABELS = {
            "ty_vi": "Tỳ Vị (Tiêu hóa)", "can_dom": "Can Đởm (Gan Mật)",
            "than": "Thận (Bàng quang)", "phe_dai_trang": "Phế Đại tràng",
            "tam_tieu_trang": "Tâm Tiểu tràng",
        }
        tp_pd = tp_counts.to_pandas()
        tp_pd["tang_phu_label"] = tp_pd["tang_phu"].map(TANG_PHU_LABELS)
        col1, col2 = st.columns(2)
        with col1:
            st.plotly_chart(px.pie(
                tp_pd, values="count", names="tang_phu_label", title="Tỷ lệ đề cập tạng phủ",
                hole=0.4, color_discrete_sequence=px.colors.qualitative.Set2,
            ), use_container_width=True)
        with col2:
            st.plotly_chart(px.bar(
                tp_pd, x="tang_phu_label", y="count", color="count",
                color_continuous_scale="Teal", title="Số chunk đề cập theo tạng phủ",
                labels={"count": "Số chunks", "tang_phu_label": "Tạng phủ"},
            ), use_container_width=True)
        st.dataframe(tp_pd, use_container_width=True)
    except Exception as e:
        st.error(f"Lỗi load dữ liệu: {e}")

# ── Tab 3: Chunks stats ───────────────────────────────────────────────────────
with tab3:
    st.subheader("Thống kê chunks")
    try:
        chunk_df = load_parquet("yhct-gold", "gold/chunks/gold_yhct_chunks.parquet")
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Tổng chunks",          f"{chunk_df.shape[0]:,}")
        c2.metric("Tổng trang",           f"{chunk_df['page_num'].n_unique():,}")
        c3.metric("TB từ/chunk",          f"{chunk_df['word_count'].mean():.0f}")
        c4.metric("Số nguồn tài liệu",   f"{chunk_df['source_file'].n_unique():,}")
        st.plotly_chart(px.histogram(
            chunk_df.to_pandas(), x="word_count", nbins=40,
            color_discrete_sequence=["#2d9e5f"],
            title="Phân bố số từ trong mỗi chunk",
            labels={"word_count": "Số từ", "count": "Số chunks"},
        ), use_container_width=True)
        by_source = chunk_df.group_by("source_file").agg(pl.len().alias("chunks")).sort("chunks", descending=True)
        st.plotly_chart(px.bar(
            by_source.to_pandas(), x="source_file", y="chunks",
            color="chunks", color_continuous_scale="Blues",
            title="Số chunks theo nguồn tài liệu",
        ), use_container_width=True)
    except Exception as e:
        st.error(f"Lỗi load dữ liệu: {e}")

# ── Tab 4: Nguồn tài liệu ────────────────────────────────────────────────────
with tab4:
    st.subheader("📚 Danh sách tài liệu trong hệ thống")
    try:
        bronze_df   = load_parquet("yhct-bronze", "bronze/pdf/bronze_pdf_pages.parquet")
        source_stats = (
            bronze_df.group_by(["source_file", "doc_id"])
            .agg([pl.len().alias("total_pages"), pl.col("word_count").sum().alias("total_words")])
            .sort("source_file")
        )
        for row in source_stats.iter_rows(named=True):
            with st.expander(f"📖 {row['source_file']}"):
                col1, col2, col3 = st.columns(3)
                col1.metric("Tổng trang", row["total_pages"])
                col2.metric("Tổng từ",    f"{row['total_words']:,}")
                col3.metric("doc_id",     row["doc_id"])
    except Exception as e:
        st.error(f"Lỗi load dữ liệu: {e}")

# ── Tab 5: Upload PDF ────────────────────────────────────────────────────────
with tab5:
    st.subheader("📥 Nhập tài liệu mới vào hệ thống")
    st.info("Upload file PDF — **Dagster Sensor** tự phát hiện trong 30 giây và trigger pipeline đầy đủ.", icon="🤖")
    col_upload, col_status = st.columns(2)
    with col_upload:
        st.markdown("#### 1. Chọn và lưu file")
        uploaded_file = st.file_uploader("Chọn file PDF", type=["pdf"], key="pdf_upload")
        if uploaded_file:
            file_bytes   = uploaded_file.getvalue()
            file_size_kb = len(file_bytes) // 1024
            st.info(f"**{uploaded_file.name}** · {file_size_kb:,} KB")
            if os.path.exists(os.path.join(RAW_DATA_DIR, uploaded_file.name)):
                st.warning("File đã tồn tại — upload sẽ ghi đè.")
            if st.button("💾 Lưu vào hệ thống", type="primary"):
                try:
                    os.makedirs(RAW_DATA_DIR, exist_ok=True)
                    with open(os.path.join(RAW_DATA_DIR, uploaded_file.name), "wb") as f:
                        f.write(file_bytes)
                    st.success(f"✅ Đã lưu vào `{RAW_DATA_DIR}/{uploaded_file.name}`")
                except Exception as e:
                    st.error(f"❌ Lỗi lưu file: {e}")
                    st.stop()
                try:
                    _upload_to_minio(uploaded_file.name, file_bytes)
                    st.success("✅ Đã lưu lên MinIO (yhct-docs)")
                except Exception as e:
                    st.warning(f"⚠ MinIO: {e}")
                st.success("🤖 Sensor sẽ tự kích hoạt pipeline trong ~30 giây.")
    with col_status:
        st.markdown("#### 2. Theo dõi pipeline")
        if st.button("🔄 Refresh", key="refresh_runs"):
            load_parquet.clear()
            st.rerun()
        import datetime as _dt
        STATUS_ICON = {"SUCCESS": "✅", "FAILURE": "❌", "STARTED": "🔄", "QUEUED": "⏳", "CANCELED": "🛑"}
        for run in _get_recent_runs(limit=5):
            icon  = STATUS_ICON.get(run["status"], "❓")
            start = _dt.datetime.fromtimestamp(run["startTime"]).strftime("%H:%M:%S") if run["startTime"] else "—"
            end   = _dt.datetime.fromtimestamp(run["endTime"]).strftime("%H:%M:%S") if run["endTime"] else "đang chạy"
            tags  = {t["key"]: t["value"] for t in run.get("tags", [])}
            with st.expander(f"{icon} `{run['runId'][:8]}` · {start}→{end} · _{tags.get('triggered_by','manual')}_",
                             expanded=run["status"] in ("STARTED", "QUEUED")):
                st.write(f"**Status:** {run['status']}")
                if files := tags.get("new_files"):
                    st.write(f"**File mới:** {files}")
                st.write(f"**Run ID:** `{run['runId']}`")

# ═══════════════════════════════════════════════════════════════════════════════
# Tab 6: Tương tác Người dùng — đầy đủ
# ═══════════════════════════════════════════════════════════════════════════════
with tab6:
    st.subheader("👥 Phân tích Tương tác Người dùng Web")

    try:
        user_eng_df = load_parquet("yhct-gold", "gold/mongodb/gold_user_engagement.parquet")
    except Exception as e:
        st.error(f"Lỗi load dữ liệu engagement: {e}")
        user_eng_df = pl.DataFrame()

    # Load chat performance để lấy thêm chỉ số câu hỏi
    try:
        chat_perf_df = load_parquet("yhct-gold", "gold/mongodb/gold_chat_performance.parquet")
        chat_pd      = chat_perf_df.to_pandas() if not chat_perf_df.is_empty() else pd.DataFrame()
    except Exception:
        chat_pd = pd.DataFrame()

    if user_eng_df.is_empty():
        st.info("Chưa có dữ liệu tương tác người dùng. Hãy chạy ETL pipeline.")
    else:
        df_pd = user_eng_df.to_pandas()

        # ── KPI row 1 ──────────────────────────────────────────────────────
        latest       = df_pd.iloc[-1]
        avg_duration = df_pd["average_session_duration_sec"].mean()
        avg_bounce   = df_pd["bounce_rate_pct"].mean()
        total_views  = int(df_pd["total_page_views"].sum())
        total_sess   = int(len(chat_pd)) if not chat_pd.empty else 0
        avg_msgs     = float(chat_pd["total_messages_exchanged"].mean()) if not chat_pd.empty else 0.0
        avg_retain   = df_pd["retention_rate_pct"].mean()

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("DAU (ngày gần nhất)",  f"{int(latest['total_active_users']):,}")
        c2.metric("Tổng lượt xem trang",  f"{total_views:,}")
        c3.metric("Thời gian TB/phiên",   f"{avg_duration:.1f}s")
        c4.metric("Tỷ lệ thoát (Bounce)", f"{avg_bounce:.1f}%")

        c5, c6, c7, c8 = st.columns(4)
        c5.metric("Tổng phiên chat",      f"{total_sess:,}")
        c6.metric("TB câu hỏi/phiên",     f"{avg_msgs:.1f}")
        c7.metric("Retention rate TB",    f"{avg_retain:.1f}%")
        c8.metric("Người dùng mới TB/ngày", f"{df_pd['new_registered_users'].mean():.1f}")

        st.markdown("---")

        # ── Chart 1: DAU trend ──────────────────────────────────────────────
        col1, col2 = st.columns([3, 2])
        with col1:
            fig_dau = px.line(
                df_pd, x="date", y="total_active_users",
                title="📈 Xu hướng người dùng hoạt động (DAU)",
                markers=True, line_shape="spline",
                color_discrete_sequence=["#2E86C1"],
            )
            fig_dau.update_layout(xaxis_title="", yaxis_title="Active Users",
                                  plot_bgcolor="rgba(0,0,0,0)")
            st.plotly_chart(fig_dau, use_container_width=True)
        with col2:
            avg_d = df_pd["device_desktop_pct"].mean()
            avg_m = df_pd["device_mobile_pct"].mean()
            fig_dev = px.pie(
                names=["Desktop", "Mobile", "Khác"],
                values=[avg_d, avg_m, max(0, 100 - avg_d - avg_m)],
                title="📱 Phân bố thiết bị", hole=0.4,
                color_discrete_sequence=["#117A65", "#48C9B0", "#A9CCE3"],
            )
            fig_dev.update_layout(margin=dict(t=50, b=0, l=0, r=0))
            st.plotly_chart(fig_dev, use_container_width=True)

        # ── Chart 2: Người mới vs Quay lại ─────────────────────────────────
        df_pd["returning_users"] = (
            df_pd["total_active_users"] - df_pd["new_registered_users"]
        ).clip(lower=0)
        fig_stack = px.bar(
            df_pd, x="date",
            y=["new_registered_users", "returning_users"],
            title="👤 Người dùng mới vs Quay lại theo ngày",
            barmode="stack",
            color_discrete_map={
                "new_registered_users": "#2E86C1",
                "returning_users":      "#27AE60",
            },
            labels={"value": "Số người", "variable": ""},
        )
        fig_stack.for_each_trace(lambda t: t.update(name={
            "new_registered_users": "Người mới",
            "returning_users":      "Quay lại",
        }.get(t.name, t.name)))
        fig_stack.update_layout(plot_bgcolor="rgba(0,0,0,0)", xaxis_title="")
        st.plotly_chart(fig_stack, use_container_width=True)

        # ── Chart 3: Retention + session duration ───────────────────────────
        col3, col4 = st.columns(2)
        with col3:
            fig_ret = px.line(
                df_pd, x="date", y="retention_rate_pct",
                title="🔁 Tỷ lệ quay lại (Retention Rate %)",
                markers=True, color_discrete_sequence=["#8E44AD"],
            )
            fig_ret.update_layout(xaxis_title="", yaxis_title="Retention (%)",
                                  plot_bgcolor="rgba(0,0,0,0)")
            st.plotly_chart(fig_ret, use_container_width=True)
        with col4:
            fig_dur = px.bar(
                df_pd, x="date", y="average_session_duration_sec",
                title="⏱️ Thời gian ở lại trung bình (giây)",
                color="average_session_duration_sec",
                color_continuous_scale="Blues",
            )
            fig_dur.update_layout(xaxis_title="", yaxis_title="Giây",
                                  plot_bgcolor="rgba(0,0,0,0)")
            st.plotly_chart(fig_dur, use_container_width=True)

        # ── Chart 4: Page views trend ───────────────────────────────────────
        fig_pv = px.area(
            df_pd, x="date", y="total_page_views",
            title="📄 Lượt xem trang theo ngày",
            color_discrete_sequence=["#F39C12"],
            line_shape="spline",
        )
        fig_pv.update_layout(plot_bgcolor="rgba(0,0,0,0)", xaxis_title="")
        st.plotly_chart(fig_pv, use_container_width=True)

        with st.expander("🔍 Dữ liệu thô"):
            st.dataframe(df_pd, use_container_width=True)

# ═══════════════════════════════════════════════════════════════════════════════
# Tab 7: Hiệu năng Chatbot — đầy đủ
# ═══════════════════════════════════════════════════════════════════════════════
with tab7:
    st.subheader("🤖 Phân tích Hiệu năng Chatbot & Câu hỏi")

    try:
        chat_perf_df = load_parquet("yhct-gold", "gold/mongodb/gold_chat_performance.parquet")
    except Exception as e:
        st.error(f"Lỗi load dữ liệu: {e}")
        chat_perf_df = pl.DataFrame()

    if chat_perf_df.is_empty():
        st.info("Chưa có dữ liệu hiệu năng chatbot.")
    else:
        df_pd = chat_perf_df.to_pandas()

        # ── KPIs ────────────────────────────────────────────────────────────
        total_sessions = len(df_pd)
        avg_latency    = df_pd["average_latency_ms"].mean()
        avg_rating     = df_pd["feedback_rating"].dropna().mean()
        avg_msgs       = df_pd["total_messages_exchanged"].mean()
        max_msgs       = int(df_pd["total_messages_exchanged"].max())
        zero_rate      = df_pd["is_zero_query_ratio"].mean() * 100
        total_msgs     = int(df_pd["total_messages_exchanged"].sum())

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Tổng phiên chat",       f"{total_sessions:,}")
        c2.metric("Tổng câu hỏi",          f"{total_msgs:,}")
        c3.metric("TB câu hỏi/phiên",      f"{avg_msgs:.1f}")
        c4.metric("Nhiều nhất/phiên",      f"{max_msgs}")

        c5, c6, c7, c8 = st.columns(4)
        c5.metric("Độ trễ TB (ms)",        f"{avg_latency:.0f}" if pd.notna(avg_latency) else "N/A")
        c6.metric("Đánh giá TB",           f"{avg_rating:.1f} ⭐" if pd.notna(avg_rating) else "Chưa có")
        c7.metric("Tỷ lệ 0 kết quả",      f"{zero_rate:.1f}%")
        c8.metric("Phiên có đánh giá",     f"{df_pd['feedback_rating'].notna().sum():,}")

        st.markdown("---")

        # ── Chart 1: Số câu hỏi/phiên + phân bố latency ────────────────────
        col1, col2 = st.columns(2)
        with col1:
            fig_msgs = px.histogram(
                df_pd, x="total_messages_exchanged", nbins=25,
                title="❓ Phân bố số câu hỏi/phiên chat",
                color_discrete_sequence=["#E67E22"],
                labels={"total_messages_exchanged": "Số câu hỏi", "count": "Số phiên"},
            )
            fig_msgs.update_layout(plot_bgcolor="rgba(0,0,0,0)", bargap=0.05)
            st.plotly_chart(fig_msgs, use_container_width=True)
        with col2:
            fig_lat = px.histogram(
                df_pd, x="average_latency_ms", nbins=30,
                title="⏱️ Phân bố độ trễ phản hồi (ms)",
                color_discrete_sequence=["#9B59B6"],
                marginal="box",
                labels={"average_latency_ms": "Độ trễ (ms)", "count": "Số phiên"},
            )
            fig_lat.update_layout(plot_bgcolor="rgba(0,0,0,0)")
            st.plotly_chart(fig_lat, use_container_width=True)

        # ── Chart 2: Sessions per day + rating distribution ─────────────────
        col3, col4 = st.columns([3, 2])
        with col3:
            if "session_start_time" in df_pd.columns:
                df_pd["date"] = pd.to_datetime(df_pd["session_start_time"], errors="coerce").dt.strftime("%Y-%m-%d")
                sessions_by_day = (
                    df_pd.groupby("date")
                    .agg(sessions=("session_id", "count"), total_msgs=("total_messages_exchanged", "sum"))
                    .reset_index()
                )
                fig_sd = go.Figure()
                fig_sd.add_trace(go.Bar(
                    x=sessions_by_day["date"], y=sessions_by_day["sessions"],
                    name="Phiên chat", marker_color="#3498DB",
                ))
                fig_sd.add_trace(go.Scatter(
                    x=sessions_by_day["date"], y=sessions_by_day["total_msgs"],
                    name="Tổng câu hỏi", mode="lines+markers",
                    line=dict(color="#E74C3C", width=2), yaxis="y2",
                ))
                fig_sd.update_layout(
                    title="📅 Phiên chat & câu hỏi theo ngày",
                    xaxis_title="", yaxis=dict(title="Số phiên"),
                    yaxis2=dict(title="Số câu hỏi", overlaying="y", side="right"),
                    plot_bgcolor="rgba(0,0,0,0)", legend=dict(x=0, y=1.1, orientation="h"),
                )
                st.plotly_chart(fig_sd, use_container_width=True)
        with col4:
            rating_counts = chat_perf_df.group_by("feedback_rating").agg(pl.len().alias("count")).drop_nulls()
            if not rating_counts.is_empty():
                st.plotly_chart(px.pie(
                    rating_counts.to_pandas(), names="feedback_rating", values="count",
                    title="⭐ Phân bố đánh giá (Sao)", hole=0.4,
                    color_discrete_sequence=["#F1C40F","#D4AC0D","#E67E22","#D35400","#CB4335"],
                ), use_container_width=True)

        # ── Chart 3: Zero-result rate trend ────────────────────────────────
        if "date" in df_pd.columns:
            zero_trend = (
                df_pd.groupby("date")["is_zero_query_ratio"]
                .mean().reset_index()
                .rename(columns={"is_zero_query_ratio": "zero_rate"})
            )
            zero_trend["zero_rate"] *= 100
            fig_zero = px.line(
                zero_trend, x="date", y="zero_rate",
                title="⚠️ Tỷ lệ câu hỏi không có kết quả theo ngày (%)",
                markers=True, color_discrete_sequence=["#E74C3C"],
            )
            fig_zero.add_hline(y=20, line_dash="dash", line_color="orange",
                               annotation_text="Ngưỡng 20%")
            fig_zero.update_layout(yaxis_title="Zero-result (%)", xaxis_title="",
                                   plot_bgcolor="rgba(0,0,0,0)")
            st.plotly_chart(fig_zero, use_container_width=True)

        # ── Top users by engagement ─────────────────────────────────────────
        st.subheader("🏆 Top người dùng theo số câu hỏi")
        top_users = (
            df_pd[df_pd["user_id"].notna()]
            .groupby("user_id")
            .agg(sessions=("session_id", "count"), total_qs=("total_messages_exchanged", "sum"),
                 avg_latency=("average_latency_ms", "mean"))
            .sort_values("total_qs", ascending=False)
            .head(10)
            .reset_index()
        )
        if not top_users.empty:
            top_users["avg_latency"] = top_users["avg_latency"].round(0).astype(int)
            top_users.columns = ["User ID", "Số phiên", "Tổng câu hỏi", "Độ trễ TB (ms)"]
            st.dataframe(top_users, use_container_width=True, hide_index=True)
            st.page_link("pages/2_👥_Quản_lý_người_dùng.py",
                         label="→ Xem chi tiết người dùng", icon="👥")

        with st.expander("🔍 Dữ liệu thô"):
            st.dataframe(df_pd, use_container_width=True)

# ═══════════════════════════════════════════════════════════════════════════════
# Tab 8: Xu hướng Dịch tễ — đầy đủ
# ═══════════════════════════════════════════════════════════════════════════════
with tab8:
    st.subheader("🩺 Xu hướng sức khỏe & Những gì người dùng quan tâm nhất")

    try:
        medical_df = load_parquet("yhct-gold", "gold/mongodb/gold_medical_insights.parquet")
    except Exception as e:
        st.error(f"Lỗi load dữ liệu: {e}")
        medical_df = pl.DataFrame()

    if medical_df.is_empty():
        st.info("Chưa có dữ liệu dịch tễ học.")
    else:
        df_pd = medical_df.to_pandas()

        symptoms_list   = _parse_json_list_column(medical_df["symptoms_list"])
        diseases_list   = _parse_json_list_column(medical_df["diseases_list"])
        herbs_queried   = _parse_json_list_column(medical_df["herbs_list"])
        body_parts_list = _parse_json_list_column(medical_df["body_parts_list"])

        # ── KPIs ────────────────────────────────────────────────────────────
        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("Tổng ca ghi nhận",        f"{len(df_pd):,}")
        c2.metric("Loại triệu chứng",         f"{len(set(symptoms_list)):,}" if symptoms_list else "0")
        c3.metric("Loại bệnh",                f"{len(set(diseases_list)):,}" if diseases_list else "0")
        c4.metric("Dược liệu được hỏi",       f"{len(set(herbs_queried)):,}" if herbs_queried else "0")
        top_city = df_pd["user_city"].dropna().mode()
        c5.metric("Khu vực phổ biến",         top_city.iloc[0] if not top_city.empty else "N/A")

        st.markdown("---")

        # ── 4 biểu đồ chính (2×2) ───────────────────────────────────────────
        col1, col2 = st.columns(2)

        with col1:
            # Triệu chứng phổ biến nhất
            if symptoms_list:
                sym_counts = (pd.Series(symptoms_list).value_counts()
                              .head(15).reset_index())
                sym_counts.columns = ["Triệu chứng", "Số lần"]
                fig_sym = px.bar(
                    sym_counts, x="Số lần", y="Triệu chứng", orientation="h",
                    title="🦠 Top 15 triệu chứng người dùng quan tâm nhất",
                    color="Số lần", color_continuous_scale="Reds",
                )
                fig_sym.update_layout(yaxis=dict(autorange="reversed"),
                                      plot_bgcolor="rgba(0,0,0,0)")
                st.plotly_chart(fig_sym, use_container_width=True)
            else:
                st.info("Chưa có dữ liệu triệu chứng.")

            # Bộ phận cơ thể
            if body_parts_list:
                bp_counts = (pd.Series(body_parts_list).value_counts()
                             .head(10).reset_index())
                bp_counts.columns = ["Bộ phận", "Số lần"]
                fig_bp = px.bar(
                    bp_counts, x="Số lần", y="Bộ phận", orientation="h",
                    title="🫁 Bộ phận cơ thể được đề cập",
                    color="Số lần", color_continuous_scale="Blues",
                )
                fig_bp.update_layout(yaxis=dict(autorange="reversed"),
                                     plot_bgcolor="rgba(0,0,0,0)")
                st.plotly_chart(fig_bp, use_container_width=True)

        with col2:
            # Bệnh phổ biến nhất
            if diseases_list:
                dis_counts = (pd.Series(diseases_list).value_counts()
                              .head(15).reset_index())
                dis_counts.columns = ["Bệnh", "Số lần"]
                fig_dis = px.bar(
                    dis_counts, x="Số lần", y="Bệnh", orientation="h",
                    title="🏥 Top 15 bệnh người dùng hỏi nhiều nhất",
                    color="Số lần", color_continuous_scale="Oranges",
                )
                fig_dis.update_layout(yaxis=dict(autorange="reversed"),
                                      plot_bgcolor="rgba(0,0,0,0)")
                st.plotly_chart(fig_dis, use_container_width=True)
            else:
                st.info("Chưa có dữ liệu bệnh lý.")

            # Dược liệu người dùng hỏi
            if herbs_queried:
                herb_counts = (pd.Series(herbs_queried).value_counts()
                               .head(10).reset_index())
                herb_counts.columns = ["Dược liệu", "Số lần"]
                fig_herb_q = px.bar(
                    herb_counts, x="Số lần", y="Dược liệu", orientation="h",
                    title="🌿 Dược liệu người dùng quan tâm nhất",
                    color="Số lần", color_continuous_scale="Greens",
                )
                fig_herb_q.update_layout(yaxis=dict(autorange="reversed"),
                                         plot_bgcolor="rgba(0,0,0,0)")
                st.plotly_chart(fig_herb_q, use_container_width=True)

        st.markdown("---")

        # ── Xu hướng theo thời gian ─────────────────────────────────────────
        if "timestamp" in df_pd.columns:
            df_pd["date"] = pd.to_datetime(df_pd["timestamp"], errors="coerce").dt.strftime("%Y-%m-%d")
            cases_by_day  = df_pd.groupby("date").size().reset_index(name="Số ca")
            if not cases_by_day.empty:
                fig_trend = px.area(
                    cases_by_day, x="date", y="Số ca",
                    title="📅 Xu hướng số ca ghi nhận theo ngày",
                    color_discrete_sequence=["#E74C3C"], line_shape="spline",
                )
                fig_trend.update_layout(plot_bgcolor="rgba(0,0,0,0)", xaxis_title="")
                st.plotly_chart(fig_trend, use_container_width=True)

        # ── Nhân khẩu học ───────────────────────────────────────────────────
        col5, col6, col7 = st.columns(3)
        with col5:
            gender_counts = df_pd["user_gender"].dropna().value_counts().reset_index()
            gender_counts.columns = ["Giới tính", "Số lượng"]
            if not gender_counts.empty:
                st.plotly_chart(px.pie(
                    gender_counts, names="Giới tính", values="Số lượng",
                    title="👥 Cơ cấu giới tính", hole=0.4,
                    color_discrete_sequence=["#3498DB","#E74C3C","#95A5A6"],
                ), use_container_width=True)
        with col6:
            city_counts = df_pd["user_city"].dropna().value_counts().head(8).reset_index()
            city_counts.columns = ["Thành phố", "Số lượng"]
            if not city_counts.empty:
                st.plotly_chart(px.bar(
                    city_counts, x="Thành phố", y="Số lượng",
                    title="🗺️ Phân bố theo thành phố",
                    color="Số lượng", color_continuous_scale="Purples",
                ), use_container_width=True)
        with col7:
            if "user_age" in df_pd.columns:
                age_data = df_pd["user_age"].dropna()
                if not age_data.empty:
                    st.plotly_chart(px.histogram(
                        df_pd, x="user_age", nbins=20,
                        title="🎂 Phân bố độ tuổi",
                        color_discrete_sequence=["#1ABC9C"],
                        labels={"user_age": "Tuổi", "count": "Số người"},
                    ), use_container_width=True)

        with st.expander("🔍 Dữ liệu thô"):
            st.dataframe(df_pd, use_container_width=True)
