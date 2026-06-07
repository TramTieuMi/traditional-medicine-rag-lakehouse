# streamlit_app/pages/2_analytics.py

import os
from io import BytesIO

import httpx
import plotly.express as px
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


# ── Dagster helpers ───────────────────────────────────────────────────────────
def _discover_repo_info() -> tuple[str, str, str] | None:
    """
    Query Dagster GraphQL để tự động phát hiện location, repo, và job name.
    Trả về (location_name, repo_name, job_name) hoặc None nếu lỗi.

    Schema Dagster dùng: repositoriesOrError → RepositoryConnection
    (không phải repositoryLocationsOrError — field đó không tồn tại trong phiên bản này)
    """
    query = """
    query {
      repositoriesOrError {
        ... on RepositoryConnection {
          nodes {
            name
            location { name }
            pipelines { name }
          }
        }
        ... on PythonError { message }
      }
    }
    """
    try:
        r = httpx.post(f"{DAGSTER_URL}/graphql", json={"query": query}, timeout=10)
        r.raise_for_status()
        data = r.json()
        nodes = data["data"]["repositoriesOrError"].get("nodes", [])

        if not nodes:
            return None

        repo_name = nodes[0]["name"]
        loc_name  = nodes[0]["location"]["name"]
        pipelines = [p["name"] for p in nodes[0].get("pipelines", [])]

        # Ưu tiên __ASSET_JOB, fallback sang job đầu tiên tìm được
        job_name = "__ASSET_JOB" if "__ASSET_JOB" in pipelines else (pipelines[0] if pipelines else "__ASSET_JOB")

        return loc_name, repo_name, job_name

    except Exception:
        return None


def _launch_pipeline(loc_name: str, repo_name: str, job_name: str) -> tuple[str | None, str | None]:
    """
    Kích hoạt Dagster pipeline để materialize toàn bộ assets.
    Trả về (run_id, error_message).
    """
    mutation = """
    mutation LaunchRun($executionParams: ExecutionParams!) {
      launchRun(executionParams: $executionParams) {
        ... on LaunchRunSuccess {
          run { runId status }
        }
        ... on PipelineNotFoundError { message }
        ... on InvalidSubsetError    { message }
        ... on PythonError           { message }
      }
    }
    """
    variables = {
        "executionParams": {
            "selector": {
                "repositoryLocationName": loc_name,
                "repositoryName":         repo_name,
                "jobName":                job_name,
            },
            "executionMetadata": {},
            "runConfigData":     "{}",
        }
    }
    try:
        r = httpx.post(
            f"{DAGSTER_URL}/graphql",
            json={"query": mutation, "variables": variables},
            timeout=30,
        )
        r.raise_for_status()
        result = r.json()["data"]["launchRun"]

        if "run" in result:
            return result["run"]["runId"], None
        elif "message" in result:
            return None, result["message"]

    except Exception as e:
        return None, str(e)

    return None, "Unknown error"


def _get_run_status(run_id: str) -> str:
    """Lấy trạng thái run hiện tại từ Dagster."""
    query = """
    query RunStatus($runId: ID!) {
      runOrError(runId: $runId) {
        ... on Run          { runId status }
        ... on RunNotFoundError { message }
        ... on PythonError      { message }
      }
    }
    """
    try:
        r = httpx.post(
            f"{DAGSTER_URL}/graphql",
            json={"query": query, "variables": {"runId": run_id}},
            timeout=10,
        )
        r.raise_for_status()
        run = r.json()["data"]["runOrError"]
        return run.get("status", "UNKNOWN")
    except Exception:
        return "UNKNOWN"


def _upload_to_minio(filename: str, data: bytes) -> None:
    """Upload PDF lên MinIO bucket yhct-docs để dùng cho citation links."""
    client = Minio(MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, secure=False)
    if not client.bucket_exists("yhct-docs"):
        client.make_bucket("yhct-docs")
    client.put_object(
        "yhct-docs", filename,
        BytesIO(data),
        length=len(data),
        content_type="application/pdf",
    )


# ── Tabs ─────────────────────────────────────────────────────────────────────
st.title("📊 YHCT Analytics Dashboard")

tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "🌿 Dược liệu",
    "🫀 Tạng phủ",
    "📄 Chunks",
    "📚 Nguồn tài liệu",
    "📥 Thêm tài liệu",
])

# ── Tab 1: Herb mentions ──────────────────────────────────────────────────────
with tab1:
    st.subheader("Top dược liệu xuất hiện nhiều nhất")
    try:
        herb_df = load_parquet("yhct-gold", "gold/herbs/gold_herb_mentions.parquet")
        top_herbs = (
            herb_df.group_by("herb_name")
            .agg(pl.sum("count_in_chunk").alias("total"))
            .sort("total", descending=True)
            .head(20)
        )
        fig = px.bar(
            top_herbs.to_pandas(),
            x="total", y="herb_name",
            orientation="h",
            color="total",
            color_continuous_scale="Greens",
            title="Top 20 dược liệu trong tài liệu YHCT",
            labels={"total": "Số lần xuất hiện", "herb_name": "Dược liệu"}
        )
        fig.update_layout(height=600, yaxis=dict(autorange="reversed"))
        st.plotly_chart(fig, use_container_width=True)

        st.subheader("Bảng chi tiết")
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
            "ty_vi":          "Tỳ Vị (Tiêu hóa)",
            "can_dom":        "Can Đởm (Gan Mật)",
            "than":           "Thận (Bàng quang)",
            "phe_dai_trang":  "Phế Đại tràng",
            "tam_tieu_trang": "Tâm Tiểu tràng",
        }
        tp_pd = tp_counts.to_pandas()
        tp_pd["tang_phu_label"] = tp_pd["tang_phu"].map(TANG_PHU_LABELS)

        col1, col2 = st.columns(2)
        with col1:
            fig_pie = px.pie(
                tp_pd, values="count", names="tang_phu_label",
                title="Tỷ lệ đề cập tạng phủ",
                color_discrete_sequence=px.colors.qualitative.Set2
            )
            st.plotly_chart(fig_pie, use_container_width=True)
        with col2:
            fig_bar = px.bar(
                tp_pd, x="tang_phu_label", y="count",
                color="count", color_continuous_scale="Teal",
                title="Số chunk đề cập theo tạng phủ",
                labels={"count": "Số chunks", "tang_phu_label": "Tạng phủ"}
            )
            st.plotly_chart(fig_bar, use_container_width=True)

        st.dataframe(tp_pd, use_container_width=True)
    except Exception as e:
        st.error(f"Lỗi load dữ liệu: {e}")

# ── Tab 3: Chunks stats ───────────────────────────────────────────────────────
with tab3:
    st.subheader("Thống kê chunks")
    try:
        chunk_df = load_parquet("yhct-gold", "gold/chunks/gold_yhct_chunks.parquet")

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Tổng chunks",    f"{chunk_df.shape[0]:,}")
        c2.metric("Tổng trang",     f"{chunk_df['page_num'].n_unique():,}")
        c3.metric("TB từ/chunk",    f"{chunk_df['word_count'].mean():.0f}")
        c4.metric("Số nguồn tài liệu",
                  f"{chunk_df['source_file'].n_unique():,}")

        fig_hist = px.histogram(
            chunk_df.to_pandas(), x="word_count",
            nbins=40, color_discrete_sequence=["#2d9e5f"],
            title="Phân bố số từ trong mỗi chunk",
            labels={"word_count": "Số từ", "count": "Số chunks"}
        )
        st.plotly_chart(fig_hist, use_container_width=True)

        by_source = (
            chunk_df.group_by("source_file")
            .agg(pl.len().alias("chunks"))
            .sort("chunks", descending=True)
        )
        fig_src = px.bar(
            by_source.to_pandas(),
            x="source_file", y="chunks",
            color="chunks",
            color_continuous_scale="Blues",
            title="Số chunks theo nguồn tài liệu",
        )
        fig_src.update_xaxes(tickangle=15)
        st.plotly_chart(fig_src, use_container_width=True)

    except Exception as e:
        st.error(f"Lỗi load dữ liệu: {e}")

# ── Tab 4: Nguồn tài liệu ────────────────────────────────────────────────────
with tab4:
    st.subheader("📚 Danh sách tài liệu trong hệ thống")
    try:
        bronze_df = load_parquet("yhct-bronze", "bronze/pdf/bronze_pdf_pages.parquet")
        source_stats = (
            bronze_df.group_by(["source_file", "doc_id"])
            .agg([
                pl.len().alias("total_pages"),
                pl.col("word_count").sum().alias("total_words"),
            ])
            .sort("source_file")
        )

        for row in source_stats.iter_rows(named=True):
            with st.expander(f"📖 {row['source_file']}"):
                col1, col2, col3 = st.columns(3)
                col1.metric("Tổng trang",  row["total_pages"])
                col2.metric("Tổng từ",     f"{row['total_words']:,}")
                col3.metric("doc_id",      row["doc_id"])

    except Exception as e:
        st.error(f"Lỗi load dữ liệu: {e}")

# ── Tab 5: Upload PDF ─────────────────────────────────────────────────────────
with tab5:
    st.subheader("📥 Nhập tài liệu mới vào hệ thống")
    st.markdown(
        "Upload file PDF — hệ thống sẽ tự động chạy toàn bộ pipeline "
        "(Bronze → Silver → Gold → Embeddings) rồi thông báo khi xong."
    )

    col_upload, col_status = st.columns([1, 1])

    with col_upload:
        uploaded_file = st.file_uploader(
            "Chọn file PDF", type=["pdf"], key="pdf_upload",
            help="File sẽ được lưu vào kho dữ liệu và pipeline tự động chạy.",
        )

        if uploaded_file:
            file_bytes   = uploaded_file.getvalue()
            file_size_kb = len(file_bytes) // 1024
            st.info(f"**{uploaded_file.name}** · {file_size_kb:,} KB")

            already_exists = os.path.exists(
                os.path.join(RAW_DATA_DIR, uploaded_file.name)
            )
            if already_exists:
                st.warning(
                    f"File `{uploaded_file.name}` đã tồn tại trong kho. "
                    "Upload sẽ ghi đè và chạy lại pipeline."
                )

            if st.button("🚀 Nhập và xử lý", type="primary"):
                errors = []

                # 1. Lưu vào thư mục data/raw (shared với etl_pipeline container)
                with st.spinner("Đang lưu file..."):
                    try:
                        os.makedirs(RAW_DATA_DIR, exist_ok=True)
                        raw_path = os.path.join(RAW_DATA_DIR, uploaded_file.name)
                        with open(raw_path, "wb") as f:
                            f.write(file_bytes)
                        st.success(f"✅ Đã lưu: `{raw_path}`")
                    except Exception as e:
                        errors.append(f"Lưu file: {e}")
                        st.error(f"❌ Không lưu được file: {e}")

                # 2. Upload lên MinIO yhct-docs (cho citation links)
                with st.spinner("Đang upload lên MinIO..."):
                    try:
                        _upload_to_minio(uploaded_file.name, file_bytes)
                        st.success("✅ Đã upload lên MinIO (yhct-docs)")
                    except Exception as e:
                        st.warning(f"⚠ MinIO upload thất bại (không ảnh hưởng pipeline): {e}")

                # 3. Kích hoạt Dagster pipeline
                if not errors:
                    with st.spinner("Đang kích hoạt Dagster pipeline..."):
                        repo_info = _discover_repo_info()
                        if repo_info is None:
                            st.error(
                                "❌ Không kết nối được Dagster tại `"
                                + DAGSTER_URL + "`. "
                                "Kiểm tra container dagster đang chạy."
                            )
                        else:
                            loc_name, repo_name, job_name = repo_info
                            run_id, err = _launch_pipeline(loc_name, repo_name, job_name)

                            if run_id:
                                st.session_state.dagster_run_id       = run_id
                                st.session_state.dagster_uploaded_file = uploaded_file.name
                                st.session_state.dagster_loc          = loc_name
                                st.session_state.dagster_repo         = repo_name
                                st.session_state.dagster_job          = job_name
                                st.rerun()
                            else:
                                st.error(f"❌ Pipeline không khởi động được: {err}")

    with col_status:
        if "dagster_run_id" in st.session_state:
            run_id   = st.session_state.dagster_run_id
            filename = st.session_state.get("dagster_uploaded_file", "")
            status   = _get_run_status(run_id)

            STATUS_INFO = {
                "SUCCESS":     ("✅", "success"),
                "FAILURE":     ("❌", "error"),
                "QUEUED":      ("⏳", "info"),
                "NOT_STARTED": ("⏳", "info"),
                "STARTING":    ("🔄", "info"),
                "STARTED":     ("🔄", "info"),
                "MANAGED":     ("🔄", "info"),
                "CANCELING":   ("🛑", "warning"),
                "CANCELED":    ("🛑", "warning"),
            }
            icon, level = STATUS_INFO.get(status, ("❓", "info"))

            st.markdown(f"### {icon} Trạng thái pipeline")
            st.code(
                f"File   : {filename}\n"
                f"Run ID : {run_id}\n"
                f"Status : {status}\n"
                f"Dagster: {DAGSTER_URL}"
            )

            if status == "SUCCESS":
                load_parquet.clear()   # xóa cache để các tab analytics hiển thị dữ liệu mới
                st.success(
                    "🎉 Tài liệu đã được xử lý thành công!\n\n"
                    "Các tab Analytics đã được cập nhật. "
                    "Reload trang chatbot để bắt đầu hỏi về tài liệu mới."
                )
                if st.button("🗑 Xóa thông báo", key="clear_success"):
                    for key in ["dagster_run_id", "dagster_uploaded_file",
                                "dagster_loc", "dagster_repo", "dagster_job"]:
                        st.session_state.pop(key, None)
                    st.rerun()

            elif status in ("FAILURE", "CANCELED"):
                st.error(
                    f"❌ Pipeline {status.lower()}. "
                    f"Xem logs chi tiết tại Dagster UI: `{DAGSTER_URL}`"
                )
                if st.button("🗑 Xóa thông báo", key="clear_failure"):
                    for key in ["dagster_run_id", "dagster_uploaded_file",
                                "dagster_loc", "dagster_repo", "dagster_job"]:
                        st.session_state.pop(key, None)
                    st.rerun()

            else:
                st.info("Pipeline đang chạy... Bấm refresh để cập nhật trạng thái.")
                if st.button("🔄 Refresh trạng thái", key="refresh_status"):
                    load_parquet.clear()   # clear cache sẵn khi refresh
                    st.rerun()

        else:
            st.markdown("""
            <div style="padding:40px; text-align:center; color:#aaa; border:1px dashed #ccc; border-radius:12px;">
                <p style="font-size:32px; margin:0">📂</p>
                <p>Upload file PDF và bấm <strong>Nhập và xử lý</strong><br>để bắt đầu pipeline tự động.</p>
            </div>
            """, unsafe_allow_html=True)
