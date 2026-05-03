# streamlit_app/pages/2_analytics.py

import streamlit as st
import polars as pl
import plotly.express as px
from minio import Minio
from io import BytesIO
import os

st.set_page_config(page_title="YHCT Analytics", page_icon="📊", layout="wide")

MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT",   "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY",  "minio")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY",  "minio123")


@st.cache_data(ttl=300)
def load_parquet(bucket: str, key: str) -> pl.DataFrame:
    client = Minio(MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, secure=False)
    obj    = client.get_object(bucket, key)
    return pl.read_parquet(BytesIO(obj.read()))


st.title("📊 YHCT Analytics Dashboard")

tab1, tab2, tab3, tab4 = st.tabs([
    "🌿 Dược liệu", "🫀 Tạng phủ", "📄 Chunks", "📚 Nguồn tài liệu"
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
            .agg(pl.len().alias("count"))   # ← FIX: pl.count() → pl.len()
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

        # Chunks theo nguồn tài liệu
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