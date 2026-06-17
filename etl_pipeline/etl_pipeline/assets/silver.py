# etl_pipeline/etl_pipeline/assets/silver.py
#
# Silver layer — lọc trang YHCT liên quan từ Bronze.
# Engine: Apache Spark (kết nối spark://spark-master:7077).
# Fallback về Polars nếu PySpark chưa khả dụng.
#
# Tại sao dùng Spark cho bước này?
#   - Số trang text có thể lên đến hàng chục nghìn khi thêm nhiều sách.
#   - Spark cho phép xử lý song song trên nhiều worker (horizontal scaling).
#   - UDF-based filtering chạy distributed thay vì sequential Python loop.

import os
from datetime import datetime

import polars as pl
from dagster import AssetIn, MetadataValue, Output, asset

# ── Optional PySpark import ───────────────────────────────────────────────────
try:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, lit, udf
    from pyspark.sql.types import StringType
    # Lỗi executor crash loop trên worker vì thiếu thư viện Python đồng bộ
    # Tạm thời vô hiệu hóa PySpark, sử dụng Polars cực nhanh thay thế.
    PYSPARK_AVAILABLE = False
except ImportError:
    PYSPARK_AVAILABLE = False


SPARK_MASTER = os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077")


# ── Keywords YHCT — trang phải chứa ít nhất 1 trong các keyword này ──────────
YHCT_KEYWORDS = [
    # Thuốc & bài thuốc
    "thuốc", "vị thuốc", "dược", "bài thuốc", "thang",
    "cây thuốc", "dược liệu", "sắc", "uống", "liều",
    "phương", "hoàn", "tán", "cao", "đan",
    # Tạng phủ YHCT
    "tỳ", "can", "thận", "phế", "tâm",
    "vị", "đởm", "bàng quang", "tiểu tràng", "đại tràng",
    # Bát cương
    "hàn", "nhiệt", "hư", "thực", "âm", "dương",
    "biểu", "lý", "khí", "huyết", "đàm",
    # Điều trị
    "châm", "cứu", "chữa", "trị", "điều trị",
    "pháp trị", "bổ", "tả", "thanh", "ôn",
    # Bệnh tiêu hóa
    "tiêu hóa", "dạ dày", "ruột", "gan", "mật",
    "táo bón", "tiêu chảy", "đau bụng", "buồn nôn",
    "đầy bụng", "chướng bụng", "ợ chua", "nôn mửa",
    "thượng vị", "đại tràng", "viêm gan", "xơ gan",
    # Tên vị thuốc phổ biến
    "cam thảo", "đương quy", "hoàng kỳ", "bạch truật",
    "phục linh", "sinh địa", "thục địa", "hoàng liên",
    "sài hồ", "bán hạ", "trần bì", "nhân sâm",
    # Thuật ngữ bào chế
    "sắc uống", "tán bột", "ngâm rượu", "sao vàng",
    "liều dùng", "cách dùng", "chỉ định", "chống chỉ định",
    # Bệnh học YHCT
    "biện chứng", "luận trị", "nguyên nhân bệnh",
    "triệu chứng", "lâm sàng", "phân thể",
]

# ── Trang chứa các từ này → loại bỏ ─────────────────────────────────────────
STOPWORDS = [
    "mục lục",
    "tài liệu tham khảo",
    "bibliography",
    "contents",
    "lời nói đầu",
    "lưu hành nội bộ",
    "ban hành kèm theo",
    "quyết định số",
    "chương trình đào tạo",
]

# Trang quá ngắn (ít hơn N từ) → loại bỏ
MIN_WORDS = 40


# ── Hàm lọc thuần Python — dùng cho cả Polars fallback và Spark UDF ──────────
def _classify_page(text: str) -> str:
    """
    Trả về '' nếu trang hợp lệ YHCT.
    Trả về lý do nếu bị loại ('too_short', 'stopword:...', 'no_yhct_keyword').
    """
    if not text:
        return "empty"
    if len(text.split()) < MIN_WORDS:
        return "too_short"
    low = text.lower()
    for sw in STOPWORDS:
        if sw in low:
            return f"stopword:{sw}"
    if not any(kw in low for kw in YHCT_KEYWORDS):
        return "no_yhct_keyword"
    return ""


# ── Spark processing ──────────────────────────────────────────────────────────

def _get_spark(context) -> "SparkSession":
    """Tạo SparkSession kết nối đến cluster, fallback về local nếu lỗi."""
    try:
        spark = (
            SparkSession.builder
            .appName("YHCT-Silver-Filtering")
            .master(SPARK_MASTER)
            .config("spark.driver.memory",              "1g")
            .config("spark.executor.memory",            "1g")
            .config("spark.sql.shuffle.partitions",     "4")
            .config("spark.driver.bindAddress",         "0.0.0.0")
            .config("spark.driver.host",                "etl_pipeline")
            .getOrCreate()
        )
        context.log.info(f"⚡ Spark kết nối thành công: {SPARK_MASTER}")
        return spark
    except Exception as e:
        context.log.warning(f"⚠️  Không thể kết nối {SPARK_MASTER}: {e}")
        context.log.info("⚡ Fallback → Spark local[*]")
        return (
            SparkSession.builder
            .appName("YHCT-Silver-Filtering-Local")
            .master("local[*]")
            .config("spark.driver.memory",          "1g")
            .config("spark.sql.shuffle.partitions", "4")
            .getOrCreate()
        )


def _filter_with_spark(
    bronze_df: pl.DataFrame, context
) -> tuple[pl.DataFrame, dict, str]:
    """
    Lọc bằng Spark distributed processing.
    Trả về (silver_df, filter_stats, spark_master_used).
    """
    spark = _get_spark(context)
    master_used = spark.sparkContext.master

    # Spark UDF — closure capture YHCT_KEYWORDS + STOPWORDS + MIN_WORDS
    _kws  = YHCT_KEYWORDS
    _sws  = STOPWORDS
    _minw = MIN_WORDS

    @udf(returnType=StringType())
    def classify_udf(text: str) -> str:
        if not text:
            return "empty"
        if len(text.split()) < _minw:
            return "too_short"
        low = text.lower()
        for sw in _sws:
            if sw in low:
                return f"stopword:{sw}"
        if not any(kw in low for kw in _kws):
            return "no_yhct_keyword"
        return ""

    context.log.info(
        f"⚡ Spark [{master_used}]: phân tán {bronze_df.shape[0]} trang "
        f"ra các worker để lọc..."
    )

    # Polars → Pandas → Spark DataFrame
    spark_df = spark.createDataFrame(bronze_df.to_pandas())

    # Thêm cột filter_reason bằng UDF (chạy distributed trên worker)
    classified = spark_df.withColumn("filter_reason", classify_udf(col("page_text")))

    # Tính stats trước khi filter (lazy evaluation — 1 pass duy nhất)
    classified.cache()

    stats_rows = (
        classified
        .filter(col("filter_reason") != "")
        .groupBy("filter_reason")
        .count()
        .collect()
    )
    filter_stats = {row["filter_reason"]: row["count"] for row in stats_rows}

    # Giữ lại trang pass filter
    kept_spark = classified.filter(col("filter_reason") == "")

    # Spark → Pandas → Polars
    kept_pandas = kept_spark.toPandas()
    classified.unpersist()
    spark.stop()

    if kept_pandas.empty:
        raise ValueError("Silver (Spark): không có trang nào pass filter!")

    silver_df = pl.from_pandas(kept_pandas)
    silver_df = silver_df.with_columns(
        pl.lit(datetime.utcnow()).alias("silver_time"),
        pl.lit(False).alias("is_filtered"),
    )
    return silver_df, filter_stats, master_used


def _filter_with_polars(bronze_df: pl.DataFrame, context) -> tuple[pl.DataFrame, dict]:
    """Fallback: lọc bằng Polars (single-node) khi PySpark chưa khả dụng."""
    context.log.warning("⚠️  PySpark chưa cài — dùng Polars fallback.")
    kept_rows    = []
    filter_stats = {}

    for row in bronze_df.iter_rows(named=True):
        reason = _classify_page(row["page_text"])
        if reason == "":
            kept_rows.append({
                **row,
                "is_filtered":   False,
                "filter_reason": "",
                "silver_time":   datetime.utcnow(),
            })
        else:
            filter_stats[reason] = filter_stats.get(reason, 0) + 1

    if not kept_rows:
        raise ValueError("Silver: không có trang nào pass filter!")

    return pl.DataFrame(kept_rows), filter_stats


# ══════════════════════════════════════════════════════════════════════════════
# DAGSTER ASSET
# ══════════════════════════════════════════════════════════════════════════════

@asset(
    name="silver_filtered_pages",
    key_prefix=["silver", "pdf"],
    group_name="silver",
    io_manager_key="minio_io_manager",
    compute_kind="spark" if PYSPARK_AVAILABLE else "python",
    ins={"bronze_pdf_pages": AssetIn(key_prefix=["bronze", "pdf"])},
    description=(
        "Lọc trang YHCT liên quan từ Bronze → Silver. "
        "Engine: Apache Spark (distributed UDF filtering). "
        "Fallback về Polars nếu PySpark chưa cài."
    ),
)
def silver_filtered_pages(context, bronze_pdf_pages: pl.DataFrame) -> Output:
    context.log.info(f"📥 Nhận {bronze_pdf_pages.shape[0]} trang từ Bronze")

    # Log input stats theo file
    file_counts = (
        bronze_pdf_pages.group_by("source_file")
        .agg(pl.len().alias("pages"))
        .sort("source_file")
    )
    for row in file_counts.iter_rows(named=True):
        context.log.info(f"   └─ {row['source_file']}: {row['pages']} trang")

    # ── Chọn engine xử lý ────────────────────────────────────────────────────
    if PYSPARK_AVAILABLE:
        silver_df, filter_stats, spark_master = _filter_with_spark(bronze_pdf_pages, context)
        engine = f"spark ({spark_master})"
    else:
        silver_df, filter_stats = _filter_with_polars(bronze_pdf_pages, context)
        engine = "polars (fallback)"

    total_removed = bronze_pdf_pages.shape[0] - silver_df.shape[0]

    # Log kết quả
    context.log.info(f"\n{'='*50}")
    context.log.info(f"✅ TỔNG KẾT SILVER [{engine}]:")
    context.log.info(f"   Giữ lại: {silver_df.shape[0]} trang")
    context.log.info(f"   Loại bỏ: {total_removed} trang")
    for reason, count in sorted(filter_stats.items(), key=lambda x: x[1], reverse=True):
        context.log.info(f"   └─ {reason}: {count} trang")

    # Stats theo file
    file_kept = (
        silver_df.group_by("source_file")
        .agg(pl.len().alias("pages_kept"))
        .sort("source_file")
    )
    for row in file_kept.iter_rows(named=True):
        context.log.info(f"   Giữ [{row['source_file']}]: {row['pages_kept']} trang")

    preview_df = silver_df.select(
        ["source_file", "doc_id", "page_num", "word_count"]
    ).head(8)

    return Output(
        value=silver_df,
        metadata={
            "engine":         MetadataValue.text(engine),
            "total_input":    MetadataValue.int(bronze_pdf_pages.shape[0]),
            "total_kept":     MetadataValue.int(silver_df.shape[0]),
            "total_removed":  MetadataValue.int(total_removed),
            "filter_stats":   MetadataValue.json(filter_stats),
            "kept_by_file":   MetadataValue.md(
                file_kept.to_pandas().to_markdown()
            ),
            "preview":        MetadataValue.md(
                preview_df.to_pandas().to_markdown()
            ),
        },
    )
