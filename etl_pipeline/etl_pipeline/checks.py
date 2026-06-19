# etl_pipeline/etl_pipeline/checks.py
#
# Asset Checks — validate chất lượng dữ liệu tại mỗi layer.
# Nguyên tắc:
#   ERROR   → dữ liệu sai nghiêm trọng, pipeline không nên tiếp tục
#   WARN    → bất thường nhưng không chặn pipeline, cần xem xét

import os
from io import BytesIO

import polars as pl
from dagster import AssetCheckResult, AssetCheckSeverity, MetadataValue, asset_check
from minio import Minio
from minio.error import S3Error

from .assets.bronze      import bronze_pdf_ingestion
from .assets.silver      import silver_filtered_pages
from .assets.gold_chunks import gold_yhct_chunks
from .assets.user_gold   import gold_user_engagement, gold_chat_performance, gold_medical_insights

MINIO_ENDPOINT   = "minio:9000"
MINIO_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID",     "minio")
MINIO_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "minio123")


def _read_parquet(bucket: str, key: str) -> pl.DataFrame:
    client = Minio(MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, secure=False)
    obj    = client.get_object(bucket, key)
    return pl.read_parquet(BytesIO(obj.read()))


def _not_found_result(check_name: str) -> AssetCheckResult:
    """Check result bỏ qua nhẹ nhàng khi Parquet chưa tồn tại (first run)."""
    return AssetCheckResult(
        passed=True,
        severity=AssetCheckSeverity.WARN,
        metadata={"status": MetadataValue.text("skipped — parquet not yet materialized")},
    )


def _error_result(e: Exception) -> AssetCheckResult:
    """Check result khi có lỗi thật (không phải file-not-found)."""
    return AssetCheckResult(
        passed=False,
        severity=AssetCheckSeverity.WARN,
        metadata={"error": MetadataValue.text(str(e))},
    )


# ══════════════════════════════════════════════════════════════════════════════
# BRONZE CHECKS
# ══════════════════════════════════════════════════════════════════════════════

@asset_check(
    asset=bronze_pdf_ingestion,
    name="bronze_has_data",
    description="Bronze phải có ít nhất 1 trang sau khi ingest.",
)
def check_bronze_has_data(context) -> AssetCheckResult:
    df = _read_parquet("yhct-bronze", "bronze/pdf/bronze_pdf_pages.parquet")
    return AssetCheckResult(
        passed=df.shape[0] > 0,
        severity=AssetCheckSeverity.ERROR,
        metadata={
            "total_pages":    MetadataValue.int(df.shape[0]),
            "unique_sources": MetadataValue.int(df["source_file"].n_unique()),
        },
    )


@asset_check(
    asset=bronze_pdf_ingestion,
    name="bronze_text_not_garbled",
    description="Trung bình từ/trang phải ≥ 20 — PDF không bị corrupt hay scan lỗi.",
)
def check_bronze_text_not_garbled(context) -> AssetCheckResult:
    df     = _read_parquet("yhct-bronze", "bronze/pdf/bronze_pdf_pages.parquet")
    avg_wc = float(df["word_count"].mean()) if df.shape[0] > 0 else 0.0
    return AssetCheckResult(
        passed=avg_wc >= 20,
        severity=AssetCheckSeverity.WARN,
        metadata={"avg_words_per_page": MetadataValue.float(round(avg_wc, 1))},
    )


@asset_check(
    asset=bronze_pdf_ingestion,
    name="bronze_no_duplicate_pages",
    description="Không được có (doc_id, page_num) trùng lặp trong Bronze.",
)
def check_bronze_no_duplicate_pages(context) -> AssetCheckResult:
    df     = _read_parquet("yhct-bronze", "bronze/pdf/bronze_pdf_pages.parquet")
    total  = df.shape[0]
    unique = df.select(["doc_id", "page_num"]).unique().shape[0]
    return AssetCheckResult(
        passed=total == unique,
        severity=AssetCheckSeverity.ERROR,
        metadata={
            "total_rows":  MetadataValue.int(total),
            "unique_rows": MetadataValue.int(unique),
            "duplicates":  MetadataValue.int(total - unique),
        },
    )


# ══════════════════════════════════════════════════════════════════════════════
# SILVER CHECKS
# ══════════════════════════════════════════════════════════════════════════════

@asset_check(
    asset=silver_filtered_pages,
    name="silver_filter_rate_reasonable",
    description=(
        "Tỷ lệ trang được giữ lại phải nằm trong 5%–95%. "
        "Lọc quá nhiều → keywords quá chặt. Lọc quá ít → keywords quá lỏng."
    ),
)
def check_silver_filter_rate(context) -> AssetCheckResult:
    bronze_df = _read_parquet("yhct-bronze", "bronze/pdf/bronze_pdf_pages.parquet")
    silver_df = _read_parquet("yhct-silver", "silver/pdf/silver_filtered_pages.parquet")

    if bronze_df.shape[0] == 0:
        return AssetCheckResult(
            passed=False,
            severity=AssetCheckSeverity.ERROR,
            metadata={"reason": MetadataValue.text("bronze is empty")},
        )

    keep_rate = silver_df.shape[0] / bronze_df.shape[0]
    return AssetCheckResult(
        passed=0.05 <= keep_rate <= 0.95,
        severity=AssetCheckSeverity.WARN,
        metadata={
            "bronze_pages":  MetadataValue.int(bronze_df.shape[0]),
            "silver_pages":  MetadataValue.int(silver_df.shape[0]),
            "keep_rate_pct": MetadataValue.float(round(keep_rate * 100, 1)),
        },
    )


@asset_check(
    asset=silver_filtered_pages,
    name="silver_no_empty_text",
    description="Không có trang nào có page_text rỗng trong Silver.",
)
def check_silver_no_empty_text(context) -> AssetCheckResult:
    df          = _read_parquet("yhct-silver", "silver/pdf/silver_filtered_pages.parquet")
    empty_count = df.filter(pl.col("page_text").str.len_chars() == 0).shape[0]
    return AssetCheckResult(
        passed=empty_count == 0,
        severity=AssetCheckSeverity.ERROR,
        metadata={"empty_text_pages": MetadataValue.int(empty_count)},
    )


# ══════════════════════════════════════════════════════════════════════════════
# GOLD CHECKS
# ══════════════════════════════════════════════════════════════════════════════

@asset_check(
    asset=gold_yhct_chunks,
    name="gold_chunk_size_in_range",
    description="Chunk size trung bình phải nằm trong 50–400 từ.",
)
def check_gold_chunk_size(context) -> AssetCheckResult:
    df = _read_parquet("yhct-gold", "gold/chunks/gold_yhct_chunks.parquet")
    if df.shape[0] == 0:
        return AssetCheckResult(
            passed=False,
            severity=AssetCheckSeverity.ERROR,
            metadata={"reason": MetadataValue.text("no chunks found")},
        )
    avg_wc = float(df["word_count"].mean())
    return AssetCheckResult(
        passed=50 <= avg_wc <= 400,
        severity=AssetCheckSeverity.WARN,
        metadata={
            "avg_word_count": MetadataValue.float(round(avg_wc, 1)),
            "min_word_count": MetadataValue.float(float(df["word_count"].min())),
            "max_word_count": MetadataValue.float(float(df["word_count"].max())),
            "total_chunks":   MetadataValue.int(df.shape[0]),
        },
    )


@asset_check(
    asset=gold_yhct_chunks,
    name="gold_no_duplicate_chunks",
    description="chunk_id phải unique — không được có embedding trùng lặp.",
)
def check_gold_no_duplicate_chunks(context) -> AssetCheckResult:
    df     = _read_parquet("yhct-gold", "gold/chunks/gold_yhct_chunks.parquet")
    total  = df.shape[0]
    unique = df["chunk_id"].n_unique()
    return AssetCheckResult(
        passed=total == unique,
        severity=AssetCheckSeverity.ERROR,
        metadata={
            "total_chunks":  MetadataValue.int(total),
            "unique_chunks": MetadataValue.int(unique),
            "duplicates":    MetadataValue.int(total - unique),
        },
    )


@asset_check(
    asset=gold_yhct_chunks,
    name="gold_coverage_per_source",
    description="Mỗi nguồn tài liệu phải đóng góp ít nhất 10 chunks.",
)
def check_gold_coverage_per_source(context) -> AssetCheckResult:
    df     = _read_parquet("yhct-gold", "gold/chunks/gold_yhct_chunks.parquet")
    by_src = (
        df.group_by("source_file")
        .agg(pl.len().alias("chunks"))
        .sort("chunks")
    )
    thin   = by_src.filter(pl.col("chunks") < 10)
    return AssetCheckResult(
        passed=thin.shape[0] == 0,
        severity=AssetCheckSeverity.WARN,
        metadata={
            "sources_below_10": MetadataValue.json(thin["source_file"].to_list()),
            "min_chunks":       MetadataValue.int(int(by_src["chunks"].min())),
            "max_chunks":       MetadataValue.int(int(by_src["chunks"].max())),
        },
    )


# ══════════════════════════════════════════════════════════════════════════════
# USER LAKEHOUSE GOLD CHECKS
# ══════════════════════════════════════════════════════════════════════════════

@asset_check(
    asset=gold_user_engagement,
    name="gold_engagement_no_null_date",
    description="Ngày trong bảng gold_user_engagement không được rỗng.",
)
def check_gold_engagement_no_nulls(context) -> AssetCheckResult:
    try:
        df = _read_parquet("yhct-gold", "gold/mongodb/gold_user_engagement.parquet")
        null_dates = df.filter(pl.col("date").is_null() | (pl.col("date") == "")).shape[0]
        return AssetCheckResult(
            passed=(null_dates == 0),
            severity=AssetCheckSeverity.ERROR,
            metadata={"null_dates_count": MetadataValue.int(null_dates)},
        )
    except S3Error as e:
        if e.code == "NoSuchKey":
            return _not_found_result("gold_engagement_no_null_date")
        return _error_result(e)
    except Exception as e:
        return _error_result(e)


@asset_check(
    asset=gold_chat_performance,
    name="gold_chat_rating_range",
    description="feedback_rating nếu được đánh giá thì phải nằm trong khoảng [1, 5].",
)
def check_gold_chat_rating_range(context) -> AssetCheckResult:
    try:
        df = _read_parquet("yhct-gold", "gold/mongodb/gold_chat_performance.parquet")
        invalid_ratings = df.filter(
            (pl.col("feedback_rating").is_not_null()) &
            ((pl.col("feedback_rating") < 1) | (pl.col("feedback_rating") > 5))
        ).shape[0]
        return AssetCheckResult(
            passed=(invalid_ratings == 0),
            severity=AssetCheckSeverity.ERROR,
            metadata={"invalid_ratings_count": MetadataValue.int(invalid_ratings)},
        )
    except S3Error as e:
        if e.code == "NoSuchKey":
            return _not_found_result("gold_chat_rating_range")
        return _error_result(e)
    except Exception as e:
        return _error_result(e)


@asset_check(
    asset=gold_medical_insights,
    name="gold_insights_valid_ids",
    description="log_id trong bảng gold_medical_insights phải là duy nhất và không được rỗng.",
)
def check_gold_insights_valid(context) -> AssetCheckResult:
    try:
        df          = _read_parquet("yhct-gold", "gold/mongodb/gold_medical_insights.parquet")
        total       = df.shape[0]
        unique_logs = df["log_id"].n_unique()
        null_logs   = df.filter(pl.col("log_id").is_null() | (pl.col("log_id") == "")).shape[0]
        return AssetCheckResult(
            passed=(total == unique_logs) and (null_logs == 0),
            severity=AssetCheckSeverity.ERROR,
            metadata={
                "total_records": MetadataValue.int(total),
                "unique_logs":   MetadataValue.int(unique_logs),
                "null_logs":     MetadataValue.int(null_logs),
            },
        )
    except S3Error as e:
        if e.code == "NoSuchKey":
            return _not_found_result("gold_insights_valid_ids")
        return _error_result(e)
    except Exception as e:
        return _error_result(e)


# Danh sách export cho __init__.py
ALL_CHECKS = [
    check_bronze_has_data,
    check_bronze_text_not_garbled,
    check_bronze_no_duplicate_pages,
    check_silver_filter_rate,
    check_silver_no_empty_text,
    check_gold_chunk_size,
    check_gold_no_duplicate_chunks,
    check_gold_coverage_per_source,
    check_gold_engagement_no_nulls,
    check_gold_chat_rating_range,
    check_gold_insights_valid,
]
