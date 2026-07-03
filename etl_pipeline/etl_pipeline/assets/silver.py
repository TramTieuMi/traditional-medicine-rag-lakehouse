# etl_pipeline/etl_pipeline/assets/silver.py
#
# Silver layer — lọc trang YHCT liên quan từ Bronze.
# Engine: Polars (single-node, đủ nhanh cho quy mô hiện tại).
# Incremental: chỉ filter trang mới chưa có trong Silver, giữ nguyên trang cũ.

import os
from datetime import datetime
from io import BytesIO

import polars as pl
from dagster import AssetIn, MetadataValue, Output, asset
from minio import Minio
from minio.error import S3Error
from .text_cleaner import clean_ocr_text

MINIO_ENDPOINT   = "minio:9000"
MINIO_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID",     "minio")
MINIO_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "minio123")
SILVER_BUCKET    = "yhct-silver"
SILVER_KEY       = "silver/pdf/silver_filtered_pages.parquet"

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

MIN_WORDS = 40


def _classify_page(text: str) -> str:
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


def _load_existing_silver() -> pl.DataFrame | None:
    """Đọc Silver hiện có từ MinIO. Trả về None nếu chưa có."""
    try:
        client = Minio(MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, secure=False)
        obj    = client.get_object(SILVER_BUCKET, SILVER_KEY)
        return pl.read_parquet(BytesIO(obj.read()))
    except S3Error as e:
        if e.code == "NoSuchKey":
            return None
        raise
    except Exception:
        return None


def _filter_with_polars(pages_df: pl.DataFrame, context) -> tuple[pl.DataFrame, dict]:
    """Lọc trang YHCT bằng Polars. Chỉ gọi với trang CHƯA có trong Silver."""
    kept_rows    = []
    filter_stats = {}

    for row in pages_df.iter_rows(named=True):
        # Làm sạch và sửa lỗi chính tả văn bản trước khi phân loại và lưu
        cleaned_text = clean_ocr_text(row["page_text"])
        reason = _classify_page(cleaned_text)
        if reason == "":
            cleaned_row = dict(row)
            cleaned_row["page_text"] = cleaned_text
            cleaned_row["word_count"] = len(cleaned_text.split())
            kept_rows.append({
                **cleaned_row,
                "is_filtered":   False,
                "filter_reason": "",
                "silver_time":   datetime.utcnow(),
            })
        else:
            filter_stats[reason] = filter_stats.get(reason, 0) + 1

    if not kept_rows:
        # Return empty DataFrame with correct schema instead of raising ValueError
        empty_df = pages_df.clone().head(0).with_columns([
            pl.lit(False).alias("is_filtered").cast(pl.Boolean),
            pl.lit("").alias("filter_reason").cast(pl.Utf8),
            pl.lit(datetime.utcnow()).alias("silver_time").cast(pl.Datetime)
        ])
        return empty_df, filter_stats

    return pl.DataFrame(kept_rows), filter_stats


# ══════════════════════════════════════════════════════════════════════════════
# DAGSTER ASSET
# ══════════════════════════════════════════════════════════════════════════════

@asset(
    name="silver_filtered_pages",
    key_prefix=["silver", "pdf"],
    group_name="silver",
    io_manager_key="minio_io_manager",
    compute_kind="python",
    ins={"bronze_pdf_pages": AssetIn(key_prefix=["bronze", "pdf"])},
    description=(
        "Lọc trang YHCT liên quan từ Bronze → Silver (Polars). "
        "Incremental: chỉ filter trang mới, giữ nguyên Silver cũ."
    ),
)
def silver_filtered_pages(context, bronze_pdf_pages: pl.DataFrame) -> Output:
    context.log.info(f"📥 Nhận {bronze_pdf_pages.shape[0]} trang từ Bronze")

    # ── Incremental: tìm trang đã có trong Silver ────────────────────────────
    existing_silver = _load_existing_silver()
    already_done: set[tuple] = set()

    if existing_silver is not None:
        # Thêm filter_reason nếu Silver cũ không có (backward compat)
        if "filter_reason" not in existing_silver.columns:
            existing_silver = existing_silver.with_columns(pl.lit("").alias("filter_reason"))

        already_done = set(zip(
            existing_silver["doc_id"].to_list(),
            existing_silver["page_num"].to_list(),
        ))
        context.log.info(
            f"📊 Silver hiện có: {existing_silver.shape[0]} trang "
            f"từ {existing_silver['source_file'].n_unique()} file"
        )
    else:
        context.log.info("📊 Chưa có Silver data — lần đầu chạy toàn bộ.")

    # ── Tìm trang mới chưa có trong Silver ──────────────────────────────────
    new_pages = [
        row for row in bronze_pdf_pages.iter_rows(named=True)
        if (row["doc_id"], row["page_num"]) not in already_done
    ]

    if not new_pages:
        context.log.info("✅ Không có trang mới — dùng lại Silver hiện có.")
        return Output(
            value=existing_silver,
            metadata={
                "status":       MetadataValue.text("incremental_no_new_pages"),
                "total_kept":   MetadataValue.int(existing_silver.shape[0]),
                "new_filtered": MetadataValue.int(0),
            },
        )

    context.log.info(f"🆕 Cần filter {len(new_pages)} trang mới")
    new_bronze_df = pl.DataFrame(new_pages)

    # Log theo file
    file_counts = (
        new_bronze_df.group_by("source_file")
        .agg(pl.len().alias("pages"))
        .sort("source_file")
    )
    for row in file_counts.iter_rows(named=True):
        context.log.info(f"   └─ {row['source_file']}: {row['pages']} trang mới")

    # ── Filter chỉ trang mới ─────────────────────────────────────────────────
    new_silver_df, filter_stats = _filter_with_polars(new_bronze_df, context)
    total_removed = len(new_pages) - new_silver_df.shape[0]

    # ── Merge với Silver cũ ──────────────────────────────────────────────────
    if existing_silver is not None:
        # Align columns just in case schemas have evolved
        for col in new_silver_df.columns:
            if col not in existing_silver.columns:
                existing_silver = existing_silver.with_columns(pl.lit(None).alias(col).cast(new_silver_df[col].dtype))
        for col in existing_silver.columns:
            if col not in new_silver_df.columns:
                new_silver_df = new_silver_df.with_columns(pl.lit(None).alias(col).cast(existing_silver[col].dtype))
        
        # Ensure correct types and ordering
        new_silver_df = new_silver_df.with_columns(
            pl.col("silver_time").cast(existing_silver["silver_time"].dtype)
        ).select(existing_silver.columns)
        
        combined_df = pl.concat([existing_silver, new_silver_df])
        context.log.info(
            f"🔗 Kết hợp: {existing_silver.shape[0]} trang cũ + "
            f"{new_silver_df.shape[0]} trang mới = {combined_df.shape[0]} tổng"
        )
    else:
        combined_df = new_silver_df

    # Log filter stats
    context.log.info(f"\n{'='*50}")
    context.log.info(f"✅ TỔNG KẾT SILVER (incremental):")
    context.log.info(f"   Trang mới giữ lại: {new_silver_df.shape[0]}")
    context.log.info(f"   Trang mới loại bỏ: {total_removed}")
    for reason, count in sorted(filter_stats.items(), key=lambda x: x[1], reverse=True):
        context.log.info(f"   └─ {reason}: {count} trang")
    context.log.info(f"   Tổng Silver: {combined_df.shape[0]} trang")

    file_kept = (
        combined_df.group_by("source_file")
        .agg(pl.len().alias("pages_kept"))
        .sort("source_file")
    )
    preview_df = combined_df.select(
        ["source_file", "doc_id", "page_num", "word_count"]
    ).head(8)

    return Output(
        value=combined_df,
        metadata={
            "engine":         MetadataValue.text("polars"),
            "total_kept":     MetadataValue.int(combined_df.shape[0]),
            "new_filtered":   MetadataValue.int(new_silver_df.shape[0]),
            "new_removed":    MetadataValue.int(total_removed),
            "filter_stats":   MetadataValue.json(filter_stats),
            "kept_by_file":   MetadataValue.md(file_kept.to_pandas().to_markdown()),
            "preview":        MetadataValue.md(preview_df.to_pandas().to_markdown()),
        },
    )
