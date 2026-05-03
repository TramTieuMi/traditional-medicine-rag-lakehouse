# etl_pipeline/etl_pipeline/assets/silver.py

from dagster import asset, Output, MetadataValue, AssetIn
import polars as pl
from datetime import datetime


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


def is_relevant(text: str) -> tuple[bool, str]:
    """
    Kiểm tra trang có liên quan đến YHCT không.
    Trả về (True/False, lý do nếu bị loại)
    """
    words = text.split()

    # Quá ngắn
    if len(words) < MIN_WORDS:
        return False, "too_short"

    low = text.lower()

    # Chứa stopword → loại
    for sw in STOPWORDS:
        if sw in low:
            return False, f"stopword:{sw}"

    # Không chứa keyword YHCT → loại
    if not any(kw in low for kw in YHCT_KEYWORDS):
        return False, "no_yhct_keyword"

    return True, ""


@asset(
    name="silver_filtered_pages",
    key_prefix=["silver", "pdf"],
    group_name="silver",
    io_manager_key="minio_io_manager",
    compute_kind="python",
    ins={
        "bronze_pdf_pages": AssetIn(
            key_prefix=["bronze", "pdf"]
        )
    },
    description="Lọc trang YHCT liên quan từ Bronze → Silver Layer"
)
def silver_filtered_pages(context, bronze_pdf_pages: pl.DataFrame) -> Output:

    context.log.info(f"📥 Nhận {bronze_pdf_pages.shape[0]} trang từ Bronze")

    # Log thống kê theo file
    file_counts = bronze_pdf_pages.group_by("source_file").agg(
        pl.len().alias("pages")
    ).sort("source_file")
    for row in file_counts.iter_rows(named=True):
        context.log.info(f"   └─ {row['source_file']}: {row['pages']} trang")

    kept_rows    = []
    filter_stats = {}          # lý do → số trang
    file_kept    = {}          # file → số trang giữ lại

    for row in bronze_pdf_pages.iter_rows(named=True):
        ok, reason = is_relevant(row["page_text"])

        if ok:
            kept_rows.append({
                **row,
                "is_filtered":   False,
                "filter_reason": "",
                "silver_time":   datetime.utcnow(),
            })
            file_kept[row["source_file"]] = \
                file_kept.get(row["source_file"], 0) + 1
        else:
            filter_stats[reason] = filter_stats.get(reason, 0) + 1

    total_removed = bronze_pdf_pages.shape[0] - len(kept_rows)

    context.log.info(f"\n{'='*50}")
    context.log.info(f"✅ TỔNG KẾT SILVER:")
    context.log.info(f"   Giữ lại: {len(kept_rows)} trang")
    context.log.info(f"   Loại bỏ: {total_removed} trang")
    context.log.info("   Lý do loại:")
    for reason, count in sorted(filter_stats.items(),
                                key=lambda x: x[1], reverse=True):
        context.log.info(f"      └─ {reason}: {count} trang")
    context.log.info("   Giữ lại theo file:")
    for fname, cnt in sorted(file_kept.items()):
        context.log.info(f"      └─ {fname}: {cnt} trang")

    if not kept_rows:
        raise ValueError("Silver: không có trang nào pass filter!")

    silver_df = pl.DataFrame(kept_rows)

    preview_df = silver_df.select([
        "source_file", "doc_id", "page_num", "word_count"
    ]).head(8)

    # Stats theo file
    file_stats_df = pl.DataFrame([
        {"source_file": k, "pages_kept": v}
        for k, v in sorted(file_kept.items())
    ])

    return Output(
        value=silver_df,
        metadata={
            "total_input":    MetadataValue.int(bronze_pdf_pages.shape[0]),
            "total_kept":     MetadataValue.int(len(kept_rows)),
            "total_removed":  MetadataValue.int(total_removed),
            "filter_stats":   MetadataValue.json(filter_stats),
            "kept_by_file":   MetadataValue.md(
                file_stats_df.to_pandas().to_markdown()
            ),
            "preview":        MetadataValue.md(
                preview_df.to_pandas().to_markdown()
            ),
        }
    )