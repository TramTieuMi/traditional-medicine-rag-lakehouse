# etl_pipeline/etl_pipeline/assets/bronze.py
#
# INCREMENTAL LOADING:
# Mỗi lần chạy, bronze chỉ xử lý các PDF chưa từng được ingest trước đó.
# Dữ liệu mới được concat vào parquet hiện có thay vì ghi đè toàn bộ.
# Idempotency đảm bảo bằng doc_id cố định (MD5 của tên file).

import hashlib
import os
from datetime import datetime
from io import BytesIO

import fitz as pymupdf
import polars as pl
from dagster import MetadataValue, Output, asset
from minio import Minio
from minio.error import S3Error
from pathlib import Path

MINIO_ENDPOINT   = "minio:9000"
MINIO_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID",     "minio")
MINIO_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "minio123")
BRONZE_BUCKET    = "yhct-bronze"
BRONZE_KEY       = "bronze/pdf/bronze_pdf_pages.parquet"


def _load_existing_bronze() -> pl.DataFrame | None:
    """Đọc parquet bronze hiện có từ MinIO. Trả về None nếu chưa có."""
    try:
        client = Minio(MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, secure=False)
        obj    = client.get_object(BRONZE_BUCKET, BRONZE_KEY)
        return pl.read_parquet(BytesIO(obj.read()))
    except S3Error as e:
        if e.code == "NoSuchKey":
            return None
        raise
    except Exception:
        return None


@asset(
    name="bronze_pdf_pages",
    key_prefix=["bronze", "pdf"],
    group_name="bronze",
    io_manager_key="minio_io_manager",
    compute_kind="python",
    description=(
        "Ingest PDF trong data/raw → Bronze Layer (MinIO Parquet). "
        "Incremental: chỉ xử lý file mới, giữ nguyên dữ liệu cũ."
    ),
)
def bronze_pdf_ingestion(context) -> Output:
    raw_dir = Path("/opt/dagster/app/data/raw")

    if not raw_dir.exists():
        raise FileNotFoundError(f"Không tìm thấy thư mục: {raw_dir}")

    pdf_files = sorted(raw_dir.glob("*.pdf"))
    if not pdf_files:
        raise FileNotFoundError(f"Không có file PDF nào trong: {raw_dir}")

    # ── Incremental: tìm file đã xử lý ──────────────────────────────────────
    existing_df  = _load_existing_bronze()
    already_done: set[str] = set()

    if existing_df is not None:
        already_done = set(existing_df["source_file"].unique().to_list())
        context.log.info(
            f"📊 Bronze hiện có: {existing_df.shape[0]} trang "
            f"từ {len(already_done)} file: {sorted(already_done)}"
        )
    else:
        context.log.info("📊 Chưa có bronze data — lần đầu chạy toàn bộ.")

    new_pdf_files  = [f for f in pdf_files if f.name not in already_done]
    skip_pdf_files = [f for f in pdf_files if f.name in already_done]

    if skip_pdf_files:
        context.log.info(
            f"⏭  Bỏ qua {len(skip_pdf_files)} file đã có trong bronze: "
            f"{[f.name for f in skip_pdf_files]}"
        )

    # ── Không có file mới → trả về data hiện có ─────────────────────────────
    if not new_pdf_files:
        context.log.info("✅ Không có file PDF mới — dùng lại bronze hiện có.")
        return Output(
            value=existing_df,
            metadata={
                "status":         MetadataValue.text("incremental_no_new_files"),
                "total_pages":    MetadataValue.int(existing_df.shape[0]),
                "total_files":    MetadataValue.int(len(already_done)),
                "skipped_files":  MetadataValue.json([f.name for f in skip_pdf_files]),
            },
        )

    # ── Xử lý các file PDF mới ───────────────────────────────────────────────
    context.log.info(f"🆕 Xử lý {len(new_pdf_files)} file PDF mới:")
    for f in new_pdf_files:
        context.log.info(f"   └─ {f.name}")

    new_pages  = []
    file_stats = []
    ingest_ts  = datetime.utcnow()

    for pdf_path in new_pdf_files:
        doc_id = "pdf_" + hashlib.md5(pdf_path.name.encode()).hexdigest()[:12]
        context.log.info(f"📖 Đang xử lý: {pdf_path.name} ...")

        try:
            doc = pymupdf.open(str(pdf_path))
            total_pages = len(doc)

            for page_num in range(total_pages):
                page = doc[page_num]
                text = page.get_text("text")
                new_pages.append({
                    "doc_id":         doc_id,
                    "page_num":       page_num + 1,
                    "page_text":      text,
                    "word_count":     len(text.split()),
                    "source_file":    pdf_path.name,
                    "ingestion_time": ingest_ts,
                    "total_pages":    total_pages,
                })

            doc.close()
            file_stats.append({"source_file": pdf_path.name, "doc_id": doc_id, "pages": total_pages})
            context.log.info(f"   ✅ {pdf_path.name}: {total_pages} trang | doc_id={doc_id}")

        except Exception as e:
            context.log.error(f"   ❌ Lỗi xử lý {pdf_path.name}: {e}")
            raise

    new_df = pl.DataFrame(new_pages)

    # ── Merge với data cũ ────────────────────────────────────────────────────
    if existing_df is not None:
        # Đảm bảo schema nhất quán trước khi concat
        new_df = new_df.with_columns(
            pl.col("ingestion_time").cast(existing_df["ingestion_time"].dtype)
        )
        combined_df = pl.concat([existing_df, new_df])
        context.log.info(
            f"🔗 Kết hợp: {existing_df.shape[0]} trang cũ + "
            f"{new_df.shape[0]} trang mới = {combined_df.shape[0]} tổng"
        )
    else:
        combined_df = new_df

    stats_df    = pl.DataFrame(file_stats)
    preview_df  = combined_df.select(["doc_id", "source_file", "page_num", "word_count"]).head(8)

    context.log.info(f"\n{'='*50}")
    context.log.info(f"✅ TỔNG KẾT BRONZE (sau incremental load):")
    context.log.info(f"   File mới xử lý: {len(new_pdf_files)}")
    context.log.info(f"   File bỏ qua:    {len(skip_pdf_files)}")
    context.log.info(f"   Tổng trang:     {combined_df.shape[0]}")
    context.log.info(f"   Tổng từ:        {combined_df['word_count'].sum():,}")

    return Output(
        value=combined_df,
        metadata={
            "status":          MetadataValue.text("incremental_new_files_processed"),
            "total_pages":     MetadataValue.int(combined_df.shape[0]),
            "total_files":     MetadataValue.int(len(pdf_files)),
            "new_files":       MetadataValue.int(len(new_pdf_files)),
            "skipped_files":   MetadataValue.int(len(skip_pdf_files)),
            "files_ingested":  MetadataValue.json([f.name for f in new_pdf_files]),
            "ingestion_time":  ingest_ts.isoformat(),
            "new_file_stats":  MetadataValue.md(stats_df.to_pandas().to_markdown()),
            "preview":         MetadataValue.md(preview_df.to_pandas().to_markdown()),
        },
    )
