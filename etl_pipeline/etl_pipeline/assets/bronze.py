# etl_pipeline/etl_pipeline/assets/bronze.py

from dagster import asset, Output, MetadataValue
import uuid
from datetime import datetime
import fitz as pymupdf
import polars as pl
from pathlib import Path


@asset(
    name="bronze_pdf_pages",
    key_prefix=["bronze", "pdf"],
    group_name="bronze",
    io_manager_key="minio_io_manager",
    compute_kind="python",
    description="Ingest tất cả PDF trong data/raw → Bronze Layer (MinIO Parquet)"
)
def bronze_pdf_ingestion(context) -> Output:
    raw_dir = Path("/opt/dagster/app/data/raw")

    if not raw_dir.exists():
        raise FileNotFoundError(f"Không tìm thấy thư mục: {raw_dir}")

    pdf_files = sorted(raw_dir.glob("*.pdf"))
    if not pdf_files:
        raise FileNotFoundError(f"Không có file PDF nào trong: {raw_dir}")

    context.log.info(f"📂 Tìm thấy {len(pdf_files)} file PDF:")
    for f in pdf_files:
        context.log.info(f"   └─ {f.name}")

    all_pages = []
    ingestion_time = datetime.utcnow()
    file_stats = []

    for pdf_path in pdf_files:
        doc_id = f"pdf_{uuid.uuid4().hex[:12]}"
        context.log.info(f"📖 Đang xử lý: {pdf_path.name} ...")

        try:
            doc = pymupdf.open(str(pdf_path))
            total_pages = len(doc)

            for page_num in range(total_pages):
                page = doc[page_num]
                text = page.get_text("text")
                all_pages.append({
                    "doc_id":         doc_id,
                    "page_num":       page_num + 1,
                    "page_text":      text,
                    "word_count":     len(text.split()),
                    "source_file":    pdf_path.name,
                    "ingestion_time": ingestion_time,
                    "total_pages":    total_pages,
                })

            doc.close()
            file_stats.append({
                "source_file": pdf_path.name,
                "doc_id":      doc_id,
                "pages":       total_pages,
            })
            context.log.info(f"   ✅ {pdf_path.name}: {total_pages} trang | doc_id={doc_id}")

        except Exception as e:
            context.log.error(f"   ❌ Lỗi xử lý {pdf_path.name}: {e}")
            raise

    df = pl.DataFrame(all_pages)

    # Thống kê theo từng file
    stats_df = pl.DataFrame(file_stats)
    context.log.info(f"\n{'='*50}")
    context.log.info(f"✅ TỔNG KẾT BRONZE:")
    context.log.info(f"   Số file PDF: {len(pdf_files)}")
    context.log.info(f"   Tổng trang:  {len(df)}")
    context.log.info(f"   Tổng từ:     {df['word_count'].sum():,}")

    preview_df = df.select([
        "doc_id", "source_file", "page_num", "word_count"
    ]).head(8)

    return Output(
        value=df,
        metadata={
            "total_pages":    MetadataValue.int(len(df)),
            "total_files":    MetadataValue.int(len(pdf_files)),
            "files_ingested": MetadataValue.json([f.name for f in pdf_files]),
            "ingestion_time": ingestion_time.isoformat(),
            "stats_by_file":  MetadataValue.md(
                stats_df.to_pandas().to_markdown()
            ),
            "preview":        MetadataValue.md(
                preview_df.to_pandas().to_markdown()
            ),
        }
    )