# etl_pipeline/etl_pipeline/__init__.py

from dagster import Definitions
from .assets.bronze                  import bronze_pdf_ingestion
from .assets.silver                  import silver_filtered_pages
from .assets.gold_chunks             import gold_yhct_chunks
from .assets.gold_embeddings         import gold_embeddings
from .assets.gold_evaluation         import gold_evaluation
from .resources.minio_io_manager     import MinIOIOManager

# ── Đã bỏ ────────────────────────────────────────────────────────────────────
# from .assets.gold_herb_mentions     import gold_herb_mentions
# from .assets.gold_tang_phu_mentions import gold_tang_phu_mentions
#
# Lý do: 2 assets này dùng herbs_dict.txt và tang_phu_dict.py hardcode tay,
# không derive từ nội dung PDF thực tế → số liệu không đáng tin.
# Thay thế bằng gold_structured (sẽ implement sau) — trích xuất
# có cấu trúc trực tiếp từ text PDF bằng regex + LLM fallback.
# ─────────────────────────────────────────────────────────────────────────────

MINIO_CONFIG = {
    "endpoint_url":  "minio:9000",
    "access_key":    "minio",
    "secret_key":    "minio123",
    "bronze_bucket": "yhct-bronze",
    "silver_bucket": "yhct-silver",
    "gold_bucket":   "yhct-gold",
}

defs = Definitions(
    assets=[
        bronze_pdf_ingestion,
        silver_filtered_pages,
        gold_yhct_chunks,
        gold_embeddings,
        gold_evaluation,
    ],
    resources={
        "minio_io_manager": MinIOIOManager(MINIO_CONFIG),
    },
)