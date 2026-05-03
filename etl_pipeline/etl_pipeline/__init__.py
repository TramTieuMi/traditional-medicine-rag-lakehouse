# etl_pipeline/etl_pipeline/__init__.py

from dagster import Definitions
from .assets.bronze                  import bronze_pdf_ingestion
from .assets.silver                  import silver_filtered_pages
from .assets.gold_chunks             import gold_yhct_chunks
from .assets.gold_herb_mentions      import gold_herb_mentions
from .assets.gold_tang_phu_mentions  import gold_tang_phu_mentions
from .assets.gold_embeddings         import gold_embeddings
from .resources.minio_io_manager     import MinIOIOManager

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
        gold_herb_mentions,
        gold_tang_phu_mentions,
        gold_embeddings,          # ← THÊM
    ],
    resources={
        "minio_io_manager": MinIOIOManager(MINIO_CONFIG),
    },
)