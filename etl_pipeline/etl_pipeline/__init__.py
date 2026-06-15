# etl_pipeline/etl_pipeline/__init__.py

from dagster import Definitions

from .assets.bronze                  import bronze_pdf_ingestion
from .assets.silver                  import silver_filtered_pages
from .assets.gold_chunks             import gold_yhct_chunks
from .assets.gold_embeddings         import gold_embeddings
from .assets.gold_evaluation         import gold_evaluation
from .assets.gold_herb_mentions      import gold_herb_mentions
from .assets.gold_tang_phu_mentions  import gold_tang_phu_mentions
from .assets.gold_mongodb_sync       import gold_mongodb_users, gold_mongodb_sessions, gold_mongodb_events
from .resources.minio_io_manager     import MinIOIOManager
from .sensors                        import new_pdf_sensor, daily_pipeline_schedule, all_assets_job
from .checks                         import ALL_CHECKS

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
        gold_herb_mentions,
        gold_tang_phu_mentions,
        gold_mongodb_users,
        gold_mongodb_sessions,
        gold_mongodb_events,
    ],
    asset_checks=ALL_CHECKS,
    jobs=[all_assets_job],
    resources={
        "minio_io_manager": MinIOIOManager(MINIO_CONFIG),
    },
    sensors=[new_pdf_sensor],
    schedules=[daily_pipeline_schedule],
)
