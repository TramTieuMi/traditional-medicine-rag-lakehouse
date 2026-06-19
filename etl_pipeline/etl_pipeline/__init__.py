# etl_pipeline/etl_pipeline/__init__.py

from dagster import Definitions

from .assets.bronze                  import bronze_pdf_ingestion
from .assets.silver                  import silver_filtered_pages
from .assets.gold_chunks             import gold_yhct_chunks
from .assets.gold_embeddings         import gold_embeddings
from .assets.gold_evaluation         import gold_evaluation
from .assets.gold_herb_mentions      import gold_herb_mentions
from .assets.gold_tang_phu_mentions  import gold_tang_phu_mentions
from .assets.gold_pdf_extraction     import gold_pdf_extraction

# Import new User Lakehouse Medallion assets
from .assets.user_bronze             import (
    bronze_mongodb_users, 
    bronze_mongodb_conversations, 
    bronze_mongodb_events, 
    bronze_mongodb_medical_logs
)
from .assets.user_silver             import (
    silver_mongodb_users, 
    silver_mongodb_conversations, 
    silver_mongodb_events, 
    silver_mongodb_medical_logs
)
from .assets.user_gold               import (
    gold_user_engagement, 
    gold_chat_performance, 
    gold_medical_insights
)

from .resources.minio_io_manager     import MinIOIOManager
from .sensors                        import (
    new_pdf_sensor,
    mongodb_change_sensor,
    daily_pipeline_schedule,
    user_lakehouse_schedule,
    all_assets_job,
    user_lakehouse_job
)
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
        gold_pdf_extraction,
        
        # User Lakehouse - Bronze
        bronze_mongodb_users,
        bronze_mongodb_conversations,
        bronze_mongodb_events,
        bronze_mongodb_medical_logs,
        
        # User Lakehouse - Silver
        silver_mongodb_users,
        silver_mongodb_conversations,
        silver_mongodb_events,
        silver_mongodb_medical_logs,
        
        # User Lakehouse - Gold
        gold_user_engagement,
        gold_chat_performance,
        gold_medical_insights,
    ],
    asset_checks=ALL_CHECKS,
    jobs=[all_assets_job, user_lakehouse_job],
    resources={
        "minio_io_manager": MinIOIOManager(MINIO_CONFIG),
    },
    sensors=[new_pdf_sensor, mongodb_change_sensor],
    schedules=[daily_pipeline_schedule, user_lakehouse_schedule],
)
