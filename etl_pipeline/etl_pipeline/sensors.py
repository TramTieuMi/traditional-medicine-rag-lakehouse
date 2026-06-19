# etl_pipeline/etl_pipeline/sensors.py
#
# Sensor: event-driven — tự động trigger pipeline khi phát hiện PDF mới.
# Schedule: time-driven — chạy pipeline mỗi ngày lúc 2 giờ sáng để
#           đảm bảo dữ liệu luôn được cập nhật và checks được chạy định kỳ.

import json
from pathlib import Path

from dagster import (
    AssetSelection,
    RunRequest,
    ScheduleDefinition,
    SensorEvaluationContext,
    SkipReason,
    define_asset_job,
    sensor,
)

__all__ = [
    "all_assets_job", "user_lakehouse_job",
    "new_pdf_sensor", "mongodb_change_sensor",
    "daily_pipeline_schedule", "user_lakehouse_schedule",
]
from pymongo import MongoClient
import os

# Job rõ ràng để schedule có thể attach vào (không dùng implicit __ASSET_JOB)
all_assets_job = define_asset_job(
    name="all_assets_job",
    selection=AssetSelection.all(),
    description="Chạy toàn bộ pipeline: Bronze → Silver → Gold → Embeddings",
)

# Job chạy riêng biệt cho User Data Lakehouse
user_lakehouse_job = define_asset_job(
    name="user_lakehouse_job",
    selection=AssetSelection.groups("user_lakehouse"),
    description="Chạy pipeline User Data Lakehouse: Bronze → Silver → Gold",
)

RAW_DIR           = Path("/opt/dagster/app/data/raw")
POLL_INTERVAL_SEC = 30   # kiểm tra mỗi 30 giây


@sensor(
    job=all_assets_job,
    minimum_interval_seconds=POLL_INTERVAL_SEC,
    description=(
        "Theo dõi thư mục data/raw/ mỗi 30 giây. "
        "Khi phát hiện PDF mới → tự động kích hoạt toàn bộ pipeline "
        "(Bronze → Silver → Gold Chunks → Embeddings → Herb/Tang Phủ)."
    ),
)
def new_pdf_sensor(context: SensorEvaluationContext):
    """
    Sensor phát hiện PDF mới và trigger pipeline.

    Cursor lưu danh sách tên file đã được trigger (JSON list).
    Dùng run_key để Dagster đảm bảo mỗi tập file mới chỉ trigger 1 run
    dù sensor có bị gọi nhiều lần (idempotent).
    """
    if not RAW_DIR.exists():
        yield SkipReason(f"Thư mục {RAW_DIR} chưa tồn tại.")
        return

    current_pdfs: list[str] = sorted(
        f.name for f in RAW_DIR.glob("*.pdf") if f.is_file()
    )

    if not current_pdfs:
        yield SkipReason("Chưa có file PDF nào trong data/raw/.")
        return

    processed: set[str] = set(json.loads(context.cursor)) if context.cursor else set()
    new_pdfs  = [f for f in current_pdfs if f not in processed]

    if not new_pdfs:
        yield SkipReason(
            f"Không có PDF mới. "
            f"Đang theo dõi {len(current_pdfs)} file(s): {', '.join(current_pdfs)}."
        )
        return

    context.update_cursor(json.dumps(current_pdfs))
    context.log.info(
        f"🆕 Phát hiện {len(new_pdfs)} PDF mới: {new_pdfs}. Kích hoạt pipeline..."
    )

    run_key = "sensor_" + "_".join(new_pdfs).replace(" ", "_")[:120]

    yield RunRequest(
        run_key=run_key,
        tags={
            "triggered_by": "new_pdf_sensor",
            "new_files":    ", ".join(new_pdfs),
            "total_files":  str(len(current_pdfs)),
        },
    )


# ── Schedule: cron 2AM mỗi ngày ─────────────────────────────────────────────
daily_pipeline_schedule = ScheduleDefinition(
    name="daily_pipeline_2am",
    cron_schedule="0 2 * * *",
    job=all_assets_job,
    description=(
        "Chạy toàn bộ pipeline lúc 2 giờ sáng mỗi ngày. "
        "Đảm bảo dữ liệu fresh, asset checks được validate, "
        "và metrics được cập nhật tự động."
    ),
)

# ── Schedule: User Lakehouse mỗi 15 phút ────────────────────────────────────
# Đảm bảo data từ MongoDB luôn được sync vào Silver/Gold
# tối đa sau 15 phút kể từ lúc có hoạt động mới (chat, đăng ký, v.v.)
user_lakehouse_schedule = ScheduleDefinition(
    name="user_lakehouse_every_15min",
    cron_schedule="*/15 * * * *",
    job=user_lakehouse_job,
    description=(
        "Sync MongoDB → Bronze → Silver → Gold mỗi 15 phút. "
        "Đảm bảo dashboard Streamlit luôn hiển thị dữ liệu gần nhất."
    ),
)


@sensor(
    job=user_lakehouse_job,
    minimum_interval_seconds=900,  # 15 phút
    description="Kiểm tra thay đổi dữ liệu trên MongoDB và trigger chạy pipeline User Lakehouse",
)
def mongodb_change_sensor(context: SensorEvaluationContext):
    last_ts = context.cursor if context.cursor else "1970-01-01T00:00:00.000000"
    
    from datetime import datetime
    try:
        last_dt = datetime.fromisoformat(last_ts.replace("Z", "+00:00"))
    except Exception:
        last_dt = datetime(1970, 1, 1)
        
    mongo_uri = os.getenv("MONGO_URI", "mongodb://mongodb:27017/yhct_db")
    client = MongoClient(mongo_uri)
    db = client.get_database()
    
    collections_to_check = {
        "users": "created_at",
        "conversations": "start_time",
        "analyticsevents": "timestamp",
        "medicalentitylogs": "timestamp"
    }
    
    max_found_dt = last_dt
    has_new_data = False
    
    for col_name, ts_field in collections_to_check.items():
        col = db.get_collection(col_name)
        new_doc = col.find_one({ts_field: {"$gt": last_dt}}, sort=[(ts_field, -1)])
        if new_doc:
            has_new_data = True
            val = new_doc[ts_field]
            # MongoDB Date is returned as a timezone-naive datetime (usually UTC) or timezone-aware depending on settings.
            # Let's ensure both are naive for clean comparison.
            if val.tzinfo is not None:
                val = val.replace(tzinfo=None)
            if last_dt.tzinfo is not None:
                last_dt = last_dt.replace(tzinfo=None)
                
            if val > max_found_dt.replace(tzinfo=None):
                max_found_dt = val
                
    client.close()
    
    if not has_new_data:
        yield SkipReason("Không có dữ liệu mới trên MongoDB kể từ lần cuối kiểm tra.")
        return
        
    new_cursor_val = max_found_dt.isoformat()
    context.update_cursor(new_cursor_val)
    
    yield RunRequest(
        run_key=f"mongodb_sync_{new_cursor_val.replace(':', '_')}",
        tags={"triggered_by": "mongodb_change_sensor", "last_timestamp": new_cursor_val}
    )
