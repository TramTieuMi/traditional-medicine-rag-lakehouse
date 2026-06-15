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

# Job rõ ràng để schedule có thể attach vào (không dùng implicit __ASSET_JOB)
all_assets_job = define_asset_job(
    name="all_assets_job",
    selection=AssetSelection.all(),
    description="Chạy toàn bộ pipeline: Bronze → Silver → Gold → Embeddings",
)

RAW_DIR           = Path("/opt/dagster/app/data/raw")
POLL_INTERVAL_SEC = 30   # kiểm tra mỗi 30 giây


@sensor(
    job_name="__ASSET_JOB",
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
# Chạy toàn bộ pipeline mỗi sáng sớm để:
#   - Kiểm tra asset checks tự động (data quality gate)
#   - Đảm bảo ChromaDB và MinIO không out-of-sync
#   - Thu thập metrics gold_evaluation định kỳ
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
