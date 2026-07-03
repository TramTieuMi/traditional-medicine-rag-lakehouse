# etl_pipeline/etl_pipeline/assets/user_bronze.py
#
# Incremental loading từ MongoDB → Bronze layer.
# Mỗi lần chạy chỉ lấy record MỚI (dựa vào max timestamp của Parquet hiện có).
# Full scan chỉ xảy ra lần đầu tiên khi Parquet chưa tồn tại.

import hashlib
import json
import os
import uuid
from datetime import datetime
from io import BytesIO

import polars as pl
from dagster import Output, MetadataValue, asset
from minio import Minio
from minio.error import S3Error
from pymongo import MongoClient

MONGO_URI        = os.getenv("MONGO_URI", "mongodb://mongodb:27017/yhct_db")
MINIO_ENDPOINT   = "minio:9000"
MINIO_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID",     "minio")
MINIO_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "minio123")
BRONZE_BUCKET    = "yhct-bronze"


def _minio_client() -> Minio:
    return Minio(MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, secure=False)


def _load_existing_parquet(key: str) -> pl.DataFrame | None:
    """Đọc Parquet Bronze từ MinIO. Trả về None nếu chưa có."""
    try:
        obj = _minio_client().get_object(BRONZE_BUCKET, key)
        return pl.read_parquet(BytesIO(obj.read()))
    except S3Error as e:
        if e.code == "NoSuchKey":
            return None
        raise
    except Exception:
        return None


def _parse_last_dt(df: pl.DataFrame, col: str) -> datetime | None:
    """Lấy max timestamp từ DataFrame để làm watermark cho MongoDB query."""
    if df is None or df.is_empty():
        return None
    max_ts = df[col].max()
    if not max_ts:
        return None
    try:
        return datetime.fromisoformat(str(max_ts))
    except (ValueError, TypeError):
        return None


def _to_iso(val, fallback_dt: datetime) -> str:
    if isinstance(val, datetime):
        return val.isoformat()
    if val is not None:
        return str(val)
    return fallback_dt.isoformat()


def _align_and_concat(existing_df: pl.DataFrame | None, new_df: pl.DataFrame) -> pl.DataFrame:
    """Ghép hai DataFrame Polars một cách an toàn kể cả khi cấu trúc cột khác nhau."""
    if existing_df is None or existing_df.is_empty():
        return new_df
    if new_df is None or new_df.is_empty():
        return existing_df

    # Align columns from new_df to existing_df
    for col in new_df.columns:
        if col not in existing_df.columns:
            existing_df = existing_df.with_columns(pl.lit(None).alias(col).cast(new_df[col].dtype))
            
    # Align columns from existing_df to new_df
    for col in existing_df.columns:
        if col not in new_df.columns:
            new_df = new_df.with_columns(pl.lit(None).alias(col).cast(existing_df[col].dtype))

    # Reorder columns of new_df to match existing_df
    new_df = new_df.select(existing_df.columns)
    return pl.concat([existing_df, new_df])


# ── 1. Bronze Users ──────────────────────────────────────────────────────────
@asset(
    name="bronze_mongodb_users",
    key_prefix=["bronze", "mongodb"],
    group_name="user_lakehouse",
    io_manager_key="minio_io_manager",
    compute_kind="python",
    description="Nạp thô dữ liệu người dùng từ MongoDB sang Bronze layer (Parquet). Incremental theo created_at.",
)
def bronze_mongodb_users(context) -> Output:
    key         = "bronze/mongodb/bronze_mongodb_users.parquet"
    existing_df = _load_existing_parquet(key)
    last_dt     = _parse_last_dt(existing_df, "created_at")

    if last_dt:
        context.log.info(f"📊 Incremental users: lấy từ sau {last_dt.isoformat()}")
    else:
        context.log.info("📊 Full scan users: lần đầu hoặc Parquet trống.")

    now  = datetime.utcnow()
    rows = []
    query = {"created_at": {"$gt": last_dt}} if last_dt else {}

    with MongoClient(MONGO_URI) as client:
        for u in client.get_database().get_collection("users").find(query):
            email_raw = u.get("email", "").strip().lower()
            rows.append({
                "user_id":       str(u["_id"]),
                "user_uuid":     u.get("user_uuid") or str(uuid.uuid5(uuid.NAMESPACE_OID, str(u["_id"]))),
                "full_name":     u.get("full_name", ""),
                "email":         email_raw,
                "email_sha256":  hashlib.sha256(email_raw.encode("utf-8")).hexdigest() if email_raw else "",
                "age":           int(u.get("age") or 0),
                "gender":        u.get("gender", "khác"),
                "created_at":    _to_iso(u.get("created_at"), now),
                "last_login_at": _to_iso(u.get("last_login_at"), now),
            })

    if not rows:
        result_df = existing_df if existing_df is not None else pl.DataFrame({
            "user_id":       pl.Series([], dtype=pl.Utf8),
            "user_uuid":     pl.Series([], dtype=pl.Utf8),
            "full_name":     pl.Series([], dtype=pl.Utf8),
            "email":         pl.Series([], dtype=pl.Utf8),
            "email_sha256":  pl.Series([], dtype=pl.Utf8),
            "age":           pl.Series([], dtype=pl.Int32),
            "gender":        pl.Series([], dtype=pl.Utf8),
            "created_at":    pl.Series([], dtype=pl.Utf8),
            "last_login_at": pl.Series([], dtype=pl.Utf8),
        })
        context.log.info("✅ Không có user mới.")
        return Output(value=result_df, metadata={
            "total_records": MetadataValue.int(result_df.shape[0]),
            "new_records":   MetadataValue.int(0),
            "sync_time":     now.isoformat(),
        })

    new_df = pl.DataFrame(rows)
    result_df = _align_and_concat(existing_df, new_df)

    context.log.info(f"✅ +{len(rows)} users mới | tổng {result_df.shape[0]}")
    return Output(value=result_df, metadata={
        "total_records": MetadataValue.int(result_df.shape[0]),
        "new_records":   MetadataValue.int(len(rows)),
        "sync_time":     now.isoformat(),
    })


# ── 2. Bronze Conversations ───────────────────────────────────────────────────
@asset(
    name="bronze_mongodb_conversations",
    key_prefix=["bronze", "mongodb"],
    group_name="user_lakehouse",
    io_manager_key="minio_io_manager",
    compute_kind="python",
    description="Nạp thô dữ liệu phiên hội thoại từ MongoDB sang Bronze layer (Parquet). Incremental theo start_time.",
)
def bronze_mongodb_conversations(context) -> Output:
    key         = "bronze/mongodb/bronze_mongodb_conversations.parquet"
    existing_df = _load_existing_parquet(key)
    last_dt     = _parse_last_dt(existing_df, "start_time")

    if last_dt:
        context.log.info(f"📊 Incremental conversations: lấy từ sau {last_dt.isoformat()}")
    else:
        context.log.info("📊 Full scan conversations: lần đầu hoặc Parquet trống.")

    now   = datetime.utcnow()
    rows  = []
    query = {"start_time": {"$gt": last_dt}} if last_dt else {}

    with MongoClient(MONGO_URI) as client:
        for c in client.get_database().get_collection("conversations").find(query):
            messages_json = []
            for msg in c.get("messages", []):
                messages_json.append({
                    "message_content": msg.get("message_content", ""),
                    "ai_response":     msg.get("ai_response", ""),
                    "timestamp":       _to_iso(msg.get("timestamp"), now),
                    "elapsed_ms":      int(msg.get("elapsed_ms") or 0),
                    "is_zero":         bool(msg.get("is_zero", False)),
                    "sources":         msg.get("sources", []),
                    "sims":            msg.get("sims", []),
                    "metadatas":       [str(m) for m in msg.get("metadatas", [])],
                })
            rows.append({
                "conversation_id":      str(c["_id"]),
                "session_id":           c.get("session_id", ""),
                "user_id":              str(c.get("user_id", "")),
                "start_time":           _to_iso(c.get("start_time"), now),
                "total_messages":       int(c.get("total_messages") or 0),
                "session_duration_sec": float(c.get("session_duration_sec") or 0.0),
                "feedback_rating":      int(c["feedback_rating"]) if c.get("feedback_rating") is not None else -1,
                "messages_json":        json.dumps(messages_json, ensure_ascii=False),
            })

    if not rows:
        result_df = existing_df if existing_df is not None else pl.DataFrame({
            "conversation_id":      pl.Series([], dtype=pl.Utf8),
            "session_id":           pl.Series([], dtype=pl.Utf8),
            "user_id":              pl.Series([], dtype=pl.Utf8),
            "start_time":           pl.Series([], dtype=pl.Utf8),
            "total_messages":       pl.Series([], dtype=pl.Int32),
            "session_duration_sec": pl.Series([], dtype=pl.Float64),
            "feedback_rating":      pl.Series([], dtype=pl.Int32),
            "messages_json":        pl.Series([], dtype=pl.Utf8),
        })
        context.log.info("✅ Không có conversation mới.")
        return Output(value=result_df, metadata={
            "total_records": MetadataValue.int(result_df.shape[0]),
            "new_records":   MetadataValue.int(0),
            "sync_time":     now.isoformat(),
        })

    new_df    = pl.DataFrame(rows)
    result_df = _align_and_concat(existing_df, new_df)

    context.log.info(f"✅ +{len(rows)} conversations mới | tổng {result_df.shape[0]}")
    return Output(value=result_df, metadata={
        "total_records": MetadataValue.int(result_df.shape[0]),
        "new_records":   MetadataValue.int(len(rows)),
        "sync_time":     now.isoformat(),
    })


# ── 3. Bronze Events ──────────────────────────────────────────────────────────
@asset(
    name="bronze_mongodb_events",
    key_prefix=["bronze", "mongodb"],
    group_name="user_lakehouse",
    io_manager_key="minio_io_manager",
    compute_kind="python",
    description="Nạp thô dữ liệu sự kiện tương tác từ MongoDB sang Bronze layer (Parquet). Incremental theo timestamp.",
)
def bronze_mongodb_events(context) -> Output:
    key         = "bronze/mongodb/bronze_mongodb_events.parquet"
    existing_df = _load_existing_parquet(key)
    last_dt     = _parse_last_dt(existing_df, "timestamp")

    if last_dt:
        context.log.info(f"📊 Incremental events: lấy từ sau {last_dt.isoformat()}")
    else:
        context.log.info("📊 Full scan events: lần đầu hoặc Parquet trống.")

    now   = datetime.utcnow()
    rows  = []
    query = {"timestamp": {"$gt": last_dt}} if last_dt else {}

    with MongoClient(MONGO_URI) as client:
        for e in client.get_database().get_collection("analyticsevents").find(query):
            rows.append({
                "event_id":       str(e["_id"]),
                "session_id":     e.get("session_id", ""),
                "user_id":        str(e.get("user_id", "")) if e.get("user_id") else "",
                "event_type":     e.get("event_type", ""),
                "timestamp":      _to_iso(e.get("timestamp"), now),
                "device_type":    e.get("device_type", "desktop"),
                "browser":        e.get("browser", "Unknown"),
                "os":             e.get("os", "Unknown"),
                "ip_address":     e.get("ip_address", ""),
                "country":        e.get("country", "Vietnam"),
                "city":           e.get("city", "Unknown"),
                "referrer_url":   e.get("referrer_url", ""),
                "route":          e.get("route", ""),
                "button_name":    e.get("button_name") or "",
                "search_keywords": json.dumps(e.get("search_keywords", [])),
                "bounce":         int(bool(e.get("bounce", False))),
                "utm_source":     e.get("utm_source") or "",
                "utm_medium":     e.get("utm_medium") or "",
            })

    if not rows:
        result_df = existing_df if existing_df is not None else pl.DataFrame({
            "event_id":        pl.Series([], dtype=pl.Utf8),
            "session_id":      pl.Series([], dtype=pl.Utf8),
            "user_id":         pl.Series([], dtype=pl.Utf8),
            "event_type":      pl.Series([], dtype=pl.Utf8),
            "timestamp":       pl.Series([], dtype=pl.Utf8),
            "device_type":     pl.Series([], dtype=pl.Utf8),
            "browser":         pl.Series([], dtype=pl.Utf8),
            "os":              pl.Series([], dtype=pl.Utf8),
            "ip_address":      pl.Series([], dtype=pl.Utf8),
            "country":         pl.Series([], dtype=pl.Utf8),
            "city":            pl.Series([], dtype=pl.Utf8),
            "referrer_url":    pl.Series([], dtype=pl.Utf8),
            "route":           pl.Series([], dtype=pl.Utf8),
            "button_name":     pl.Series([], dtype=pl.Utf8),
            "search_keywords": pl.Series([], dtype=pl.Utf8),
            "bounce":          pl.Series([], dtype=pl.Int32),
            "utm_source":      pl.Series([], dtype=pl.Utf8),
            "utm_medium":      pl.Series([], dtype=pl.Utf8),
        })
        context.log.info("✅ Không có event mới.")
        return Output(value=result_df, metadata={
            "total_records": MetadataValue.int(result_df.shape[0]),
            "new_records":   MetadataValue.int(0),
            "sync_time":     now.isoformat(),
        })

    new_df    = pl.DataFrame(rows)
    result_df = _align_and_concat(existing_df, new_df)

    context.log.info(f"✅ +{len(rows)} events mới | tổng {result_df.shape[0]}")
    return Output(value=result_df, metadata={
        "total_records": MetadataValue.int(result_df.shape[0]),
        "new_records":   MetadataValue.int(len(rows)),
        "sync_time":     now.isoformat(),
    })


# ── 4. Bronze Medical Logs ────────────────────────────────────────────────────
@asset(
    name="bronze_mongodb_medical_logs",
    key_prefix=["bronze", "mongodb"],
    group_name="user_lakehouse",
    io_manager_key="minio_io_manager",
    compute_kind="python",
    description="Nạp thô dữ liệu thực thể y tế từ MongoDB sang Bronze layer (Parquet). Incremental theo timestamp.",
)
def bronze_mongodb_medical_logs(context) -> Output:
    key         = "bronze/mongodb/bronze_mongodb_medical_logs.parquet"
    existing_df = _load_existing_parquet(key)
    last_dt     = _parse_last_dt(existing_df, "timestamp")

    if last_dt:
        context.log.info(f"📊 Incremental medical_logs: lấy từ sau {last_dt.isoformat()}")
    else:
        context.log.info("📊 Full scan medical_logs: lần đầu hoặc Parquet trống.")

    now   = datetime.utcnow()
    rows  = []
    query = {"timestamp": {"$gt": last_dt}} if last_dt else {}

    with MongoClient(MONGO_URI) as client:
        for log in client.get_database().get_collection("medicalentitylogs").find(query):
            rows.append({
                "log_id":               str(log["_id"]),
                "session_id":           log.get("session_id", ""),
                "user_id":              str(log.get("user_id", "")),
                "timestamp":            _to_iso(log.get("timestamp"), now),
                "symptoms_mentioned":   json.dumps(log.get("symptoms_mentioned", [])),
                "diseases_mentioned":   json.dumps(log.get("diseases_mentioned", [])),
                "body_parts_mentioned": json.dumps(log.get("body_parts_mentioned", [])),
                "herbs_queried":        json.dumps(log.get("herbs_queried", [])),
            })

    if not rows:
        result_df = existing_df if existing_df is not None else pl.DataFrame({
            "log_id":               pl.Series([], dtype=pl.Utf8),
            "session_id":           pl.Series([], dtype=pl.Utf8),
            "user_id":              pl.Series([], dtype=pl.Utf8),
            "timestamp":            pl.Series([], dtype=pl.Utf8),
            "symptoms_mentioned":   pl.Series([], dtype=pl.Utf8),
            "diseases_mentioned":   pl.Series([], dtype=pl.Utf8),
            "body_parts_mentioned": pl.Series([], dtype=pl.Utf8),
            "herbs_queried":        pl.Series([], dtype=pl.Utf8),
        })
        context.log.info("✅ Không có medical log mới.")
        return Output(value=result_df, metadata={
            "total_records": MetadataValue.int(result_df.shape[0]),
            "new_records":   MetadataValue.int(0),
            "sync_time":     now.isoformat(),
        })

    new_df    = pl.DataFrame(rows)
    result_df = _align_and_concat(existing_df, new_df)

    context.log.info(f"✅ +{len(rows)} medical logs mới | tổng {result_df.shape[0]}")
    return Output(value=result_df, metadata={
        "total_records": MetadataValue.int(result_df.shape[0]),
        "new_records":   MetadataValue.int(len(rows)),
        "sync_time":     now.isoformat(),
    })
