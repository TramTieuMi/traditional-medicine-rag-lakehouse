# etl_pipeline/etl_pipeline/assets/gold_mongodb_sync.py

from dagster import asset, Output, MetadataValue
import polars as pl
from pymongo import MongoClient
import os
from datetime import datetime

MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongodb:27017/yhct_db")

def get_mongo_db():
    client = MongoClient(MONGO_URI)
    return client.get_database()

# ── 1. Sync Users ─────────────────────────────────────────────────────────────
@asset(
    name="gold_mongodb_users",
    key_prefix=["gold", "users"],
    group_name="gold_sync",
    io_manager_key="minio_io_manager",
    compute_kind="python",
    description="Sync dữ liệu người dùng từ MongoDB sang Gold layer (Parquet)"
)
def gold_mongodb_users(context) -> Output:
    db = get_mongo_db()
    users_col = db.get_collection("users")
    
    users_cursor = users_col.find()
    rows = []
    
    for u in users_cursor:
        rows.append({
            "user_id": str(u["_id"]),
            "full_name": u.get("full_name", ""),
            "email": u.get("email", ""),
            "age": int(u.get("age", 0)),
            "gender": u.get("gender", "khác"),
            "created_at": u.get("created_at", datetime.utcnow()).isoformat(),
            "last_login_at": u.get("last_login_at", datetime.utcnow()).isoformat()
        })
        
    if not rows:
        # Fallback empty df
        df = pl.DataFrame({
            "user_id": pl.Series([], dtype=pl.Utf8),
            "full_name": pl.Series([], dtype=pl.Utf8),
            "email": pl.Series([], dtype=pl.Utf8),
            "age": pl.Series([], dtype=pl.Int32),
            "gender": pl.Series([], dtype=pl.Utf8),
            "created_at": pl.Series([], dtype=pl.Utf8),
            "last_login_at": pl.Series([], dtype=pl.Utf8)
        })
    else:
        df = pl.DataFrame(rows)
        
    context.log.info(f"✅ Đã đồng bộ {df.shape[0]} người dùng từ MongoDB.")
    
    return Output(
        value=df,
        metadata={
            "total_users": MetadataValue.int(df.shape[0]),
            "sync_time": datetime.utcnow().isoformat()
        }
    )

# ── 2. Sync Conversations ─────────────────────────────────────────────────────
@asset(
    name="gold_mongodb_sessions",
    key_prefix=["gold", "sessions"],
    group_name="gold_sync",
    io_manager_key="minio_io_manager",
    compute_kind="python",
    description="Sync dữ liệu phiên chat (sessions) từ MongoDB sang Gold layer (Parquet)"
)
def gold_mongodb_sessions(context) -> Output:
    db = get_mongo_db()
    conv_col = db.get_collection("conversations")
    
    conv_cursor = conv_col.find()
    rows = []
    
    for c in conv_cursor:
        rows.append({
            "session_id": c.get("session_id", ""),
            "user_id": str(c.get("user_id", "")),
            "start_time": c.get("start_time", datetime.utcnow()).isoformat(),
            "total_messages": int(c.get("total_messages", 0)),
            "session_duration_sec": float(c.get("session_duration_sec", 0.0)),
            "feedback_rating": int(c["feedback_rating"]) if c.get("feedback_rating") is not None else -1
        })
        
    if not rows:
        df = pl.DataFrame({
            "session_id": pl.Series([], dtype=pl.Utf8),
            "user_id": pl.Series([], dtype=pl.Utf8),
            "start_time": pl.Series([], dtype=pl.Utf8),
            "total_messages": pl.Series([], dtype=pl.Int32),
            "session_duration_sec": pl.Series([], dtype=pl.Float64),
            "feedback_rating": pl.Series([], dtype=pl.Int32)
        })
    else:
        df = pl.DataFrame(rows)
        
    context.log.info(f"✅ Đã đồng bộ {df.shape[0]} phiên chat từ MongoDB.")
    
    return Output(
        value=df,
        metadata={
            "total_sessions": MetadataValue.int(df.shape[0]),
            "sync_time": datetime.utcnow().isoformat()
        }
    )

# ── 3. Sync Analytics Events ──────────────────────────────────────────────────
@asset(
    name="gold_mongodb_events",
    key_prefix=["gold", "events"],
    group_name="gold_sync",
    io_manager_key="minio_io_manager",
    compute_kind="python",
    description="Sync dữ liệu analytics events từ MongoDB sang Gold layer (Parquet)"
)
def gold_mongodb_events(context) -> Output:
    db = get_mongo_db()
    events_col = db.get_collection("analyticsevents")
    
    events_cursor = events_col.find()
    rows = []
    
    for e in events_cursor:
        rows.append({
            "event_id": str(e["_id"]),
            "session_id": e.get("session_id", ""),
            "user_id": str(e.get("user_id", "")) if e.get("user_id") else "",
            "event_type": e.get("event_type", ""),
            "timestamp": e.get("timestamp", datetime.utcnow()).isoformat(),
            "device_type": e.get("device_type", "desktop"),
            "browser": e.get("browser", "Unknown"),
            "os": e.get("os", "Unknown"),
            "ip_address": e.get("ip_address", ""),
            "country": e.get("country", "Vietnam"),
            "city": e.get("city", "Unknown"),
            "referrer_url": e.get("referrer_url", ""),
            "route": e.get("route", ""),
            "button_name": e.get("button_name") or "",
            "bounce": int(e.get("bounce", False)),
            "utm_source": e.get("utm_source") or "",
            "utm_medium": e.get("utm_medium") or ""
        })
        
    if not rows:
        df = pl.DataFrame({
            "event_id": pl.Series([], dtype=pl.Utf8),
            "session_id": pl.Series([], dtype=pl.Utf8),
            "user_id": pl.Series([], dtype=pl.Utf8),
            "event_type": pl.Series([], dtype=pl.Utf8),
            "timestamp": pl.Series([], dtype=pl.Utf8),
            "device_type": pl.Series([], dtype=pl.Utf8),
            "browser": pl.Series([], dtype=pl.Utf8),
            "os": pl.Series([], dtype=pl.Utf8),
            "ip_address": pl.Series([], dtype=pl.Utf8),
            "country": pl.Series([], dtype=pl.Utf8),
            "city": pl.Series([], dtype=pl.Utf8),
            "referrer_url": pl.Series([], dtype=pl.Utf8),
            "route": pl.Series([], dtype=pl.Utf8),
            "button_name": pl.Series([], dtype=pl.Utf8),
            "bounce": pl.Series([], dtype=pl.Int32),
            "utm_source": pl.Series([], dtype=pl.Utf8),
            "utm_medium": pl.Series([], dtype=pl.Utf8)
        })
    else:
        df = pl.DataFrame(rows)
        
    context.log.info(f"✅ Đã đồng bộ {df.shape[0]} sự kiện từ MongoDB.")
    
    return Output(
        value=df,
        metadata={
            "total_events": MetadataValue.int(df.shape[0]),
            "sync_time": datetime.utcnow().isoformat()
        }
    )
