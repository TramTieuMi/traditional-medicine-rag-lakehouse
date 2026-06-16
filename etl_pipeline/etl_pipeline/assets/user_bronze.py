# etl_pipeline/etl_pipeline/assets/user_bronze.py

from dagster import asset, Output, MetadataValue
import polars as pl
from pymongo import MongoClient
import os
import json
from datetime import datetime

MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongodb:27017/yhct_db")

def get_mongo_db():
    client = MongoClient(MONGO_URI)
    return client.get_database()

# ── 1. Bronze Users ──────────────────────────────────────────────────────────
@asset(
    name="bronze_mongodb_users",
    key_prefix=["bronze", "mongodb"],
    group_name="user_lakehouse",
    io_manager_key="minio_io_manager",
    compute_kind="python",
    description="Nạp thô dữ liệu người dùng từ MongoDB sang Bronze layer (Parquet)"
)
def bronze_mongodb_users(context) -> Output:
    db = get_mongo_db()
    users_col = db.get_collection("users")
    
    users_cursor = users_col.find()
    rows = []
    
    for u in users_cursor:
        rows.append({
            "user_id": str(u["_id"]),
            "full_name": u.get("full_name", ""),
            "email": u.get("email", ""),
            "age": int(u.get("age", 0)) if u.get("age") is not None else 0,
            "gender": u.get("gender", "khác"),
            "created_at": u.get("created_at", datetime.utcnow()).isoformat() if isinstance(u.get("created_at"), datetime) else str(u.get("created_at", "")),
            "last_login_at": u.get("last_login_at", datetime.utcnow()).isoformat() if isinstance(u.get("last_login_at"), datetime) else str(u.get("last_login_at", ""))
        })
        
    if not rows:
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
        
    context.log.info(f"✅ Đã đồng bộ {df.shape[0]} người dùng từ MongoDB sang Bronze.")
    
    return Output(
        value=df,
        metadata={
            "total_records": MetadataValue.int(df.shape[0]),
            "sync_time": datetime.utcnow().isoformat()
        }
    )

# ── 2. Bronze Conversations ───────────────────────────────────────────────────
@asset(
    name="bronze_mongodb_conversations",
    key_prefix=["bronze", "mongodb"],
    group_name="user_lakehouse",
    io_manager_key="minio_io_manager",
    compute_kind="python",
    description="Nạp thô dữ liệu phiên hội thoại từ MongoDB sang Bronze layer (Parquet)"
)
def bronze_mongodb_conversations(context) -> Output:
    db = get_mongo_db()
    conv_col = db.get_collection("conversations")
    
    conv_cursor = conv_col.find()
    rows = []
    
    for c in conv_cursor:
        # Serialize nested messages to JSON string to prevent schema complexities in Parquet
        messages_raw = c.get("messages", [])
        messages_json = []
        for msg in messages_raw:
            messages_json.append({
                "message_content": msg.get("message_content", ""),
                "ai_response": msg.get("ai_response", ""),
                "timestamp": msg.get("timestamp", datetime.utcnow()).isoformat() if isinstance(msg.get("timestamp"), datetime) else str(msg.get("timestamp", "")),
                "elapsed_ms": int(msg.get("elapsed_ms", 0)) if msg.get("elapsed_ms") is not None else 0,
                "is_zero": bool(msg.get("is_zero", False)),
                "sources": msg.get("sources", []),
                "sims": msg.get("sims", []),
                "metadatas": [str(m) for m in msg.get("metadatas", [])]
            })
            
        rows.append({
            "conversation_id": str(c["_id"]),
            "session_id": c.get("session_id", ""),
            "user_id": str(c.get("user_id", "")),
            "start_time": c.get("start_time", datetime.utcnow()).isoformat() if isinstance(c.get("start_time"), datetime) else str(c.get("start_time", "")),
            "total_messages": int(c.get("total_messages", 0)) if c.get("total_messages") is not None else 0,
            "session_duration_sec": float(c.get("session_duration_sec", 0.0)) if c.get("session_duration_sec") is not None else 0.0,
            "feedback_rating": int(c["feedback_rating"]) if c.get("feedback_rating") is not None else -1,
            "messages_json": json.dumps(messages_json, ensure_ascii=False)
        })
        
    if not rows:
        df = pl.DataFrame({
            "conversation_id": pl.Series([], dtype=pl.Utf8),
            "session_id": pl.Series([], dtype=pl.Utf8),
            "user_id": pl.Series([], dtype=pl.Utf8),
            "start_time": pl.Series([], dtype=pl.Utf8),
            "total_messages": pl.Series([], dtype=pl.Int32),
            "session_duration_sec": pl.Series([], dtype=pl.Float64),
            "feedback_rating": pl.Series([], dtype=pl.Int32),
            "messages_json": pl.Series([], dtype=pl.Utf8)
        })
    else:
        df = pl.DataFrame(rows)
        
    context.log.info(f"✅ Đã đồng bộ {df.shape[0]} phiên hội thoại từ MongoDB sang Bronze.")
    
    return Output(
        value=df,
        metadata={
            "total_records": MetadataValue.int(df.shape[0]),
            "sync_time": datetime.utcnow().isoformat()
        }
    )

# ── 3. Bronze Events ──────────────────────────────────────────────────────────
@asset(
    name="bronze_mongodb_events",
    key_prefix=["bronze", "mongodb"],
    group_name="user_lakehouse",
    io_manager_key="minio_io_manager",
    compute_kind="python",
    description="Nạp thô dữ liệu sự kiện tương tác từ MongoDB sang Bronze layer (Parquet)"
)
def bronze_mongodb_events(context) -> Output:
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
            "timestamp": e.get("timestamp", datetime.utcnow()).isoformat() if isinstance(e.get("timestamp"), datetime) else str(e.get("timestamp", "")),
            "device_type": e.get("device_type", "desktop"),
            "browser": e.get("browser", "Unknown"),
            "os": e.get("os", "Unknown"),
            "ip_address": e.get("ip_address", ""),
            "country": e.get("country", "Vietnam"),
            "city": e.get("city", "Unknown"),
            "referrer_url": e.get("referrer_url", ""),
            "route": e.get("route", ""),
            "button_name": e.get("button_name") or "",
            "search_keywords": json.dumps(e.get("search_keywords", [])),
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
            "search_keywords": pl.Series([], dtype=pl.Utf8),
            "bounce": pl.Series([], dtype=pl.Int32),
            "utm_source": pl.Series([], dtype=pl.Utf8),
            "utm_medium": pl.Series([], dtype=pl.Utf8)
        })
    else:
        df = pl.DataFrame(rows)
        
    context.log.info(f"✅ Đã đồng bộ {df.shape[0]} sự kiện từ MongoDB sang Bronze.")
    
    return Output(
        value=df,
        metadata={
            "total_records": MetadataValue.int(df.shape[0]),
            "sync_time": datetime.utcnow().isoformat()
        }
    )

# ── 4. Bronze Medical Logs ────────────────────────────────────────────────────
@asset(
    name="bronze_mongodb_medical_logs",
    key_prefix=["bronze", "mongodb"],
    group_name="user_lakehouse",
    io_manager_key="minio_io_manager",
    compute_kind="python",
    description="Nạp thô dữ liệu thực thể y tế từ MongoDB sang Bronze layer (Parquet)"
)
def bronze_mongodb_medical_logs(context) -> Output:
    db = get_mongo_db()
    logs_col = db.get_collection("medicalentitylogs")
    
    logs_cursor = logs_col.find()
    rows = []
    
    for l in logs_cursor:
        rows.append({
            "log_id": str(l["_id"]),
            "session_id": l.get("session_id", ""),
            "user_id": str(l.get("user_id", "")),
            "timestamp": l.get("timestamp", datetime.utcnow()).isoformat() if isinstance(l.get("timestamp"), datetime) else str(l.get("timestamp", "")),
            "symptoms_mentioned": json.dumps(l.get("symptoms_mentioned", [])),
            "diseases_mentioned": json.dumps(l.get("diseases_mentioned", [])),
            "body_parts_mentioned": json.dumps(l.get("body_parts_mentioned", [])),
            "herbs_queried": json.dumps(l.get("herbs_queried", []))
        })
        
    if not rows:
        df = pl.DataFrame({
            "log_id": pl.Series([], dtype=pl.Utf8),
            "session_id": pl.Series([], dtype=pl.Utf8),
            "user_id": pl.Series([], dtype=pl.Utf8),
            "timestamp": pl.Series([], dtype=pl.Utf8),
            "symptoms_mentioned": pl.Series([], dtype=pl.Utf8),
            "diseases_mentioned": pl.Series([], dtype=pl.Utf8),
            "body_parts_mentioned": pl.Series([], dtype=pl.Utf8),
            "herbs_queried": pl.Series([], dtype=pl.Utf8)
        })
    else:
        df = pl.DataFrame(rows)
        
    context.log.info(f"✅ Đã đồng bộ {df.shape[0]} thực thể y tế từ MongoDB sang Bronze.")
    
    return Output(
        value=df,
        metadata={
            "total_records": MetadataValue.int(df.shape[0]),
            "sync_time": datetime.utcnow().isoformat()
        }
    )
