# etl_pipeline/etl_pipeline/assets/user_silver.py

from dagster import asset, Output, MetadataValue, AssetIn
import polars as pl
import hashlib
import json
from datetime import datetime

# ── helper function for masking email ──────────────────────────────────────────
def mask_email_val(email_str: str) -> str:
    if not email_str:
        return ""
    email_clean = email_str.strip().lower()
    return hashlib.sha256(email_clean.encode("utf-8")).hexdigest()

# ── helper function to resolve IP Address ──────────────────────────────────────
def clean_geo(ip: str, existing_country: str, existing_city: str) -> tuple[str, str]:
    # Standardize local/private IP addresses to a default location for demo purposes
    local_ips = ["127.0.0.1", "::1", "localhost", "::ffff:172.18.0.1", "172.18.0.1"]
    ip_str = str(ip).strip()
    
    if any(local in ip_str for local in local_ips) or ip_str == "" or ip_str == "None":
        country = "Vietnam"
        city = "Ho Chi Minh City"
    else:
        country = existing_country if existing_country and existing_country != "Unknown" else "Vietnam"
        city = existing_city if existing_city and existing_city != "Unknown" else "Ho Chi Minh City"
        
    return country, city

# ── 1. Silver Users ──────────────────────────────────────────────────────────
@asset(
    name="silver_mongodb_users",
    key_prefix=["silver", "mongodb"],
    group_name="user_lakehouse",
    io_manager_key="minio_io_manager",
    compute_kind="python",
    ins={"bronze_mongodb_users": AssetIn(key_prefix=["bronze", "mongodb"])},
    description="Làm sạch, ẩn danh thông tin email (PII) bằng SHA256 và lưu xuống Silver layer"
)
def silver_mongodb_users(context, bronze_mongodb_users: pl.DataFrame) -> Output:
    context.log.info(f"📥 Đọc {bronze_mongodb_users.shape[0]} dòng từ Bronze Users.")
    
    if bronze_mongodb_users.is_empty():
        df_silver = bronze_mongodb_users
    else:
        # Deduplicate users by user_id
        df_dedup = bronze_mongodb_users.unique(subset=["user_id"], keep="last")

        # Use pre-computed email_sha256 from Bronze (rename to email_hashed for compat)
        # Raw email is dropped here — PII stays in Bronze only
        df_silver = df_dedup.rename({"email_sha256": "email_hashed"}).with_columns([
            pl.col("age").fill_null(0).cast(pl.Int32),
            pl.col("gender").fill_null("khác").str.to_lowercase()
        ]).drop("email")
        
    context.log.info(f"✅ Đã làm sạch và ẩn danh thông tin. Dòng còn lại: {df_silver.shape[0]}")
    
    return Output(
        value=df_silver,
        metadata={
            "total_records": MetadataValue.int(df_silver.shape[0]),
            "removed_duplicates": MetadataValue.int(bronze_mongodb_users.shape[0] - df_silver.shape[0]),
            "silver_time": datetime.utcnow().isoformat()
        }
    )

# ── 2. Silver Conversations ───────────────────────────────────────────────────
@asset(
    name="silver_mongodb_conversations",
    key_prefix=["silver", "mongodb"],
    group_name="user_lakehouse",
    io_manager_key="minio_io_manager",
    compute_kind="python",
    ins={"bronze_mongodb_conversations": AssetIn(key_prefix=["bronze", "mongodb"])},
    description="Chuẩn hóa dữ liệu cuộc trò chuyện và làm sạch chỉ số đánh giá phản hồi"
)
def silver_mongodb_conversations(context, bronze_mongodb_conversations: pl.DataFrame) -> Output:
    context.log.info(f"📥 Đọc {bronze_mongodb_conversations.shape[0]} dòng từ Bronze Conversations.")
    
    if bronze_mongodb_conversations.is_empty():
        df_silver = bronze_mongodb_conversations
    else:
        # Deduplicate conversations by session_id
        df_dedup = bronze_mongodb_conversations.unique(subset=["session_id"], keep="last")
        
        # Clean rating: feedback_rating should be null if -1, otherwise keep it
        df_silver = df_dedup.with_columns([
            pl.when(pl.col("feedback_rating") == -1)
            .then(None)
            .otherwise(pl.col("feedback_rating"))
            .alias("feedback_rating"),
            pl.col("total_messages").fill_null(0).cast(pl.Int32),
            pl.col("session_duration_sec").fill_null(0.0).cast(pl.Float64)
        ])
        
    context.log.info(f"✅ Đã chuẩn hóa cuộc trò chuyện. Dòng còn lại: {df_silver.shape[0]}")
    
    return Output(
        value=df_silver,
        metadata={
            "total_records": MetadataValue.int(df_silver.shape[0]),
            "removed_duplicates": MetadataValue.int(bronze_mongodb_conversations.shape[0] - df_silver.shape[0]),
            "silver_time": datetime.utcnow().isoformat()
        }
    )

# ── 3. Silver Events ──────────────────────────────────────────────────────────
@asset(
    name="silver_mongodb_events",
    key_prefix=["silver", "mongodb"],
    group_name="user_lakehouse",
    io_manager_key="minio_io_manager",
    compute_kind="python",
    ins={"bronze_mongodb_events": AssetIn(key_prefix=["bronze", "mongodb"])},
    description="Giải mã và phân giải IP address người dùng sang địa lý sạch, làm sạch User-Agent"
)
def silver_mongodb_events(context, bronze_mongodb_events: pl.DataFrame) -> Output:
    context.log.info(f"📥 Đọc {bronze_mongodb_events.shape[0]} dòng từ Bronze Events.")
    
    if bronze_mongodb_events.is_empty():
        df_silver = bronze_mongodb_events
    else:
        # Deduplicate tracking events by event_id
        df_dedup = bronze_mongodb_events.unique(subset=["event_id"], keep="last")
        
        # Geolocation parsing logic
        countries = []
        cities = []
        for ip, country, city in zip(df_dedup["ip_address"].to_list(), df_dedup["country"].to_list(), df_dedup["city"].to_list()):
            c, t = clean_geo(ip, country, city)
            countries.append(c)
            cities.append(t)
            
        df_silver = df_dedup.with_columns([
            pl.Series("country", countries, dtype=pl.Utf8),
            pl.Series("city", cities, dtype=pl.Utf8),
            pl.col("device_type").fill_null("desktop").str.to_lowercase(),
            pl.col("browser").fill_null("Unknown"),
            pl.col("os").fill_null("Unknown")
        ])
        
    context.log.info(f"✅ Đã phân tích địa lý và User-Agent. Dòng còn lại: {df_silver.shape[0]}")
    
    return Output(
        value=df_silver,
        metadata={
            "total_records": MetadataValue.int(df_silver.shape[0]),
            "removed_duplicates": MetadataValue.int(bronze_mongodb_events.shape[0] - df_silver.shape[0]),
            "silver_time": datetime.utcnow().isoformat()
        }
    )

# ── 4. Silver Medical Logs ────────────────────────────────────────────────────
@asset(
    name="silver_mongodb_medical_logs",
    key_prefix=["silver", "mongodb"],
    group_name="user_lakehouse",
    io_manager_key="minio_io_manager",
    compute_kind="python",
    ins={"bronze_mongodb_medical_logs": AssetIn(key_prefix=["bronze", "mongodb"])},
    description="Làm sạch và cấu trúc hóa các danh sách thực thể y học cổ truyền"
)
def silver_mongodb_medical_logs(context, bronze_mongodb_medical_logs: pl.DataFrame) -> Output:
    context.log.info(f"📥 Đọc {bronze_mongodb_medical_logs.shape[0]} dòng từ Bronze Medical Logs.")
    
    if bronze_mongodb_medical_logs.is_empty():
        df_silver = bronze_mongodb_medical_logs
    else:
        # Deduplicate medical logs by log_id
        df_silver = bronze_mongodb_medical_logs.unique(subset=["log_id"], keep="last")
        
    context.log.info(f"✅ Đã chuẩn hóa danh sách thực thể y tế. Dòng còn lại: {df_silver.shape[0]}")
    
    return Output(
        value=df_silver,
        metadata={
            "total_records": MetadataValue.int(df_silver.shape[0]),
            "silver_time": datetime.utcnow().isoformat()
        }
    )
