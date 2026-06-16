# etl_pipeline/etl_pipeline/assets/user_gold.py

from dagster import asset, Output, MetadataValue, AssetIn
import polars as pl
import json
from datetime import datetime

# ── helper function to parse conversation messages ───────────────────────────
def parse_conversation_messages(messages_json_str):
    try:
        messages = json.loads(messages_json_str)
        if not messages:
            return 0.0, 0.0
        latencies = [int(m.get("elapsed_ms", 0)) for m in messages if m.get("elapsed_ms") is not None]
        avg_latency = sum(latencies) / len(latencies) if latencies else 0.0
        
        zero_queries = sum(1 for m in messages if m.get("is_zero", False))
        zero_ratio = zero_queries / len(messages) if messages else 0.0
        
        return float(avg_latency), float(zero_ratio)
    except Exception:
        return 0.0, 0.0

# ── helper function to write to PostgreSQL ──────────────────────────────────
def write_to_postgres(df_gold: pl.DataFrame, table_name: str, context):
    try:
        import sqlalchemy
        engine = sqlalchemy.create_engine("postgresql+psycopg2://superset:superset@superset-db:5432/superset")
        df_gold.to_pandas().to_sql(
            name=table_name,
            con=engine,
            if_exists="replace",
            index=False
        )
        context.log.info(f"✅ Successfully wrote {table_name} to PostgreSQL.")
    except Exception as e:
        context.log.error(f"❌ Error writing {table_name} to PostgreSQL: {e}")

# ── 1. Gold User Engagement ──────────────────────────────────────────────────
@asset(
    name="gold_user_engagement",
    key_prefix=["gold", "mongodb"],
    group_name="user_lakehouse",
    io_manager_key="minio_io_manager",
    compute_kind="python",
    ins={
        "silver_mongodb_users": AssetIn(key_prefix=["silver", "mongodb"]),
        "silver_mongodb_conversations": AssetIn(key_prefix=["silver", "mongodb"]),
        "silver_mongodb_events": AssetIn(key_prefix=["silver", "mongodb"])
    },
    description="Thống kê hiệu năng tương tác người dùng theo ngày (KPIs, Bounce Rate, Device info, Retention)"
)
def gold_user_engagement(
    context, 
    silver_mongodb_users: pl.DataFrame, 
    silver_mongodb_conversations: pl.DataFrame, 
    silver_mongodb_events: pl.DataFrame
) -> Output:
    
    # Handle empty tables safely
    if silver_mongodb_events.is_empty() and silver_mongodb_conversations.is_empty() and silver_mongodb_users.is_empty():
        df_gold = pl.DataFrame({
            "date": pl.Series([], dtype=pl.Utf8),
            "total_active_users": pl.Series([], dtype=pl.Int32),
            "new_registered_users": pl.Series([], dtype=pl.Int32),
            "total_page_views": pl.Series([], dtype=pl.Int32),
            "average_session_duration_sec": pl.Series([], dtype=pl.Float64),
            "bounce_rate_pct": pl.Series([], dtype=pl.Float64),
            "device_desktop_pct": pl.Series([], dtype=pl.Float64),
            "device_mobile_pct": pl.Series([], dtype=pl.Float64),
            "retention_rate_pct": pl.Series([], dtype=pl.Float64),
            "updated_at": pl.Series([], dtype=pl.Utf8)
        })
        return Output(value=df_gold, metadata={"total_records": 0})

    # 1. Parse dates (YYYY-MM-DD)
    df_events = silver_mongodb_events.with_columns(
        pl.col("timestamp").str.slice(0, 10).alias("date")
    ) if not silver_mongodb_events.is_empty() else pl.DataFrame(schema={"date": pl.Utf8, "user_id": pl.Utf8, "event_type": pl.Utf8, "device_type": pl.Utf8, "bounce": pl.Int32, "session_id": pl.Utf8})
    
    df_convs = silver_mongodb_conversations.with_columns(
        pl.col("start_time").str.slice(0, 10).alias("date")
    ) if not silver_mongodb_conversations.is_empty() else pl.DataFrame(schema={"date": pl.Utf8, "user_id": pl.Utf8, "session_duration_sec": pl.Float64, "session_id": pl.Utf8})
    
    df_users = silver_mongodb_users.with_columns(
        pl.col("created_at").str.slice(0, 10).alias("date")
    ) if not silver_mongodb_users.is_empty() else pl.DataFrame(schema={"date": pl.Utf8, "user_id": pl.Utf8})
    
    # Get all unique active dates
    dates_list = []
    if "date" in df_events.columns:
        dates_list.extend(df_events["date"].drop_nulls().to_list())
    if "date" in df_convs.columns:
        dates_list.extend(df_convs["date"].drop_nulls().to_list())
    if "date" in df_users.columns:
        dates_list.extend(df_users["date"].drop_nulls().to_list())
        
    unique_dates = sorted(list(set(dates_list)))
    
    rows = []
    for d in unique_dates:
        if not d:
            continue
            
        # 1. Active Users (both from events and conversations on date d)
        active_u = set()
        if not df_events.is_empty() and "date" in df_events.columns:
            active_u.update(df_events.filter(pl.col("date") == d)["user_id"].drop_nulls().to_list())
        if not df_convs.is_empty() and "date" in df_convs.columns:
            active_u.update(df_convs.filter(pl.col("date") == d)["user_id"].drop_nulls().to_list())
        
        # Remove empty or guest users if necessary, but keep unique ones
        active_u = {uid for uid in active_u if uid and uid != "None" and uid != "null" and uid != ""}
        total_active = len(active_u)
        
        # 2. New Registered Users on date d
        new_reg = 0
        if not df_users.is_empty() and "date" in df_users.columns:
            new_reg = df_users.filter(pl.col("date") == d)["user_id"].n_unique()
            
        # 3. Total Page Views on date d
        page_views = 0
        if not df_events.is_empty() and "date" in df_events.columns:
            page_views = df_events.filter((pl.col("date") == d) & (pl.col("event_type") == "page_view")).shape[0]
            
        # 4. Average Session Duration on date d
        avg_dur = 0.0
        if not df_convs.is_empty() and "date" in df_convs.columns:
            durations = df_convs.filter(pl.col("date") == d)["session_duration_sec"].drop_nulls().to_list()
            if durations:
                avg_dur = sum(durations) / len(durations)
                
        # 5. Bounce Rate on date d
        bounce_rate = 0.0
        if not df_events.is_empty() and "date" in df_events.columns:
            d_events = df_events.filter(pl.col("date") == d)
            total_sessions_on_day = d_events["session_id"].n_unique()
            if total_sessions_on_day > 0:
                # bounce can be true (1) if the event says bounce
                bounced_sessions = d_events.filter(pl.col("bounce") == 1)["session_id"].n_unique()
                bounce_rate = (bounced_sessions / total_sessions_on_day) * 100
                
        # 6. Device Distribution
        desktop_pct = 0.0
        mobile_pct = 0.0
        if not df_events.is_empty() and "date" in df_events.columns:
            d_events = df_events.filter(pl.col("date") == d)
            total_dev = d_events.shape[0]
            if total_dev > 0:
                desktops = d_events.filter(pl.col("device_type") == "desktop").shape[0]
                mobiles = d_events.filter(pl.col("device_type") == "mobile").shape[0]
                desktop_pct = (desktops / total_dev) * 100
                mobile_pct = (mobiles / total_dev) * 100
                
        # 7. Retention Rate
        # Active users on day d who registered BEFORE day d
        retention_rate = 0.0
        if total_active > 0 and not df_users.is_empty():
            returning_users = 0
            for uid in active_u:
                # Find user registration date
                u_rec = df_users.filter(pl.col("user_id") == uid)
                if not u_rec.is_empty():
                    reg_date = u_rec["date"][0]
                    if reg_date and reg_date < d:
                        returning_users += 1
            retention_rate = (returning_users / total_active) * 100
            
        rows.append({
            "date": d,
            "total_active_users": int(total_active),
            "new_registered_users": int(new_reg),
            "total_page_views": int(page_views),
            "average_session_duration_sec": float(round(avg_dur, 2)),
            "bounce_rate_pct": float(round(bounce_rate, 2)),
            "device_desktop_pct": float(round(desktop_pct, 2)),
            "device_mobile_pct": float(round(mobile_pct, 2)),
            "retention_rate_pct": float(round(retention_rate, 2)),
            "updated_at": datetime.utcnow().isoformat()
        })
        
    df_gold = pl.DataFrame(rows) if rows else pl.DataFrame({
        "date": pl.Series([], dtype=pl.Utf8),
        "total_active_users": pl.Series([], dtype=pl.Int32),
        "new_registered_users": pl.Series([], dtype=pl.Int32),
        "total_page_views": pl.Series([], dtype=pl.Int32),
        "average_session_duration_sec": pl.Series([], dtype=pl.Float64),
        "bounce_rate_pct": pl.Series([], dtype=pl.Float64),
        "device_desktop_pct": pl.Series([], dtype=pl.Float64),
        "device_mobile_pct": pl.Series([], dtype=pl.Float64),
        "retention_rate_pct": pl.Series([], dtype=pl.Float64),
        "updated_at": pl.Series([], dtype=pl.Utf8)
    })
    
    context.log.info(f"✅ Đã tổng hợp chỉ số tương tác cho {df_gold.shape[0]} ngày.")
    write_to_postgres(df_gold, "gold_user_engagement", context)
    
    return Output(
        value=df_gold,
        metadata={
            "total_records": MetadataValue.int(df_gold.shape[0]),
            "gold_time": datetime.utcnow().isoformat()
        }
    )

# ── 2. Gold Chat Performance ─────────────────────────────────────────────────
@asset(
    name="gold_chat_performance",
    key_prefix=["gold", "mongodb"],
    group_name="user_lakehouse",
    io_manager_key="minio_io_manager",
    compute_kind="python",
    ins={
        "silver_mongodb_users": AssetIn(key_prefix=["silver", "mongodb"]),
        "silver_mongodb_conversations": AssetIn(key_prefix=["silver", "mongodb"])
    },
    description="Đánh giá hiệu năng của chatbot AI, thống kê feedback rating và độ trễ phản hồi"
)
def gold_chat_performance(
    context, 
    silver_mongodb_users: pl.DataFrame, 
    silver_mongodb_conversations: pl.DataFrame
) -> Output:
    
    if silver_mongodb_conversations.is_empty():
        df_gold = pl.DataFrame({
            "session_id": pl.Series([], dtype=pl.Utf8),
            "user_id": pl.Series([], dtype=pl.Utf8),
            "user_age": pl.Series([], dtype=pl.Int32),
            "user_gender": pl.Series([], dtype=pl.Utf8),
            "session_start_time": pl.Series([], dtype=pl.Utf8),
            "total_messages_exchanged": pl.Series([], dtype=pl.Int32),
            "average_latency_ms": pl.Series([], dtype=pl.Float64),
            "feedback_rating": pl.Series([], dtype=pl.Int32),
            "is_zero_query_ratio": pl.Series([], dtype=pl.Float64),
            "updated_at": pl.Series([], dtype=pl.Utf8)
        })
        return Output(value=df_gold, metadata={"total_records": 0})

    rows = []
    for row in silver_mongodb_conversations.iter_rows(named=True):
        session_id = row["session_id"]
        user_id = row["user_id"]
        
        # User details lookup
        age = None
        gender = None
        if user_id and user_id != "None" and user_id != "null" and user_id != "" and not silver_mongodb_users.is_empty():
            u_df = silver_mongodb_users.filter(pl.col("user_id") == user_id)
            if not u_df.is_empty():
                age = u_df["age"][0]
                gender = u_df["gender"][0]
                
        # Parse nested messages_json details
        avg_latency, zero_ratio = parse_conversation_messages(row.get("messages_json", "[]"))
        
        # Rating correction
        rating = row.get("feedback_rating")
        rating_val = int(rating) if rating is not None else None
        
        rows.append({
            "session_id": session_id,
            "user_id": user_id if user_id and user_id != "None" else None,
            "user_age": age,
            "user_gender": gender,
            "session_start_time": row.get("start_time"),
            "total_messages_exchanged": int(row.get("total_messages", 0)),
            "average_latency_ms": float(round(avg_latency, 2)),
            "feedback_rating": rating_val,
            "is_zero_query_ratio": float(round(zero_ratio, 2)),
            "updated_at": datetime.utcnow().isoformat()
        })
        
    df_gold = pl.DataFrame(rows)
    context.log.info(f"✅ Đã xử lý chat performance cho {df_gold.shape[0]} sessions.")
    write_to_postgres(df_gold, "gold_chat_performance", context)
    
    return Output(
        value=df_gold,
        metadata={
            "total_records": MetadataValue.int(df_gold.shape[0]),
            "gold_time": datetime.utcnow().isoformat()
        }
    )

# ── 3. Gold Medical Insights ──────────────────────────────────────────────────
@asset(
    name="gold_medical_insights",
    key_prefix=["gold", "mongodb"],
    group_name="user_lakehouse",
    io_manager_key="minio_io_manager",
    compute_kind="python",
    ins={
        "silver_mongodb_users": AssetIn(key_prefix=["silver", "mongodb"]),
        "silver_mongodb_events": AssetIn(key_prefix=["silver", "mongodb"]),
        "silver_mongodb_medical_logs": AssetIn(key_prefix=["silver", "mongodb"])
    },
    description="Khai phá xu hướng dịch tễ học và thuốc y văn cổ truyền từ hội thoại người bệnh"
)
def gold_medical_insights(
    context, 
    silver_mongodb_users: pl.DataFrame, 
    silver_mongodb_events: pl.DataFrame, 
    silver_mongodb_medical_logs: pl.DataFrame
) -> Output:
    
    if silver_mongodb_medical_logs.is_empty():
        df_gold = pl.DataFrame({
            "log_id": pl.Series([], dtype=pl.Utf8),
            "session_id": pl.Series([], dtype=pl.Utf8),
            "user_id": pl.Series([], dtype=pl.Utf8),
            "user_age": pl.Series([], dtype=pl.Int32),
            "user_gender": pl.Series([], dtype=pl.Utf8),
            "user_city": pl.Series([], dtype=pl.Utf8),
            "user_country": pl.Series([], dtype=pl.Utf8),
            "symptoms_list": pl.Series([], dtype=pl.Utf8),
            "diseases_list": pl.Series([], dtype=pl.Utf8),
            "herbs_list": pl.Series([], dtype=pl.Utf8),
            "body_parts_list": pl.Series([], dtype=pl.Utf8),
            "timestamp": pl.Series([], dtype=pl.Utf8),
            "updated_at": pl.Series([], dtype=pl.Utf8)
        })
        return Output(value=df_gold, metadata={"total_records": 0})
        
    # Get geolocation data grouped by session_id
    geo_map = {}
    if not silver_mongodb_events.is_empty():
        # filter records with valid country/city
        df_valid_geo = silver_mongodb_events.filter(
            pl.col("country").is_not_null() & (pl.col("country") != "") & 
            pl.col("city").is_not_null() & (pl.col("city") != "")
        )
        if not df_valid_geo.is_empty():
            for row in df_valid_geo.iter_rows(named=True):
                sid = row["session_id"]
                if sid not in geo_map:
                    geo_map[sid] = (row["country"], row["city"])
                    
    rows = []
    for row in silver_mongodb_medical_logs.iter_rows(named=True):
        log_id = row["log_id"]
        session_id = row["session_id"]
        user_id = row["user_id"]
        
        # User details lookup
        age = None
        gender = None
        if user_id and user_id != "None" and user_id != "null" and user_id != "" and not silver_mongodb_users.is_empty():
            u_df = silver_mongodb_users.filter(pl.col("user_id") == user_id)
            if not u_df.is_empty():
                age = u_df["age"][0]
                gender = u_df["gender"][0]
                
        # Geolocation lookup
        country, city = geo_map.get(session_id, ("Vietnam", "Ho Chi Minh City"))
        
        # Format the arrays as clean JSON strings or string representation
        # Polars write_parquet will store lists nicely if we convert them to actual list of strings
        try:
            symptoms = json.loads(row.get("symptoms_mentioned", "[]"))
            diseases = json.loads(row.get("diseases_mentioned", "[]"))
            herbs = json.loads(row.get("herbs_queried", "[]"))
            body_parts = json.loads(row.get("body_parts_mentioned", "[]"))
        except Exception:
            symptoms = []
            diseases = []
            herbs = []
            body_parts = []
            
        rows.append({
            "log_id": log_id,
            "session_id": session_id,
            "user_id": user_id if user_id and user_id != "None" else None,
            "user_age": age,
            "user_gender": gender,
            "user_city": city,
            "user_country": country,
            "symptoms_list": json.dumps(symptoms, ensure_ascii=False),
            "diseases_list": json.dumps(diseases, ensure_ascii=False),
            "herbs_list": json.dumps(herbs, ensure_ascii=False),
            "body_parts_list": json.dumps(body_parts, ensure_ascii=False),
            "timestamp": row.get("timestamp"),
            "updated_at": datetime.utcnow().isoformat()
        })
        
    df_gold = pl.DataFrame(rows)
    context.log.info(f"✅ Đã xử lý y tế insights cho {df_gold.shape[0]} logs.")
    write_to_postgres(df_gold, "gold_medical_insights", context)
    
    return Output(
        value=df_gold,
        metadata={
            "total_records": MetadataValue.int(df_gold.shape[0]),
            "gold_time": datetime.utcnow().isoformat()
        }
    )
