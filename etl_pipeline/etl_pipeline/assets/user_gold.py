# etl_pipeline/etl_pipeline/assets/user_gold.py

import json
import os
from datetime import datetime

import polars as pl
from dagster import asset, Output, MetadataValue, AssetIn

SUPERSET_DB_URI = os.getenv(
    "SUPERSET_DB_URI",
    "postgresql+psycopg2://superset:superset@superset-db:5432/superset",
)


def parse_conversation_messages(messages_json_str):
    try:
        messages = json.loads(messages_json_str)
        if not messages:
            return 0.0, 0.0
        latencies   = [int(m.get("elapsed_ms", 0)) for m in messages if m.get("elapsed_ms") is not None]
        avg_latency = sum(latencies) / len(latencies) if latencies else 0.0
        zero_queries = sum(1 for m in messages if m.get("is_zero", False))
        zero_ratio   = zero_queries / len(messages) if messages else 0.0
        return float(avg_latency), float(zero_ratio)
    except Exception:
        return 0.0, 0.0


def write_to_postgres(df_gold: pl.DataFrame, table_name: str, context):
    try:
        import sqlalchemy
        from sqlalchemy import Date, DateTime, text
        
        dtypes = {}
        if table_name == "gold_user_engagement":
            dtypes = {"date": Date}
        elif table_name == "gold_chat_performance":
            dtypes = {"session_start_time": DateTime}
        elif table_name == "gold_medical_insights":
            dtypes = {"timestamp": DateTime}

        engine = sqlalchemy.create_engine(SUPERSET_DB_URI)
        
        # Thử Truncate bảng trước để tránh lỗi Drop Table khi có View phụ thuộc
        try:
            with engine.begin() as conn:
                conn.execute(text(f"TRUNCATE TABLE {table_name}"))
            df_gold.to_pandas().to_sql(name=table_name, con=engine, if_exists="append", index=False, dtype=dtypes)
            context.log.info(f"✅ Đã ghi {table_name} (TRUNCATE + APPEND) vào PostgreSQL ({df_gold.shape[0]} rows).")
        except Exception as truncate_err:
            context.log.warning(f"⚠️ Truncate thất bại ({truncate_err}), chuyển sang replace...")
            df_gold.to_pandas().to_sql(name=table_name, con=engine, if_exists="replace", index=False, dtype=dtypes)
            context.log.info(f"✅ Đã ghi {table_name} (REPLACE) vào PostgreSQL ({df_gold.shape[0]} rows).")
            
    except Exception as e:
        context.log.error(f"❌ Lỗi ghi {table_name} vào PostgreSQL: {e}")


# ── 1. Gold User Engagement ──────────────────────────────────────────────────
@asset(
    name="gold_user_engagement",
    key_prefix=["gold", "mongodb"],
    group_name="user_lakehouse",
    io_manager_key="minio_io_manager",
    compute_kind="python",
    ins={
        "silver_mongodb_users":         AssetIn(key_prefix=["silver", "mongodb"]),
        "silver_mongodb_conversations": AssetIn(key_prefix=["silver", "mongodb"]),
        "silver_mongodb_events":        AssetIn(key_prefix=["silver", "mongodb"]),
    },
    description="Thống kê hiệu năng tương tác người dùng theo ngày (KPIs, Bounce Rate, Device info, Retention)",
)
def gold_user_engagement(
    context,
    silver_mongodb_users: pl.DataFrame,
    silver_mongodb_conversations: pl.DataFrame,
    silver_mongodb_events: pl.DataFrame,
) -> Output:

    _empty = pl.DataFrame({
        "date":                          pl.Series([], dtype=pl.Utf8),
        "total_active_users":            pl.Series([], dtype=pl.Int32),
        "new_registered_users":          pl.Series([], dtype=pl.Int32),
        "total_page_views":              pl.Series([], dtype=pl.Int32),
        "average_session_duration_sec":  pl.Series([], dtype=pl.Float64),
        "bounce_rate_pct":               pl.Series([], dtype=pl.Float64),
        "device_desktop_pct":            pl.Series([], dtype=pl.Float64),
        "device_mobile_pct":             pl.Series([], dtype=pl.Float64),
        "retention_rate_pct":            pl.Series([], dtype=pl.Float64),
        "updated_at":                    pl.Series([], dtype=pl.Utf8),
    })

    if silver_mongodb_events.is_empty() and silver_mongodb_conversations.is_empty() and silver_mongodb_users.is_empty():
        return Output(value=_empty, metadata={"total_records": 0})

    _empty_ev_schema   = {"date": pl.Utf8, "user_id": pl.Utf8, "event_type": pl.Utf8, "device_type": pl.Utf8, "bounce": pl.Int32, "session_id": pl.Utf8}
    _empty_conv_schema = {"date": pl.Utf8, "user_id": pl.Utf8, "session_duration_sec": pl.Float64, "session_id": pl.Utf8}
    _empty_user_schema = {"date": pl.Utf8, "user_id": pl.Utf8}

    df_events = (
        silver_mongodb_events.with_columns(pl.col("timestamp").str.slice(0, 10).alias("date"))
        if not silver_mongodb_events.is_empty()
        else pl.DataFrame(schema=_empty_ev_schema)
    )
    df_convs = (
        silver_mongodb_conversations.with_columns(pl.col("start_time").str.slice(0, 10).alias("date"))
        if not silver_mongodb_conversations.is_empty()
        else pl.DataFrame(schema=_empty_conv_schema)
    )
    df_users = (
        silver_mongodb_users.with_columns(pl.col("created_at").str.slice(0, 10).alias("date"))
        if not silver_mongodb_users.is_empty()
        else pl.DataFrame(schema=_empty_user_schema)
    )

    # Collect all unique dates
    dates_set: set[str] = set()
    for df, col in [(df_events, "date"), (df_convs, "date"), (df_users, "date")]:
        if "date" in df.columns:
            dates_set.update(v for v in df[col].drop_nulls().to_list() if v)
    unique_dates = sorted(dates_set)

    # Pre-build user registration date dict — O(U) once, avoids O(D×U²) nested loop
    user_reg_dates: dict[str, str] = {}
    if not df_users.is_empty() and "date" in df_users.columns:
        for row in df_users.iter_rows(named=True):
            uid = row.get("user_id", "")
            if uid and uid not in user_reg_dates:
                user_reg_dates[uid] = row.get("date", "")

    rows = []
    for d in unique_dates:
        # Active users: union of event + conversation user IDs on this date
        active_u: set[str] = set()
        if not df_events.is_empty() and "date" in df_events.columns:
            active_u.update(df_events.filter(pl.col("date") == d)["user_id"].drop_nulls().to_list())
        if not df_convs.is_empty() and "date" in df_convs.columns:
            active_u.update(df_convs.filter(pl.col("date") == d)["user_id"].drop_nulls().to_list())
        active_u = {uid for uid in active_u if uid and uid not in ("None", "null", "")}
        total_active = len(active_u)

        # New registered users
        new_reg = 0
        if not df_users.is_empty() and "date" in df_users.columns:
            new_reg = df_users.filter(pl.col("date") == d)["user_id"].n_unique()

        # Page views
        page_views = 0
        if not df_events.is_empty() and "date" in df_events.columns:
            page_views = df_events.filter(
                (pl.col("date") == d) & (pl.col("event_type") == "page_view")
            ).shape[0]

        # Average session duration
        avg_dur = 0.0
        if not df_convs.is_empty() and "date" in df_convs.columns:
            durations = df_convs.filter(pl.col("date") == d)["session_duration_sec"].drop_nulls().to_list()
            if durations:
                avg_dur = sum(durations) / len(durations)

        # Bounce rate
        bounce_rate = 0.0
        if not df_events.is_empty() and "date" in df_events.columns:
            d_ev = df_events.filter(pl.col("date") == d)
            total_sessions = d_ev["session_id"].n_unique()
            if total_sessions > 0:
                bounced = d_ev.filter(pl.col("bounce") == 1)["session_id"].n_unique()
                bounce_rate = (bounced / total_sessions) * 100

        # Device distribution
        desktop_pct = mobile_pct = 0.0
        if not df_events.is_empty() and "date" in df_events.columns:
            d_ev = df_events.filter(pl.col("date") == d)
            total_dev = d_ev.shape[0]
            if total_dev > 0:
                desktop_pct = (d_ev.filter(pl.col("device_type") == "desktop").shape[0] / total_dev) * 100
                mobile_pct  = (d_ev.filter(pl.col("device_type") == "mobile").shape[0]  / total_dev) * 100

        # Retention rate — O(active_users_per_day) via pre-built dict
        retention_rate = 0.0
        if total_active > 0:
            returning = sum(
                1 for uid in active_u
                if user_reg_dates.get(uid, "") not in ("", d)
                and user_reg_dates.get(uid, "") < d
            )
            retention_rate = (returning / total_active) * 100

        rows.append({
            "date":                         d,
            "total_active_users":           int(total_active),
            "new_registered_users":         int(new_reg),
            "total_page_views":             int(page_views),
            "average_session_duration_sec": float(round(avg_dur, 2)),
            "bounce_rate_pct":              float(round(bounce_rate, 2)),
            "device_desktop_pct":           float(round(desktop_pct, 2)),
            "device_mobile_pct":            float(round(mobile_pct, 2)),
            "retention_rate_pct":           float(round(retention_rate, 2)),
            "updated_at":                   datetime.utcnow().isoformat(),
        })

    df_gold = pl.DataFrame(rows) if rows else _empty
    context.log.info(f"✅ Đã tổng hợp chỉ số tương tác cho {df_gold.shape[0]} ngày.")
    write_to_postgres(df_gold, "gold_user_engagement", context)

    return Output(value=df_gold, metadata={
        "total_records": MetadataValue.int(df_gold.shape[0]),
        "gold_time":     datetime.utcnow().isoformat(),
    })


# ── 2. Gold Chat Performance ─────────────────────────────────────────────────
@asset(
    name="gold_chat_performance",
    key_prefix=["gold", "mongodb"],
    group_name="user_lakehouse",
    io_manager_key="minio_io_manager",
    compute_kind="python",
    ins={
        "silver_mongodb_users":         AssetIn(key_prefix=["silver", "mongodb"]),
        "silver_mongodb_conversations": AssetIn(key_prefix=["silver", "mongodb"]),
    },
    description="Đánh giá hiệu năng của chatbot AI, thống kê feedback rating và độ trễ phản hồi",
)
def gold_chat_performance(
    context,
    silver_mongodb_users: pl.DataFrame,
    silver_mongodb_conversations: pl.DataFrame,
) -> Output:

    _empty = pl.DataFrame({
        "session_id":               pl.Series([], dtype=pl.Utf8),
        "user_id":                  pl.Series([], dtype=pl.Utf8),
        "user_uuid":                pl.Series([], dtype=pl.Utf8),
        "email_hashed":             pl.Series([], dtype=pl.Utf8),
        "user_age":                 pl.Series([], dtype=pl.Int32),
        "user_gender":              pl.Series([], dtype=pl.Utf8),
        "session_start_time":       pl.Series([], dtype=pl.Utf8),
        "total_messages_exchanged": pl.Series([], dtype=pl.Int32),
        "average_latency_ms":       pl.Series([], dtype=pl.Float64),
        "feedback_rating":          pl.Series([], dtype=pl.Int32),
        "is_zero_query_ratio":      pl.Series([], dtype=pl.Float64),
        "updated_at":               pl.Series([], dtype=pl.Utf8),
    })

    if silver_mongodb_conversations.is_empty():
        return Output(value=_empty, metadata={"total_records": 0})

    # Pre-build user lookup dict — O(U) instead of per-row filter
    user_info: dict[str, dict] = {}
    if not silver_mongodb_users.is_empty():
        for row in silver_mongodb_users.iter_rows(named=True):
            uid = row.get("user_id", "")
            if uid:
                user_info[uid] = {
                    "age":          row.get("age"),
                    "gender":       row.get("gender"),
                    "user_uuid":    row.get("user_uuid"),
                    "email_hashed": row.get("email_hashed"),
                }

    rows = []
    for row in silver_mongodb_conversations.iter_rows(named=True):
        uid         = row["user_id"] or ""
        info        = user_info.get(uid, {}) if uid not in ("None", "null", "") else {}
        avg_latency, zero_ratio = parse_conversation_messages(row.get("messages_json", "[]"))
        rating      = row.get("feedback_rating")
        rows.append({
            "session_id":               row["session_id"],
            "user_id":                  uid if uid not in ("None", "null", "") else None,
            "user_uuid":                info.get("user_uuid"),
            "email_hashed":             info.get("email_hashed"),
            "user_age":                 info.get("age"),
            "user_gender":              info.get("gender"),
            "session_start_time":       row.get("start_time"),
            "total_messages_exchanged": int(row.get("total_messages", 0)),
            "average_latency_ms":       float(round(avg_latency, 2)),
            "feedback_rating":          int(rating) if rating is not None else None,
            "is_zero_query_ratio":      float(round(zero_ratio, 2)),
            "updated_at":               datetime.utcnow().isoformat(),
        })

    df_gold = pl.DataFrame(rows)
    context.log.info(f"✅ Đã xử lý chat performance cho {df_gold.shape[0]} sessions.")
    write_to_postgres(df_gold, "gold_chat_performance", context)

    return Output(value=df_gold, metadata={
        "total_records": MetadataValue.int(df_gold.shape[0]),
        "gold_time":     datetime.utcnow().isoformat(),
    })


# ── 3. Gold Medical Insights ──────────────────────────────────────────────────
@asset(
    name="gold_medical_insights",
    key_prefix=["gold", "mongodb"],
    group_name="user_lakehouse",
    io_manager_key="minio_io_manager",
    compute_kind="python",
    ins={
        "silver_mongodb_users":        AssetIn(key_prefix=["silver", "mongodb"]),
        "silver_mongodb_events":       AssetIn(key_prefix=["silver", "mongodb"]),
        "silver_mongodb_medical_logs": AssetIn(key_prefix=["silver", "mongodb"]),
    },
    description="Khai phá xu hướng dịch tễ học và thuốc y văn cổ truyền từ hội thoại người bệnh",
)
def gold_medical_insights(
    context,
    silver_mongodb_users: pl.DataFrame,
    silver_mongodb_events: pl.DataFrame,
    silver_mongodb_medical_logs: pl.DataFrame,
) -> Output:

    _empty = pl.DataFrame({
        "log_id":          pl.Series([], dtype=pl.Utf8),
        "session_id":      pl.Series([], dtype=pl.Utf8),
        "user_id":         pl.Series([], dtype=pl.Utf8),
        "user_uuid":       pl.Series([], dtype=pl.Utf8),
        "email_hashed":    pl.Series([], dtype=pl.Utf8),
        "user_age":        pl.Series([], dtype=pl.Int32),
        "user_gender":     pl.Series([], dtype=pl.Utf8),
        "user_city":       pl.Series([], dtype=pl.Utf8),
        "user_country":    pl.Series([], dtype=pl.Utf8),
        "symptoms_list":   pl.Series([], dtype=pl.Utf8),
        "diseases_list":   pl.Series([], dtype=pl.Utf8),
        "herbs_list":      pl.Series([], dtype=pl.Utf8),
        "body_parts_list": pl.Series([], dtype=pl.Utf8),
        "timestamp":       pl.Series([], dtype=pl.Utf8),
        "updated_at":      pl.Series([], dtype=pl.Utf8),
    })

    if silver_mongodb_medical_logs.is_empty():
        return Output(value=_empty, metadata={"total_records": 0})

    # Pre-build user info dict
    user_info: dict[str, dict] = {}
    if not silver_mongodb_users.is_empty():
        for row in silver_mongodb_users.iter_rows(named=True):
            uid = row.get("user_id", "")
            if uid:
                user_info[uid] = {
                    "age":          row.get("age"),
                    "gender":       row.get("gender"),
                    "user_uuid":    row.get("user_uuid"),
                    "email_hashed": row.get("email_hashed"),
                }

    # Pre-build geo map by session_id (first valid entry per session)
    geo_map: dict[str, tuple[str, str]] = {}
    if not silver_mongodb_events.is_empty():
        df_valid = silver_mongodb_events.filter(
            pl.col("country").is_not_null() & (pl.col("country") != "") &
            pl.col("city").is_not_null()    & (pl.col("city") != "")
        )
        for row in df_valid.iter_rows(named=True):
            sid = row["session_id"]
            if sid not in geo_map:
                geo_map[sid] = (row["country"], row["city"])

    rows = []
    for row in silver_mongodb_medical_logs.iter_rows(named=True):
        uid  = row["user_id"] or ""
        info = user_info.get(uid, {}) if uid not in ("None", "null", "") else {}
        country, city = geo_map.get(row["session_id"], ("Vietnam", "Ho Chi Minh City"))

        try:
            symptoms   = json.loads(row.get("symptoms_mentioned",   "[]"))
            diseases   = json.loads(row.get("diseases_mentioned",   "[]"))
            herbs      = json.loads(row.get("herbs_queried",        "[]"))
            body_parts = json.loads(row.get("body_parts_mentioned", "[]"))
        except Exception:
            symptoms = diseases = herbs = body_parts = []

        rows.append({
            "log_id":          row["log_id"],
            "session_id":      row["session_id"],
            "user_id":         uid if uid not in ("None", "null", "") else None,
            "user_uuid":       info.get("user_uuid"),
            "email_hashed":    info.get("email_hashed"),
            "user_age":        info.get("age"),
            "user_gender":     info.get("gender"),
            "user_city":       city,
            "user_country":    country,
            "symptoms_list":   json.dumps(symptoms,   ensure_ascii=False),
            "diseases_list":   json.dumps(diseases,   ensure_ascii=False),
            "herbs_list":      json.dumps(herbs,      ensure_ascii=False),
            "body_parts_list": json.dumps(body_parts, ensure_ascii=False),
            "timestamp":       row.get("timestamp"),
            "updated_at":      datetime.utcnow().isoformat(),
        })

    df_gold = pl.DataFrame(rows)
    context.log.info(f"✅ Đã xử lý y tế insights cho {df_gold.shape[0]} logs.")
    write_to_postgres(df_gold, "gold_medical_insights", context)

    return Output(value=df_gold, metadata={
        "total_records": MetadataValue.int(df_gold.shape[0]),
        "gold_time":     datetime.utcnow().isoformat(),
    })
