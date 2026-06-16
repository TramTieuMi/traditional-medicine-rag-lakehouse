# superset_config/superset_config.py

SECRET_KEY = "yhct_superset_secret_2024"

SQLALCHEMY_DATABASE_URI = (
    "postgresql+psycopg2://superset:superset@superset-db:5432/superset"
)

CACHE_CONFIG = {
    "CACHE_TYPE": "SimpleCache",
    "CACHE_DEFAULT_TIMEOUT": 300,
}

WTF_CSRF_ENABLED = False

FEATURE_FLAGS = {
    "ENABLE_TEMPLATE_PROCESSING": True,
    "DASHBOARD_NATIVE_FILTERS":   True,
}

# Pre-load DuckDB extensions in the parent process to avoid concurrent loading race conditions in worker threads
try:
    import duckdb
    con = duckdb.connect('/app/superset_home/yhct.duckdb')
    con.execute("INSTALL httpfs; LOAD httpfs; INSTALL json; LOAD json;")
    con.close()
    print("🦆 DuckDB extensions httpfs & json pre-loaded in parent process successfully!")
except Exception as e:
    print("⚠️ Warning: Could not pre-load DuckDB extensions:", e)