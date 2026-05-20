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