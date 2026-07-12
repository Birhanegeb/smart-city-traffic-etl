import os

# Required secret key
SECRET_KEY = os.getenv("SUPERSET_SECRET_KEY", "change-this-secret")

# Database config (optional override)
SQLALCHEMY_DATABASE_URI = os.getenv(
    "SUPERSET_DB_URI",
    "postgresql+psycopg2://airflow:airflow@postgres:5432/superset_db"
)

# Feature flags (safe defaults)
FEATURE_FLAGS = {
    "DASHBOARD_NATIVE_FILTERS": True
}

# Disable example data
LOAD_EXAMPLES = False