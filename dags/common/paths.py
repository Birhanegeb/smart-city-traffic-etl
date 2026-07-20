from pathlib import Path

# ==========================
# DATA DIRECTORIES
# ==========================

DATA_DIR = Path("/opt/data")

# Raw
RAW_DIR = DATA_DIR / "raw"
RAW_INCIDENTS_DIR = DATA_DIR / "raw_incidents"

# Bronze
BRONZE_DIR = DATA_DIR / "bronze"
BRONZE_TRAFFIC_DIR = BRONZE_DIR / "tomtom_segments"
BRONZE_INCIDENTS_DIR = BRONZE_DIR / "incidents"

# Silver
SILVER_DIR = DATA_DIR / "silver"
SILVER_TRAFFIC_DIR = SILVER_DIR / "tomtom_segments"
SILVER_INCIDENTS_DIR = SILVER_DIR / "incidents"

# Gold
GOLD_DIR = DATA_DIR / "gold"
GOLD_TRAFFIC_DIR = GOLD_DIR / "traffic_dashboard"
GOLD_INCIDENTS_DIR = GOLD_DIR / "incidents_dashboard"

# SQL
INIT_SQL_PATH = Path("/opt/airflow/utils/init.sql")


def ensure_dirs():
    """
    Create all ETL directories if they do not exist.
    """

    directories = [
        RAW_DIR,
        RAW_INCIDENTS_DIR,

        BRONZE_DIR,
        BRONZE_TRAFFIC_DIR,
        BRONZE_INCIDENTS_DIR,

        SILVER_DIR,
        SILVER_TRAFFIC_DIR,
        SILVER_INCIDENTS_DIR,

        GOLD_DIR,
        GOLD_TRAFFIC_DIR,
        GOLD_INCIDENTS_DIR,
    ]

    for directory in directories:
        directory.mkdir(
            parents=True,
            exist_ok=True
        )