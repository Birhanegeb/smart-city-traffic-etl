from pathlib import Path

RAW_DIR = Path("/opt/data/raw")
BRONZE_DIR = Path("/opt/data/bronze/tomtom_segments")
SILVER_DIR = Path("/opt/data/silver/tomtom_segments")
GOLD_DIR = Path("/opt/data/gold/traffic_dashboard")
INIT_SQL_PATH = Path("/opt/airflow/utils/init.sql")

def ensure_dirs():
    for p in [RAW_DIR, BRONZE_DIR, SILVER_DIR, GOLD_DIR]:
        p.mkdir(parents=True, exist_ok=True)