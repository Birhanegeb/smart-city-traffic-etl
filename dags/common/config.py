from __future__ import annotations

import os
from datetime import timedelta

TOMTOM_API_KEY = os.getenv("TOMTOM_API_KEY")
if not TOMTOM_API_KEY:
    raise ValueError("TOMTOM_API_KEY is missing in environment")

TOMTOM_URL = "https://api.tomtom.com/traffic/services/4/flowSegmentData/absolute/10/json"

CITY_CONFIG = {
    "berlin": {"bbox": (13.28, 52.46, 13.57, 52.57), "grid_steps": 5},
    "bremen": {"bbox": (8.72, 53.03, 8.95, 53.13), "grid_steps": 4},
    "frankfurt": {"bbox": (8.55, 50.05, 8.80, 50.18), "grid_steps": 5},
}

PG_CONN = {
    "host": "postgres",
    "port": 5432,
    "dbname": os.getenv("PG_DB", "airflow_db"),
    "user": os.getenv("PG_USER", "airflow"),
    "password": os.getenv("PG_PASSWORD", "airflow"),
}

DEFAULT_ARGS = {
    "owner": "thesis",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=10),
}