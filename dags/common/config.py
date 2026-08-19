from __future__ import annotations

import os
from datetime import timedelta

TOMTOM_API_KEY = os.getenv("TOMTOM_API_KEY")
if not TOMTOM_API_KEY:
    raise ValueError("TOMTOM_API_KEY is missing in environment")

TOMTOM_URL = "https://api.tomtom.com/traffic/services/4/flowSegmentData/absolute/10/json"


def validate_city_config(city_config: dict | None = None) -> dict:
    """Validate and normalize the city bounding-box configuration."""
    cfg = city_config or {
        "berlin": {"bbox": (13.28, 52.46, 13.57, 52.57), "grid_steps": 5},
        "bremen": {"bbox": (8.72, 53.03, 8.95, 53.13), "grid_steps": 4},
        "frankfurt": {"bbox": (8.55, 50.05, 8.80, 50.18), "grid_steps": 5},
    }

    if not isinstance(cfg, dict) or not cfg:
        raise ValueError("CITY_CONFIG must be a non-empty dictionary")

    validated = {}
    for city, details in cfg.items():
        if not isinstance(city, str) or not city.strip():
            raise ValueError("City name must be a non-empty string")

        if not isinstance(details, dict):
            raise ValueError(f"Config for city '{city}' must be a dictionary")

        bbox = details.get("bbox")
        grid_steps = details.get("grid_steps")

        if not isinstance(bbox, (tuple, list)) or len(bbox) != 4:
            raise ValueError(f"City '{city}' bbox must contain exactly four numeric values")

        if any(not isinstance(value, (int, float)) for value in bbox):
            raise ValueError(f"City '{city}' bbox must contain only numeric coordinates")

        if not isinstance(grid_steps, int) or grid_steps < 1:
            raise ValueError(f"City '{city}' grid_steps must be a positive integer")

        min_lon, min_lat, max_lon, max_lat = bbox
        if min_lon >= max_lon or min_lat >= max_lat:
            raise ValueError(f"City '{city}' bbox must have min < max for longitude and latitude")

        validated[city] = {
            "bbox": (float(min_lon), float(min_lat), float(max_lon), float(max_lat)),
            "grid_steps": grid_steps,
        }

    return validated


CITY_CONFIG = validate_city_config()

PG_CONN = {
    "host": "postgres",
    "port": 5432,
    "dbname": os.getenv("PG_DB", "airflow_db"),
    "user": os.getenv("PG_USER", "airflow"),
    "password": os.getenv("PG_PASSWORD", "airflow"),
}

DEFAULT_ARGS = {
    "owner": "Birhane",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=10),
}