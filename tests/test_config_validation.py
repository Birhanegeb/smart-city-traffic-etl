import os

os.environ.setdefault("TOMTOM_API_KEY", "test-key")

from dags.common.config import CITY_CONFIG, validate_city_config


def test_city_config_contains_expected_cities():
    assert set(CITY_CONFIG.keys()) == {"berlin", "bremen", "frankfurt"}
    assert CITY_CONFIG["berlin"]["grid_steps"] == 5
    assert len(CITY_CONFIG["berlin"]["bbox"]) == 4


def test_validate_city_config_accepts_valid_input():
    validated = validate_city_config({
        "berlin": {"bbox": (13.28, 52.46, 13.57, 52.57), "grid_steps": 5},
        "bremen": {"bbox": (8.72, 53.03, 8.95, 53.13), "grid_steps": 4},
    })

    assert validated["berlin"]["grid_steps"] == 5
    assert validated["bremen"]["bbox"][0] == 8.72


def test_validate_city_config_rejects_invalid_input():
    try:
        validate_city_config({"berlin": {"bbox": (13.28, 52.46, 13.57), "grid_steps": 0}})
        assert False, "Expected ValueError for malformed city config"
    except ValueError:
        pass
