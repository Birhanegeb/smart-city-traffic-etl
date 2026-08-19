from dags.common.config import CITY_CONFIG
from dags.common.extract import build_grid


def test_city_config_integrates_with_grid_generation():
    """Verify that each city configuration can be used to generate a grid."""

    for city, cfg in CITY_CONFIG.items():
        points = build_grid(
            cfg["bbox"],
            cfg["grid_steps"]
        )

        assert points, f"No grid points generated for {city}"

        for lat, lon in points:
            assert cfg["bbox"][1] <= lat <= cfg["bbox"][3]
            assert cfg["bbox"][0] <= lon <= cfg["bbox"][2]