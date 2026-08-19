import time

from dags.common.config import CITY_CONFIG
from dags.common.extract import build_grid


def test_city_grid_generation_is_fast_enough():
    total_start = time.perf_counter()
    total_points = 0

    for city, cfg in CITY_CONFIG.items():
        start = time.perf_counter()

        points = build_grid(cfg["bbox"], cfg["grid_steps"])

        elapsed = time.perf_counter() - start

        print(f"{city}: {elapsed:.6f} seconds, {len(points)} points")

        assert len(points) > 0
        total_points += len(points)

    total_elapsed = time.perf_counter() - total_start

    print(f"Total: {total_elapsed:.6f} seconds, {total_points} points")

    assert total_points > 0
    assert total_elapsed < 0.5
