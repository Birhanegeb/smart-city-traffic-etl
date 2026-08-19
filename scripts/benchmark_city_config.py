import time
from dags.common.config import CITY_CONFIG
from dags.common.extract import build_grid


def benchmark_city_grid_generation():
    start = time.perf_counter()
    total_points = 0

    for city, cfg in CITY_CONFIG.items():
        points = build_grid(cfg["bbox"], cfg["grid_steps"])
        total_points += len(points)

    elapsed = time.perf_counter() - start
    print(f"cities={len(CITY_CONFIG)} total_points={total_points} elapsed_seconds={elapsed:.4f}")
    return {
        "cities": len(CITY_CONFIG),
        "total_points": total_points,
        "elapsed_seconds": round(elapsed, 4),
    }


if __name__ == "__main__":
    benchmark_city_grid_generation()
