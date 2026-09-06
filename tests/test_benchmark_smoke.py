import time

from dags.common.extract import build_grid


BERLIN_BBOX = (13.28, 52.46, 13.57, 52.57)


def test_grid_generation_scales_with_input_size():
    """Measure deterministic processing work at three grid sizes."""
    measurements = []

    for grid_steps in (5, 10, 20):
        start = time.perf_counter()
        points = build_grid(BERLIN_BBOX, grid_steps)
        elapsed = time.perf_counter() - start
        measurements.append((grid_steps, len(points), elapsed))

        assert len(points) == grid_steps**2
        assert elapsed >= 0

    for grid_steps, point_count, elapsed in measurements:
        throughput = point_count / elapsed if elapsed else float("inf")
        print(
            f"grid_steps={grid_steps}, points={point_count}, "
            f"elapsed_seconds={elapsed:.6f}, points_per_second={throughput:.2f}"
        )

    assert measurements[-1][1] > measurements[0][1]
