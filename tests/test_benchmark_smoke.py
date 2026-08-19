import time


def test_benchmark_smoke_runs_fast():
    start = time.perf_counter()
    for _ in range(1000):
        pass
    elapsed = time.perf_counter() - start
    assert elapsed < 1.0
