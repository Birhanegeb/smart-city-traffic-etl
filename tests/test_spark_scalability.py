import time

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("etl-scalability-test")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


def test_spark_processing_scales_with_dataset_size(spark):
    measurements = []

    for record_count in (100, 1_000, 5_000):
        start = time.perf_counter()
        processed = (
            spark.range(record_count)
            .withColumn("free_flow_speed", (col("id") % 50) + 1)
            .withColumn("current_speed", col("free_flow_speed") * 0.7)
            .withColumn(
                "congestion_level",
                when(col("current_speed") / col("free_flow_speed") >= 0.7,
                     "moderate")
                .otherwise("heavy"),
            )
        )
        processed_count = processed.count()
        elapsed = time.perf_counter() - start
        measurements.append((record_count, processed_count, elapsed))

    for record_count, processed_count, elapsed in measurements:
        throughput = processed_count / elapsed if elapsed else float("inf")
        print(
            f"records={record_count}, elapsed_seconds={elapsed:.6f}, "
            f"records_per_second={throughput:.2f}"
        )
        assert processed_count == record_count

    assert measurements[-1][0] > measurements[0][0]