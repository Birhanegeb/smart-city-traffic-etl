import json

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, concat_ws, max as spark_max, sha2, when


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("etl-integration-test")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


def test_bronze_silver_gold_flow(spark, tmp_path):
    raw_path = tmp_path / "raw.jsonl"
    bronze_path = tmp_path / "bronze"
    silver_path = tmp_path / "silver"
    gold_path = tmp_path / "gold"
    records = [
        {
            "city": "berlin",
            "batch_ts": "20260906T120000",
            "lat": 52.46,
            "lon": 13.28,
            "frc": "FRC1",
            "current_speed": 35,
            "free_flow_speed": 50,
        },
        {
            "city": "frankfurt",
            "batch_ts": "20260906T114500",
            "lat": 50.05,
            "lon": 8.55,
            "frc": "FRC3",
            "current_speed": 25,
            "free_flow_speed": 50,
        },
        {
            "city": "berlin",
            "batch_ts": "20260906T120000",
            "lat": 52.46,
            "lon": 13.28,
            "frc": "FRC1",
            "current_speed": 35,
            "free_flow_speed": 50,
        },
        {
            "city": "bremen",
            "batch_ts": "20260906T120000",
            "lat": 53.03,
            "lon": 8.72,
            "frc": "FRC2",
            "current_speed": 20,
            "free_flow_speed": 50,
        },
        {
            "city": "berlin",
            "batch_ts": "20260906T120000",
            "lat": None,
            "lon": 13.30,
            "frc": "FRC1",
            "current_speed": 30,
            "free_flow_speed": 50,
        },
    ]
    raw_path.write_text("\n".join(json.dumps(record) for record in records))

    bronze = (
        spark.read.json(str(raw_path))
        .filter(col("city").isNotNull())
        .filter(col("lat").isNotNull())
        .filter(col("lon").isNotNull())
    )
    bronze.write.mode("overwrite").parquet(str(bronze_path))

    latest_batch = bronze.agg(spark_max("batch_ts")).collect()[0][0]
    silver = (
        spark.read.parquet(str(bronze_path))
        .filter(col("batch_ts") == latest_batch)
        .filter(col("current_speed").isNotNull())
        .filter(col("free_flow_speed").isNotNull())
        .withColumn(
            "road_id",
            sha2(concat_ws("|", "city", "lat", "lon", "frc"), 256),
        )
        .withColumn(
            "speed_ratio",
            when(col("free_flow_speed") > 0,
                 col("current_speed") / col("free_flow_speed")),
        )
        .dropDuplicates(["city", "batch_ts", "road_id"])
    )
    silver.write.mode("overwrite").parquet(str(silver_path))

    gold = (
        spark.read.parquet(str(silver_path))
        .groupBy("city")
        .agg(avg("speed_ratio").alias("speed_ratio"))
        .withColumn(
            "congestion_level",
            when(col("speed_ratio") >= 0.9, "free")
            .when(col("speed_ratio") >= 0.7, "moderate")
            .when(col("speed_ratio") >= 0.5, "heavy")
            .otherwise("severe"),
        )
    )
    gold.write.mode("overwrite").parquet(str(gold_path))

    bronze_result = spark.read.parquet(str(bronze_path))
    silver_result = spark.read.parquet(str(silver_path))
    gold_result = spark.read.parquet(str(gold_path))

    assert bronze_result.count() == 4
    assert silver_result.count() == 2
    assert silver_result.select("batch_ts").distinct().collect()[0][0] == latest_batch
    assert set(row.city for row in gold_result.select("city").collect()) == {
        "berlin",
        "bremen",
    }
    assert {field.name for field in gold_result.schema} >= {
        "speed_ratio",
        "congestion_level",
    }


def test_bronze_batch_write_is_idempotent(spark, tmp_path):
    output_path = tmp_path / "bronze"
    rows = [
        ("berlin", "20260906T120000", 52.46, 13.28),
        ("bremen", "20260906T120000", 53.03, 8.72),
    ]
    frame = spark.createDataFrame(rows, ["city", "batch_ts", "lat", "lon"])
    frame = frame.withColumn("date", col("batch_ts").substr(1, 8))

    (
        frame.write
        .mode("overwrite")
        .option("partitionOverwriteMode", "dynamic")
        .partitionBy("city", "date")
        .parquet(str(output_path))
    )
    (
        frame.write
        .mode("overwrite")
        .option("partitionOverwriteMode", "dynamic")
        .partitionBy("city", "date")
        .parquet(str(output_path))
    )

    assert spark.read.parquet(str(output_path)).count() == len(rows)