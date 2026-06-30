from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg, count, sum, when, col, round, lit, to_timestamp
)

# ---------------------------
# SPARK SESSION
# ---------------------------
spark = (
    SparkSession.builder
    .appName("tomtom_gold_layer")
    .getOrCreate()
)

# ---------------------------
# PATHS
# ---------------------------
silver_path = "/opt/data/silver/tomtom_segments"
gold_path = "/opt/data/gold/traffic_dashboard"

# ---------------------------
# READ SILVER
# ---------------------------
df = spark.read.parquet(silver_path)

# ---------------------------
# AGGREGATE METRICS
# ---------------------------
gold = (
    df.groupBy("city", "batch_ts", "road_id", "frc")
      .agg(
          round(avg("current_speed"), 2).alias("current_speed"),
          round(avg("free_flow_speed"), 2).alias("free_flow_speed"),
          round(avg("travel_time"), 2).alias("current_travel_time"),
          round(avg("travel_time"), 2).alias("free_flow_travel_time"),
          round(avg("confidence"), 3).alias("confidence"),
          sum(when(col("road_closure") == True, 1).otherwise(0)).alias("road_closure"),
          round(avg("speed_ratio"), 3).alias("speed_ratio"),
          sum(when(col("speed_ratio") < 0.80, 1).otherwise(0)).alias("congested_segments"),
          count("*").alias("road_segments"),
      )
)

# ---------------------------
# CONGESTION PERCENT & DELAY
# ---------------------------
gold = (
    gold
    .withColumn("congestion_percent",
        round((col("congested_segments") / col("road_segments")) * 100, 2))
    .withColumn("delay_seconds",
        round((col("current_travel_time") - col("free_flow_travel_time")), 2))
    .withColumnRenamed("batch_ts", "observation_ts")
    .withColumn("observation_ts", to_timestamp(col("observation_ts"), "yyyyMMdd'T'HHmmss"))
    .withColumn("road_closure", col("road_closure").cast("boolean"))
    .withColumn("congestion_level",
        when(col("speed_ratio") >= 0.9, "free")
        .when(col("speed_ratio") >= 0.7, "moderate")
        .when(col("speed_ratio") >= 0.5, "heavy")
        .otherwise("severe"))
)

# ---------------------------
# WRITE GOLD PARQUET
# ---------------------------
(
    gold.write
        .mode("overwrite")
        .partitionBy("city")
        .parquet(gold_path)
)

# ---------------------------
# WRITE GOLD TO POSTGRESQL
# ---------------------------
(
    gold.select(
        "city", "road_id", "observation_ts", "frc",
        "current_speed", "free_flow_speed",
        "current_travel_time", "free_flow_travel_time",
        "confidence", "road_closure", "delay_seconds",
        "speed_ratio", "congestion_level"
    )
    .write
    .format("jdbc")
    .option("url", "jdbc:postgresql://postgres:5432/airflow_db")
    .option("dbtable", "traffic_kpis")
    .option("user", "airflow")
    .option("password", "airflow")
    .option("driver", "org.postgresql.Driver")
    .mode("append")
    .save()
)

spark.stop()