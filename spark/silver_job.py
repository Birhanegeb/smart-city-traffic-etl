from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    sha2,
    concat_ws,
    current_timestamp,
    row_number,
    round,
    when
)
from pyspark.sql.window import Window

# ---------------------------
# SPARK SESSION
# ---------------------------
spark = (
    SparkSession.builder
    .appName("tomtom_silver_layer")
    .getOrCreate()
)

# ---------------------------
# PATHS
# ---------------------------
bronze_path = "/opt/data/bronze/tomtom_segments"
silver_path = "/opt/data/silver/tomtom_segments"

# ---------------------------
# READ BRONZE
# ---------------------------
df = spark.read.parquet(bronze_path)

# ---------------------------
# DATA CLEANING
# ---------------------------
df = (
    df.filter(col("city").isNotNull())
      .filter(col("lat").isNotNull())
      .filter(col("lon").isNotNull())
      .filter(col("current_speed").isNotNull())
      .filter(col("free_flow_speed").isNotNull())
)

# ---------------------------
# ROAD ID
# ---------------------------
df = df.withColumn(
    "road_id",
    sha2(
        concat_ws(
            "|",
            col("city"),
            col("lat"),
            col("lon"),
            col("frc")
        ),
        256
    )
)

# ---------------------------
# SPEED RATIO
# Current speed / Free-flow speed
# ---------------------------
df = df.withColumn(
    "speed_ratio",
    when(
        col("free_flow_speed") > 0,
        round(col("current_speed") / col("free_flow_speed"), 3)
    ).otherwise(None)
)

# ---------------------------
# PROCESSING TIMESTAMP
# ---------------------------
df = df.withColumn(
    "processed_at",
    current_timestamp()
)

# ---------------------------
# REMOVE DUPLICATES
# ---------------------------
window = Window.partitionBy(
    "city",
    "batch_ts",
    "road_id"
).orderBy(col("processed_at").desc())

df = (
    df.withColumn("rn", row_number().over(window))
      .filter(col("rn") == 1)
      .drop("rn")
)

# ---------------------------
# WRITE SILVER
# ---------------------------
(
    df.write
      .mode("append")
      .partitionBy("city")
      .parquet(silver_path)
)

spark.stop()