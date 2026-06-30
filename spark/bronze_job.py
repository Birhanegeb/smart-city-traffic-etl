from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType,
    BooleanType,
)

# ---------------------------
# SPARK SESSION
# ---------------------------
spark = (
    SparkSession.builder
    .appName("bronze_job")
    .getOrCreate()
)

# ---------------------------
# PATHS
# ---------------------------
RAW_PATH = "/opt/data/raw/*.jsonl"
BRONZE_PATH = "/opt/data/bronze/tomtom_segments"

# ---------------------------
# RAW DATA SCHEMA
# ---------------------------
schema = StructType([
    StructField("city", StringType(), True),
    StructField("batch_ts", StringType(), True),
    StructField("lat", DoubleType(), True),
    StructField("lon", DoubleType(), True),
    StructField("frc", StringType(), True),
    StructField("current_speed", IntegerType(), True),
    StructField("free_flow_speed", IntegerType(), True),
    StructField("travel_time", IntegerType(), True),
    StructField("confidence", DoubleType(), True),
    StructField("road_closure", BooleanType(), True),
])

# ---------------------------
# READ RAW JSONL
# ---------------------------
df = spark.read.schema(schema).json(RAW_PATH)

# ---------------------------
# BRONZE CLEANING
# ---------------------------
bronze_df = (
    df
    .filter(col("city").isNotNull())
    .filter(col("lat").isNotNull())
    .filter(col("lon").isNotNull())
    .withColumn("ingested_at", current_timestamp())
)

# ---------------------------
# WRITE BRONZE PARQUET
# ---------------------------
(
    bronze_df.write
    .mode("append")
    .partitionBy("city")
    .parquet(BRONZE_PATH)
)

print(f"✅ Bronze layer written to {BRONZE_PATH}")

spark.stop()