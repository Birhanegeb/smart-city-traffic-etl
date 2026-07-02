import time
import psycopg2
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, to_date
from pyspark.sql.types import (
    StructType, StructField, StringType,
    DoubleType, IntegerType, BooleanType,
)

def write_metrics(dag_id, task_id, batch_id, city, records_read,
                  records_written, records_dropped, spark_time, status, error=None):
    conn = psycopg2.connect(
        host="postgres", port=5432, dbname="airflow_db",
        user="airflow", password="airflow"
    )
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO pipeline_metrics (
            dag_id, task_id, batch_id, city,
            ingestion_timestamp, records_read, records_written,
            records_dropped, rows_processed, spark_execution_time,
            total_execution_time, status, error_message
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """, (
        dag_id, task_id, batch_id, city,
        datetime.utcnow(), records_read, records_written,
        records_dropped, records_written, spark_time,
        spark_time, status, error
    ))
    conn.commit()
    cur.close()
    conn.close()

spark = (
    SparkSession.builder
    .appName("bronze_job")
    .getOrCreate()
)

RAW_PATH = "/opt/data/raw/*.jsonl"
BRONZE_PATH = "/opt/data/bronze/tomtom_segments"

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

start_time = time.time()

df = spark.read.schema(schema).json(RAW_PATH)
records_read = df.count()

bronze_df = (
    df
    .filter(col("city").isNotNull())
    .filter(col("lat").isNotNull())
    .filter(col("lon").isNotNull())
    .withColumn("ingested_at", current_timestamp())
    .withColumn("date", to_date(col("batch_ts"), "yyyyMMdd'T'HHmmss"))
)

records_written = bronze_df.count()
records_dropped = records_read - records_written

(
    bronze_df.write
      .mode("append")
      .partitionBy("city", "date")
      .parquet(BRONZE_PATH)
)

spark_time = round(time.time() - start_time, 2)
batch_id = bronze_df.select("batch_ts").first()[0]

for row in bronze_df.select("city").distinct().collect():
    city_name = row[0]
    city_count = bronze_df.filter(col("city") == city_name).count()
    write_metrics(
        dag_id="tomtom_spark_pipeline",
        task_id="ingestion",
        batch_id=batch_id,
        city=city_name,
        records_read=records_read,
        records_written=city_count,
        records_dropped=records_dropped,
        spark_time=spark_time,
        status="success"
    )

print(f"Bronze layer written to {BRONZE_PATH}")
spark.stop()