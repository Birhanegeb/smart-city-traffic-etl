import time
import psycopg2
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date
from pyspark.sql.functions import max as spark_max
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType
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

spark = SparkSession.builder.appName("incidents_bronze").getOrCreate()

RAW_PATH = "/opt/data/raw_incidents/*.jsonl"
BRONZE_PATH = "/opt/data/bronze/incidents"

schema = StructType([
    StructField("city", StringType(), True),
    StructField("batch_ts", StringType(), True),
    StructField("incident_type", StringType(), True),
    StructField("category", StringType(), True),
    StructField("lat", DoubleType(), True),
    StructField("lon", DoubleType(), True),
    StructField("delay_seconds", DoubleType(), True),
    StructField("road_numbers", StringType(), True),
    StructField("from_road", StringType(), True),
    StructField("to_road", StringType(), True),
    StructField("start_time", StringType(), True),
    StructField("end_time", StringType(), True),
])

start_time = time.time()

df = spark.read.schema(schema).json(RAW_PATH)
latest_batch = df.agg(spark_max("batch_ts")).collect()[0][0]
df = df.filter(col("batch_ts") == latest_batch)
records_read = df.count()

df = df.withColumn("date", to_date(col("batch_ts"), "yyyyMMdd'T'HHmmss"))
records_written = df.count()
records_dropped = records_read - records_written

(
    df.write
      .mode("append")
      .partitionBy("city", "date")
      .parquet(BRONZE_PATH)
)

spark_time = round(time.time() - start_time, 2)
batch_id = df.select("batch_ts").first()[0] if records_written > 0 else "unknown"

for row in df.select("city").distinct().collect():
    city_name = row[0]
    city_count = df.filter(col("city") == city_name).count()
    write_metrics(
        dag_id="tomtom_incidents_pipeline",
        task_id="incidents_bronze",
        batch_id=batch_id,
        city=city_name,
        records_read=records_read,
        records_written=city_count,
        records_dropped=records_dropped,
        spark_time=spark_time,
        status="success"
    )

print("Bronze incidents written.")
spark.stop()