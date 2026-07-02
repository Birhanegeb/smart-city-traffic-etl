import time
import psycopg2
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, to_timestamp, to_date

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

spark = SparkSession.builder.appName("incidents_silver").getOrCreate()

BRONZE_PATH = "/opt/data/bronze/incidents"
SILVER_PATH = "/opt/data/silver/incidents"

start_time = time.time()

df = spark.read.parquet(BRONZE_PATH)
records_read = df.count()

df = (
    df.filter(col("city").isNotNull())
      .filter(col("lat").isNotNull())
      .filter(col("lon").isNotNull())
      .withColumn("start_time", to_timestamp(col("start_time")))
      .withColumn("end_time", to_timestamp(col("end_time")))
      .withColumn("observed_at", to_timestamp(col("batch_ts"), "yyyyMMdd'T'HHmmss"))
      .withColumn("processed_at", current_timestamp())
      .withColumn("date", to_date(col("batch_ts"), "yyyyMMdd'T'HHmmss"))
)

records_written = df.count()
records_dropped = records_read - records_written

(
    df.write
      .mode("append")
      .partitionBy("city", "date")
      .parquet(SILVER_PATH)
)

spark_time = round(time.time() - start_time, 2)
batch_id = df.select("batch_ts").first()[0] if records_written > 0 else "unknown"

for row in df.select("city").distinct().collect():
    city_name = row[0]
    city_count = df.filter(col("city") == city_name).count()
    write_metrics(
        dag_id="tomtom_incidents_pipeline",
        task_id="incidents_silver",
        batch_id=batch_id,
        city=city_name,
        records_read=records_read,
        records_written=city_count,
        records_dropped=records_dropped,
        spark_time=spark_time,
        status="success"
    )

print("Silver incidents written.")
spark.stop()