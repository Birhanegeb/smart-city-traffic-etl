import time
import psycopg2
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max as spark_max

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

spark = SparkSession.builder.appName("incidents_gold").getOrCreate()

SILVER_PATH = "/opt/data/silver/incidents"

start_time = time.time()

df = spark.read.parquet(SILVER_PATH)
records_read = df.count()

latest_batch = df.agg(spark_max("batch_ts")).collect()[0][0]
df = df.filter(col("batch_ts") == latest_batch)

records_written = df.count()
records_dropped = records_read - records_written

(
    df.select(
        "city", "incident_type", "category",
        "lat", "lon", "delay_seconds",
        "road_numbers", "from_road", "to_road",
        "start_time", "end_time", "observed_at", "batch_ts"
    )
    .write
    .format("jdbc")
    .option("url", "jdbc:postgresql://postgres:5432/airflow_db")
    .option("dbtable", "traffic_incidents")
    .option("user", "airflow")
    .option("password", "airflow")
    .option("driver", "org.postgresql.Driver")
    .mode("append")
    .save()
)

spark_time = round(time.time() - start_time, 2)

for row in df.select("city").distinct().collect():
    city_name = row[0]
    city_count = df.filter(col("city") == city_name).count()
    write_metrics(
        dag_id="tomtom_incidents_pipeline",
        task_id="incidents_gold",
        batch_id=latest_batch,
        city=city_name,
        records_read=records_read,
        records_written=city_count,
        records_dropped=records_dropped,
        spark_time=spark_time,
        status="success"
    )

print("Gold incidents written to PostgreSQL.")
spark.stop()