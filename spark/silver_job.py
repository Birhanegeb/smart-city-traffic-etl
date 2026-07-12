import time
import psycopg2
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sha2, concat_ws, current_timestamp,
    row_number, when, to_date
)
from pyspark.sql.functions import round as spark_round
from pyspark.sql.window import Window

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
    .appName("tomtom_silver_layer")
    .getOrCreate()
)

bronze_path = "/opt/data/bronze/tomtom_segments"
silver_path = "/opt/data/silver/tomtom_segments"

start_time = time.time()

df = spark.read.parquet(bronze_path)
records_read = df.count()

df = (
    df.filter(col("city").isNotNull())
      .filter(col("lat").isNotNull())
      .filter(col("lon").isNotNull())
      .filter(col("current_speed").isNotNull())
      .filter(col("free_flow_speed").isNotNull())
)

df = df.withColumn(
    "road_id",
    sha2(concat_ws("|", col("city"), col("lat"), col("lon"), col("frc")), 256)
)

df = df.withColumn(
    "speed_ratio",
    when(
        col("free_flow_speed") > 0,
        spark_round(col("current_speed") / col("free_flow_speed"), 3)
    ).otherwise(None)
)

df = df.withColumn("processed_at", current_timestamp())
df = df.withColumn("date", to_date(col("batch_ts"), "yyyyMMdd'T'HHmmss"))

window = Window.partitionBy("city", "batch_ts", "road_id").orderBy(col("processed_at").desc())

df = (
    df.withColumn("rn", row_number().over(window))
      .filter(col("rn") == 1)
      .drop("rn")
)

records_written = df.count()
records_dropped = records_read - records_written

(
    df.write
      .mode("append")
      .partitionBy("city", "date")
      .parquet(silver_path)
)

spark_time = round(time.time() - start_time, 2)
batch_id = df.select("batch_ts").first()[0]

for row in df.select("city").distinct().collect():
    city_name = row[0]
    city_count = df.filter(col("city") == city_name).count()
    write_metrics(
        dag_id="tomtom_spark_pipeline",
        task_id="bronze_to_silver",
        batch_id=batch_id,
        city=city_name,
        records_read=records_read,
        records_written=city_count,
        records_dropped=records_dropped,
        spark_time=spark_time,
        status="success"
    )

spark.stop()