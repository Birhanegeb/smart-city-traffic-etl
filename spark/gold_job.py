import time
import psycopg2
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg, count, sum, when, col, to_timestamp, max as spark_max
)
from pyspark.sql.functions import round as spark_round

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
    .appName("tomtom_gold_layer")
    .getOrCreate()
)

silver_path = "/opt/data/silver/tomtom_segments"
gold_path = "/opt/data/gold/traffic_dashboard"

start_time = time.time()

df = spark.read.parquet(silver_path)
records_read = df.count()

latest_batch = df.agg(spark_max("batch_ts")).collect()[0][0]
df = df.filter(col("batch_ts") == latest_batch)

gold_points = (
    df.select("city", "lat", "lon", "speed_ratio", "batch_ts")
      .withColumn("congestion_level",
          when(col("speed_ratio") >= 0.9, "free")
          .when(col("speed_ratio") >= 0.7, "moderate")
          .when(col("speed_ratio") >= 0.5, "heavy")
          .otherwise("severe"))
      .withColumnRenamed("batch_ts", "observation_ts")
      .withColumn("observation_ts", to_timestamp(col("observation_ts"), "yyyyMMdd'T'HHmmss"))
)

(
    gold_points.write
        .format("jdbc")
        .option("url", "jdbc:postgresql://postgres:5432/airflow_db")
        .option("dbtable", "traffic_points")
        .option("user", "airflow")
        .option("password", "airflow")
        .option("driver", "org.postgresql.Driver")
        .mode("append")
        .save()
)

gold = (
    df.groupBy("city", "batch_ts", "road_id", "frc")
      .agg(
          spark_round(avg("current_speed"), 2).alias("current_speed"),
          spark_round(avg("free_flow_speed"), 2).alias("free_flow_speed"),
          spark_round(avg("travel_time"), 2).alias("current_travel_time"),
          spark_round(avg("travel_time"), 2).alias("free_flow_travel_time"),
          spark_round(avg("confidence"), 3).alias("confidence"),
          sum(when(col("road_closure") == True, 1).otherwise(0)).alias("road_closure"),
          spark_round(avg("speed_ratio"), 3).alias("speed_ratio"),
          sum(when(col("speed_ratio") < 0.80, 1).otherwise(0)).alias("congested_segments"),
          count("*").alias("road_segments"),
      )
)

gold = (
    gold
    .withColumn("congestion_percent",
        spark_round((col("congested_segments") / col("road_segments")) * 100, 2))
    .withColumn("delay_seconds",
        spark_round((col("current_travel_time") - col("free_flow_travel_time")), 2))
    .withColumnRenamed("batch_ts", "observation_ts")
    .withColumn("observation_ts", to_timestamp(col("observation_ts"), "yyyyMMdd'T'HHmmss"))
    .withColumn("road_closure", col("road_closure").cast("boolean"))
    .withColumn("congestion_level",
        when(col("speed_ratio") >= 0.9, "free")
        .when(col("speed_ratio") >= 0.7, "moderate")
        .when(col("speed_ratio") >= 0.5, "heavy")
        .otherwise("severe"))
)

(
    gold.write
        .mode("overwrite")
        .partitionBy("city")
        .parquet(gold_path)
)

records_written = gold.count()
records_dropped = records_read - records_written
spark_time = round(time.time() - start_time, 2)

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

for row in gold.select("city").distinct().collect():
    city_name = row[0]
    city_count = gold.filter(col("city") == city_name).count()
    write_metrics(
        dag_id="tomtom_spark_pipeline",
        task_id="silver_to_gold",
        batch_id=latest_batch,
        city=city_name,
        records_read=records_read,
        records_written=city_count,
        records_dropped=records_dropped,
        spark_time=spark_time,
        status="success"
    )

spark.stop()