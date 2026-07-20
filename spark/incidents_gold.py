import time
import psycopg2
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max as spark_max


def write_metrics(
    dag_id,
    task_id,
    batch_id,
    city,
    records_read,
    records_written,
    records_dropped,
    spark_time,
    status,
    error=None
):
    conn = psycopg2.connect(
        host="postgres",
        port=5432,
        dbname="airflow_db",
        user="airflow",
        password="airflow"
    )

    cur = conn.cursor()

    cur.execute("""
        INSERT INTO pipeline_metrics (
            dag_id,
            task_id,
            batch_id,
            city,
            ingestion_timestamp,
            records_read,
            records_written,
            records_dropped,
            rows_processed,
            spark_execution_time,
            total_execution_time,
            status,
            error_message
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """,
    (
        dag_id,
        task_id,
        batch_id,
        city,
        datetime.utcnow(),
        records_read,
        records_written,
        records_dropped,
        records_written,
        spark_time,
        spark_time,
        status,
        error
    ))

    conn.commit()
    cur.close()
    conn.close()


spark = SparkSession.builder.appName("incidents_gold").getOrCreate()


SILVER_PATH = "/opt/data/silver/incidents"
GOLD_PATH = "/opt/data/gold/incidents"


start_time = time.time()


# -----------------------------
# Read Silver
# -----------------------------

df = spark.read.parquet(SILVER_PATH)


# -----------------------------
# Select latest batch only
# -----------------------------

latest_batch = df.agg(
    spark_max("batch_ts")
).collect()[0][0]


df = df.filter(
    col("batch_ts") == latest_batch
)


records_read = df.count()


# -----------------------------
# Gold transformation
# -----------------------------

gold_df = (
    df.select(
        "city",
        "incident_type",
        "category",
        "lat",
        "lon",
        "delay_seconds",
        "road_numbers",
        "from_road",
        "to_road",
        "start_time",
        "end_time",
        "observed_at",
        "batch_ts"
    )
)


records_written = gold_df.count()
records_dropped = records_read - records_written


# -----------------------------
# Write Gold Parquet
# -----------------------------

(
    gold_df.write
        .mode("append")
        .partitionBy("city")
        .parquet(GOLD_PATH)
)


# -----------------------------
# Load Gold into PostgreSQL
# -----------------------------

(
    gold_df.write
        .format("jdbc")
        .option(
            "url",
            "jdbc:postgresql://postgres:5432/airflow_db"
        )
        .option(
            "dbtable",
            "traffic_incidents"
        )
        .option(
            "user",
            "airflow"
        )
        .option(
            "password",
            "airflow"
        )
        .option(
            "driver",
            "org.postgresql.Driver"
        )
        .mode("append")
        .save()
)


spark_time = round(
    time.time() - start_time,
    2
)


batch_id = latest_batch


# -----------------------------
# Metrics per city
# -----------------------------

for row in gold_df.select("city").distinct().collect():

    city_name = row["city"]

    city_count = (
        gold_df
        .filter(col("city") == city_name)
        .count()
    )

    write_metrics(
        dag_id="tomtom_incidents_pipeline",
        task_id="incidents_gold",
        batch_id=batch_id,
        city=city_name,
        records_read=records_read,
        records_written=city_count,
        records_dropped=records_dropped,
        spark_time=spark_time,
        status="success"
    )


print(
    f"Incident Gold written successfully: {GOLD_PATH}"
)


spark.stop()