from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago
from common.config import DEFAULT_ARGS

with DAG(
    dag_id="tomtom_spark_pipeline",
    default_args=DEFAULT_ARGS,
    start_date=days_ago(1),
    schedule_interval="@hourly",
    catchup=False,
    tags=["spark", "tomtom", "pipeline"],
) as dag:

    ingestion = SparkSubmitOperator(
        task_id="ingestion",
        application="/opt/spark-apps/bronze_job.py",
        conn_id="spark_default",
        conf={
        "spark.master": "spark://spark-master:7077",
        "spark.hadoop.fs.permissions.umask-mode": "000",
        "spark.hadoop.fs.permissions.enabled": "false",
        "spark.sql.warehouse.dir": "/opt/data"
    },
    )

    bronze_to_silver = SparkSubmitOperator(
        task_id="bronze_to_silver",
        application="/opt/spark-apps/silver_job.py",
        conn_id="spark_default",
        conf={
            "spark.master": "spark://spark-master:7077",
            "spark.hadoop.fs.permissions.umask-mode": "000",
            "spark.hadoop.fs.permissions.enabled": "false",
            "spark.sql.warehouse.dir": "/opt/data"
        },
    )

    silver_to_gold = SparkSubmitOperator(
        task_id="silver_to_gold",
        application="/opt/spark-apps/gold_job.py",
        conn_id="spark_default",
        jars="/opt/spark/drivers/postgresql-42.7.11.jar",
        conf={
            "spark.master": "spark://spark-master:7077",
            "spark.hadoop.fs.permissions.umask-mode": "000",
            "spark.hadoop.fs.permissions.enabled": "false",
            "spark.sql.warehouse.dir": "/opt/data"
        },
    )

    ingestion >> bronze_to_silver >> silver_to_gold