from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago
from common.config import DEFAULT_ARGS

SPARK_CONF = {
    "spark.master": "spark://spark-master:7077",
    "spark.executor.memory": "512m",
    "spark.executor.cores": "2",
    "spark.driver.memory": "512m",
    "spark.hadoop.fs.permissions.umask-mode": "000",
    "spark.hadoop.fs.permissions.enabled": "false",
    "spark.sql.warehouse.dir": "/opt/data"
}

with DAG(
    dag_id="tomtom_spark_pipeline",
    default_args=DEFAULT_ARGS,
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    tags=["spark", "tomtom", "pipeline"],
) as dag:

    ingestion = SparkSubmitOperator(
        task_id="ingestion",
        application="/opt/spark-apps/bronze_job.py",
        conn_id="spark_default",
        conf=SPARK_CONF,
    )

    bronze_to_silver = SparkSubmitOperator(
        task_id="bronze_to_silver",
        application="/opt/spark-apps/silver_job.py",
        conn_id="spark_default",
        conf=SPARK_CONF,
    )

    ingestion >> bronze_to_silver