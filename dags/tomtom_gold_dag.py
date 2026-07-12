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
    dag_id="tomtom_gold_dag",
    default_args=DEFAULT_ARGS,
    start_date=days_ago(1),
    schedule_interval="@hourly",
    catchup=False,
    tags=["spark", "tomtom", "gold"],
) as dag:

    silver_to_gold = SparkSubmitOperator(
        task_id="silver_to_gold",
        application="/opt/spark-apps/gold_job.py",
        conn_id="spark_default",
        jars="/opt/spark/drivers/postgresql-42.7.11.jar",
        conf=SPARK_CONF,
    )