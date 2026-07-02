from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
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
    dag_id="tomtom_incidents_pipeline",
    default_args=DEFAULT_ARGS,
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    tags=["spark", "tomtom", "incidents"],
) as dag:

    incidents_bronze = SparkSubmitOperator(
        task_id="incidents_bronze",
        application="/opt/spark-apps/incidents_bronze.py",
        conn_id="spark_default",
        conf=SPARK_CONF,
    )

    incidents_silver = SparkSubmitOperator(
        task_id="incidents_silver",
        application="/opt/spark-apps/incidents_silver.py",
        conn_id="spark_default",
        conf=SPARK_CONF,
    )

    incidents_gold = SparkSubmitOperator(
        task_id="incidents_gold",
        application="/opt/spark-apps/incidents_gold.py",
        conn_id="spark_default",
        jars="/opt/spark/drivers/postgresql-42.7.11.jar",
        conf=SPARK_CONF,
    )

    trigger_alert = TriggerDagRunOperator(
        task_id="trigger_alert",
        trigger_dag_id="incident_alert_dag",
        wait_for_completion=False,
    )

    incidents_bronze >> incidents_silver >> incidents_gold >> trigger_alert