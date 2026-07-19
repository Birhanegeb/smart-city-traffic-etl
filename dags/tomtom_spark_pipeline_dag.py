from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from common.config import DEFAULT_ARGS
from datetime import datetime
from airflow.models import Variable
from airflow.operators.python import BranchPythonOperator
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

SPARK_CONF = {
    "spark.master": "spark://spark-master:7077",
    "spark.executor.memory": "512m",
    "spark.executor.cores": "2",
    "spark.driver.memory": "512m",
    "spark.hadoop.fs.permissions.umask-mode": "000",
    "spark.hadoop.fs.permissions.enabled": "false",
    "spark.sql.warehouse.dir": "/opt/data"
}
def check_spark_runs(**context):

    count = int(
        Variable.get(
            "spark_pipeline_run_count",
            default_var=0
        )
    )

    count += 1

    if count >= 4:
        Variable.set("spark_pipeline_run_count", 0)
        return "trigger_gold_dag"

    Variable.set("spark_pipeline_run_count", count)
    return "skip_gold"
with DAG(
    dag_id="tomtom_spark_pipeline",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 7, 1),
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
    check_four_runs = BranchPythonOperator(
    task_id="check_four_spark_runs",
    python_callable=check_spark_runs,
    )

    trigger_gold = TriggerDagRunOperator(
        task_id="trigger_gold_dag",
        trigger_dag_id="tomtom_gold_dag",
        wait_for_completion=False,
    )

    skip_gold = EmptyOperator(
        task_id="skip_gold",
    )
    ingestion >> bronze_to_silver

    bronze_to_silver >> check_four_runs

    check_four_runs >> trigger_gold
    check_four_runs >> skip_gold