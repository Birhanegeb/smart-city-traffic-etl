from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python import PythonOperator
from common.extract import fetch_incidents
from common.config import CITY_CONFIG, DEFAULT_ARGS
from common.paths import RAW_DIR, ensure_dirs
from datetime import datetime
import json
import logging

logger = logging.getLogger(__name__)

RAW_INCIDENTS_DIR = RAW_DIR.parent / "raw_incidents"

def fetch_incident_data(city: str, **context):

    ensure_dirs()

    RAW_INCIDENTS_DIR.mkdir(
        parents=True,
        exist_ok=True
    )

    ts = context["ts_nodash"]

    cfg = CITY_CONFIG[city]


    out_path = (
        RAW_INCIDENTS_DIR /
        f"incidents_{city}_{ts}.jsonl"
    )


    raw = fetch_incidents(
        cfg["bbox"]
    )


    incidents = raw.get(
        "incidents",
        []
    )


    if len(incidents) == 0:

        raise ValueError(
            f"[{city}] No incidents returned from TomTom"
        )


    written = 0


    with open(
        out_path,
        "w"
    ) as f:


        for incident in incidents:

            props = incident.get(
                "properties",
                {}
            )

            geometry = incident.get(
                "geometry",
                {}
            )


            coords = geometry.get(
                "coordinates",
                []
            )


            if not coords:
                continue


            if isinstance(coords[0], list):

                lon = coords[0][0]
                lat = coords[0][1]

            else:

                lon = coords[0]
                lat = coords[1]


            record = {

                "city": city,

                "batch_ts": ts,

                "incident_type":
                    incident.get("type"),

                "category":
                    props.get("iconCategory"),

                "lat": lat,

                "lon": lon,

                "delay_seconds":
                    props.get("delay"),

                "road_numbers":
                    json.dumps(
                        props.get(
                            "roadNumbers",
                            []
                        )
                    ),

                "from_road":
                    props.get("from"),

                "to_road":
                    props.get("to"),

                "start_time":
                    props.get("startTime"),

                "end_time":
                    props.get("endTime"),
            }


            f.write(
                json.dumps(record)
                + "\n"
            )

            written += 1


    if written == 0:

        raise ValueError(
            f"[{city}] Empty incident file generated"
        )


    logger.info(
        f"[{city}] "
        f"written={written}, "
        f"file={out_path}"
    )
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
    start_date=datetime(2026, 7, 1),
    schedule_interval="*/15 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["spark", "tomtom", "incidents"],
) as dag:
    incident_fetch_tasks = [
        PythonOperator(
            task_id=f"fetch_incidents_{city}",
            python_callable=fetch_incident_data,
            op_kwargs={"city": city},
        )
        for city in CITY_CONFIG
    ]
    
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
    for task in incident_fetch_tasks:
        task >> incidents_bronze
    incidents_bronze >> incidents_silver >> incidents_gold >> trigger_alert