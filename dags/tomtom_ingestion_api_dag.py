from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from common.config import CITY_CONFIG, DEFAULT_ARGS, PG_CONN
from common.paths import RAW_DIR, ensure_dirs
from common.extract import build_grid, fetch_flow_segment
from datetime import datetime
import psycopg2
import logging
import json
import time

logger = logging.getLogger(__name__)


def insert_pipeline_metric(context, city, batch_id, records_read, records_written, records_dropped, execution_time, status):
    conn = psycopg2.connect(**PG_CONN)
    cur = conn.cursor()

    cur.execute(
        """
        INSERT INTO pipeline_metrics (
            dag_id,
            run_id,
            task_id,
            batch_id,
            city,
            ingestion_timestamp,
            records_read,
            records_written,
            records_dropped,
            rows_processed,
            api_execution_time,
            spark_execution_time,
            total_execution_time,
            status,
            measured_at
        )
        VALUES (
            %s,%s,%s,%s,%s,
            NOW(),
            %s,%s,%s,%s,
            %s,%s,%s,
            %s,
            NOW()
        )
        """,
        (
            context["dag"].dag_id,
            context["run_id"],
            context["task"].task_id,
            batch_id,
            city,
            records_read,
            records_written,
            records_dropped,
            records_written,
            execution_time,
            0,
            execution_time,
            status
        )
    )

    conn.commit()
    cur.close()
    conn.close()


def fetch_api_data(city: str, **context):
    start_time = time.time()

    ensure_dirs()

    ts = context["ts_nodash"]
    cfg = CITY_CONFIG[city]

    grid = build_grid(
        cfg["bbox"],
        cfg["grid_steps"]
    )

    out_path = RAW_DIR / f"tomtom_{city}_{ts}.jsonl"

    success = 0
    failed = 0

    try:
        with open(out_path, "w") as f:

            for lat, lon in grid:

                try:
                    raw = fetch_flow_segment(lat, lon)

                    seg = raw.get("flowSegmentData", {})

                    if not seg:
                        failed += 1
                        continue

                    record = {
                        "city": city,
                        "batch_ts": ts,
                        "lat": lat,
                        "lon": lon,
                        "frc": seg.get("frc"),
                        "current_speed": seg.get("currentSpeed"),
                        "free_flow_speed": seg.get("freeFlowSpeed"),
                        "travel_time": seg.get("currentTravelTime"),
                        "confidence": float(seg.get("confidence", 0) or 0),
                        "road_closure": bool(seg.get("roadClosure", False)),
                    }

                    f.write(json.dumps(record) + "\n")
                    success += 1

                except Exception as e:
                    failed += 1
                    logger.warning(
                        f"[{city}] failed ({lat},{lon}): {e}"
                    )

        execution_time = round(
            time.time() - start_time,
            3
        )

        insert_pipeline_metric(
            context,
            city,
            ts,
            success + failed,
            success,
            failed,
            execution_time,
            "success"
        )

        logger.info(
            f"[{city}] success={success}, failed={failed}, time={execution_time}s"
        )

    except Exception as e:

        execution_time = round(
            time.time() - start_time,
            3
        )

        insert_pipeline_metric(
            context,
            city,
            ts,
            0,
            0,
            0,
            execution_time,
            "failed"
        )

        raise e


with DAG(
    dag_id="tomtom_api_ingestion",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 7, 1),
    schedule_interval="*/15 * * * *",
    max_active_runs=1,
    catchup=False,
    tags=["tomtom", "ingestion"],
) as dag:

    flow_tasks = [
        PythonOperator(
            task_id=f"fetch_{city}",
            python_callable=fetch_api_data,
            op_kwargs={"city": city},
        )
        for city in CITY_CONFIG
    ]

    trigger_spark = TriggerDagRunOperator(
        task_id="trigger_spark_pipeline",
        trigger_dag_id="tomtom_spark_pipeline",
        wait_for_completion=False,
    )

    for task in flow_tasks:
        task >> trigger_spark