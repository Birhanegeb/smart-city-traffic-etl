from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from common.config import CITY_CONFIG, DEFAULT_ARGS
from common.paths import RAW_DIR, ensure_dirs
from common.extract import build_grid, fetch_flow_segment
from datetime import datetime
import logging
import json
logger = logging.getLogger(__name__)

def fetch_api_data(city: str, **context):
    ensure_dirs()
    ts = context["ts_nodash"]
    cfg = CITY_CONFIG[city]
    grid = build_grid(cfg["bbox"], cfg["grid_steps"])
    out_path = RAW_DIR / f"tomtom_{city}_{ts}.jsonl"
    success = 0
    failed = 0
    with open(out_path, "w") as f:
        for lat, lon in grid:
            try:
                raw = fetch_flow_segment(lat, lon)
                seg = raw.get("flowSegmentData", {})
                if not seg:
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
                logger.warning(f"[{city}] failed ({lat},{lon}): {e}")
    logger.info(f"[{city}] success={success}, failed={failed}, file={out_path}")

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