from __future__ import annotations
import logging
import psycopg2
from .config import PG_CONN

logger = logging.getLogger(__name__)

def log_to_db(dag_id, run_id, city, stage, status, file_path=None, error=None, retry_count=0):
    try:
        conn = psycopg2.connect(**PG_CONN)
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO pipeline_logs
            (dag_id, run_id, city, stage, status, file_path, error_message, retry_count)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                dag_id,
                run_id,
                city,
                stage,
                status,
                str(file_path) if file_path else None,
                error,
                retry_count,
            ),
        )
        conn.commit()
        cur.close()
        conn.close()
    except Exception as exc:
        logger.warning("pipeline_logs insert failed: %s", exc)