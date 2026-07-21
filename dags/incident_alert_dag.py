from airflow import DAG
from airflow.operators.python import PythonOperator
from common.config import DEFAULT_ARGS, PG_CONN
from datetime import datetime
import psycopg2
import logging
import time

logger = logging.getLogger(__name__)

ALERT_EMAIL = "breasgd@outlook.com"
def insert_alert_metric(context, task_id, rows_processed, status, execution_time):
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
            task_id,
            context["run_id"],
            "all",
            rows_processed,
            rows_processed,
            0,
            rows_processed,
            0,
            0,
            execution_time,
            status
        )
    )

    conn.commit()
    cur.close()
    conn.close()


def check_incidents(**context):
    start_time = time.time()

    conn = psycopg2.connect(**PG_CONN)
    cur = conn.cursor()

    cur.execute(
        """
        SELECT city, incident_type, category, delay_seconds,
               from_road, to_road, observed_at
        FROM (
            SELECT *,
                ROW_NUMBER() OVER (
                    PARTITION BY city, category
                    ORDER BY delay_seconds DESC NULLS LAST
                ) AS rn
            FROM traffic_incidents
            WHERE observed_at = (
                SELECT MAX(observed_at)
                FROM traffic_incidents
            )
            AND category IN ('1', '8')
        ) ranked
        WHERE rn = 1
        ORDER BY city, category;
        """
    )

    incidents = cur.fetchall()

    cur.close()
    conn.close()

    execution_time = round(time.time() - start_time, 3)

    if not incidents:
        insert_alert_metric(
            context,
            "check_incidents",
            0,
            "success",
            execution_time
        )

        context["task_instance"].xcom_push(
            key="has_incidents",
            value=False
        )

        context["task_instance"].xcom_push(
            key="email_body",
            value=""
        )

        logger.info("No accidents or road closures detected.")
        return

    category_names = {
        "0": "Unknown",
        "1": "Accident",
        "2": "Fog",
        "3": "Dangerous Conditions",
        "4": "Rain",
        "5": "Ice",
        "6": "Jam",
        "7": "Road Works",
        "8": "Road Closed",
        "9": "Lane Closed",
        "14": "Broken Down Vehicle",
    }

    rows = ""

    for inc in incidents:
        city, inc_type, category, delay, from_road, to_road, observed_at = inc

        delay_text = f"{int(delay)} seconds" if delay else "unknown"

        rows += f"""
        <tr>
            <td>{city}</td>
            <td>{category_names.get(str(category), category)}</td>
            <td>{from_road or 'N/A'} → {to_road or 'N/A'}</td>
            <td>{delay_text}</td>
            <td>{observed_at}</td>
        </tr>
        """

    email_body = f"""
    <h5>Dear Birhane,</h5>
    <p>{len(incidents)} incident(s) have been detected.</p>

    <table border="1" cellpadding="6" cellspacing="0">
        <tr>
            <th>City</th>
            <th>Category</th>
            <th>Location</th>
            <th>Delay</th>
            <th>Detected At</th>
        </tr>
        {rows}
    </table>

    <p>Smart City Traffic Pipeline.</p>
    """

    insert_alert_metric(
        context,
        "check_incidents",
        len(incidents),
        "success",
        execution_time
    )

    context["task_instance"].xcom_push(
        key="has_incidents",
        value=True
    )

    context["task_instance"].xcom_push(
        key="email_body",
        value=email_body
    )

    logger.info(f"Found {len(incidents)} incidents.")


def send_alert(**context):
    start_time = time.time()

    has_incidents = context["task_instance"].xcom_pull(
        key="has_incidents",
        task_ids="check_incidents"
    )

    if not has_incidents:
        execution_time = round(time.time() - start_time, 3)

        insert_alert_metric(
            context,
            "send_alert",
            0,
            "skipped",
            execution_time
        )

        logger.info("No incidents. Email skipped.")
        return

    email_body = context["task_instance"].xcom_pull(
        key="email_body",
        task_ids="check_incidents"
    )

    from airflow.utils.email import send_email

    send_email(
        to=ALERT_EMAIL,
        subject="🚨 Traffic Alert: Accidents & Road Closures Detected",
        html_content=email_body,
    )

    execution_time = round(time.time() - start_time, 3)

    insert_alert_metric(
        context,
        "send_alert",
        1,
        "success",
        execution_time
    )

    logger.info("Alert email sent.")


with DAG(
    dag_id="incident_alert_dag",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 7, 1),
    schedule_interval=None,
    catchup=False,
    tags=["alerts", "incidents", "email"],
) as dag:

    check = PythonOperator(
        task_id="check_incidents",
        python_callable=check_incidents,
    )

    alert = PythonOperator(
        task_id="send_alert",
        python_callable=send_alert,
    )

    check >> alert