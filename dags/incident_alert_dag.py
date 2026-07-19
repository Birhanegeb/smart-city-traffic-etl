from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from common.config import DEFAULT_ARGS, PG_CONN
from datetime import datetime

import psycopg2
import logging

logger = logging.getLogger(__name__)

ALERT_EMAIL = "breasgd@outlook.com"

def check_incidents(**context):
    conn = psycopg2.connect(**PG_CONN)
    cur = conn.cursor()

    cur.execute("""
        SELECT city, incident_type, category,delay_seconds, from_road, to_road, 
                observed_at
        FROM (
            SELECT *,
                ROW_NUMBER() OVER (
                    PARTITION BY city, category
                    ORDER BY delay_seconds DESC NULLS LAST
                ) as rn
            FROM traffic_incidents
            WHERE observed_at = (SELECT MAX(observed_at) FROM traffic_incidents)
            AND category IN ('1', '8')
        ) ranked
        WHERE rn = 1
        ORDER BY city, category;
    """)

    incidents = cur.fetchall()
    cur.close()
    conn.close()

    if not incidents:
        logger.info("No accidents or road closures in latest batch.")
        context['task_instance'].xcom_push(key='has_incidents', value=False)
        context['task_instance'].xcom_push(key='email_body', value="")
        return

    rows = ""
    for inc in incidents:
        city, inc_type, category, delay, from_road, to_road, observed_at = inc
        delay_str = f"{int(delay)} seconds" if delay else "unknown"
        CATEGORY_NAMES = {
            '0': 'Unknown',
            '1': 'Accident',
            '2': 'Fog',
            '3': 'Dangerous Conditions',
            '4': 'Rain',
            '5': 'Ice',
            '6': 'Jam',
            '7': 'Road Works',
            '8': 'Road Closed',
            '9': 'Lane Closed',
            '14': 'Broken Down Vehicle',
        }
        rows += f"""
        <tr>
            <td>{city}</td>
            <td>{CATEGORY_NAMES.get(str(category), category)}</td>
            <td>{from_road or 'N/A'} → {to_road or 'N/A'}</td>
            <td>{delay_str}</td>
            <td>{observed_at}</td>
        </tr>
        """

    email_body = f"""
    <h5>Dear Birhane, </h5>
    <p>{len(incidents)} incident(s) have been detected. see below for details.</p>
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
    <p>@birhane</p>
    """

    context['task_instance'].xcom_push(key='has_incidents', value=True)
    context['task_instance'].xcom_push(key='email_body', value=email_body)
    logger.info(f"Found {len(incidents)} incidents — alert will be sent.")


def send_alert(**context):
    has_incidents = context['task_instance'].xcom_pull(
        key='has_incidents', task_ids='check_incidents'
    )
    if not has_incidents:
        logger.info("No incidents — skipping email.")
        return

    email_body = context['task_instance'].xcom_pull(
        key='email_body', task_ids='check_incidents'
    )

    from airflow.utils.email import send_email
    send_email(
        to=ALERT_EMAIL,
        subject="🚨 Traffic Alert: Accidents & Road Closures Detected",
        html_content=email_body,
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