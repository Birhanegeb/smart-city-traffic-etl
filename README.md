# Smart City Traffic ETL Pipeline

Real-time traffic data pipeline for Berlin, Bremen, and Frankfurt using the TomTom Traffic API, built on a medallion architecture (bronze/silver/gold) with PySpark, Airflow, PostgreSQL, and Superset - fully containerized with Docker Compose.

## Architecture

```
TomTom API → Raw (.jsonl) → Bronze (Parquet) → Silver (cleaned/deduped) → Gold (aggregated KPIs) → PostgreSQL → Superset
```

- **Ingestion**: Airflow DAG (`tomtom_api_ingestion`) fetches traffic data hourly across a city grid and writes raw JSONL.
- **Processing**: Airflow DAG (`tomtom_spark_pipeline`) runs Spark jobs for bronze → silver → gold transformations.
- **Orchestration**: `tomtom_api_ingestion` automatically triggers `tomtom_spark_pipeline` on completion via `TriggerDagRunOperator`.
- **Serving**: Gold layer is written to PostgreSQL (`traffic_kpis` table) and visualized in Superset.

## Stack

Airflow 2.8.1 · Apache Spark 3.5.8 · PostgreSQL 15 · Apache Superset · Docker Compose

## Setup

1. Copy `.env.example` to `.env` and fill in your TomTom API key and credentials.
2. Run:
   ```bash
   docker compose up -d --build
   ```
3. Access:
   - Airflow UI → `http://localhost:8080`
   - Superset UI → `http://localhost:8088`
   - Spark Master UI → `http://localhost:8081`

4. In Airflow, unpause and trigger `tomtom_api_ingestion` — this runs the full pipeline end to end in one click.

## What the Pipeline Computes

**Bronze** — preserves raw TomTom API responses as-is (per city, per coordinate point), converted from JSONL to Parquet. No transformation, just format and structure.

**Silver** — cleans and enriches bronze data:
- Drops records with missing city, coordinates, or speed values
- Generates a stable `road_id` (SHA-256 hash of city + lat + lon + frc) to uniquely identify road segments across batches
- Computes `speed_ratio` = current_speed / free_flow_speed, the core congestion indicator (1.0 = free flow, near 0 = gridlock)
- Deduplicates records per city/batch/road segment

**Gold** — aggregates silver data into traffic KPIs per city, road segment, and time batch:
- Average current speed vs. free-flow speed
- Average travel time vs. free-flow travel time, and the resulting `delay_seconds`
- `speed_ratio` and a derived `congestion_level` classification (free / moderate / heavy / severe)
- Road closure counts
- `congestion_percent` — share of monitored segments that are congested (speed_ratio < 0.80)

This gold output is what gets written to `traffic_kpis` in PostgreSQL and surfaced in Superset.

## What Gets Displayed

- **Average speed by city over time** — tracks how traffic speed trends per city across batches, the core "is traffic getting worse" view
- **Congestion level distribution** — breakdown of how many road segments fall into each congestion tier
- **Congestion percent by city** — comparative view of which city is most congested at a glance

## Database Layout

- `airflow_db` — Airflow metadata + `traffic_kpis` / `pipeline_logs` tables (pipeline output)
- `superset_db` — Superset internal metadata (dashboards, users, charts)

## Dashboards
......

## Repository Structure

```
dags/              Airflow DAGs (ingestion_dag.py, spark_pipeline_dag.py)
dags/common/        Shared config, extract helpers, path definitions
spark/              PySpark jobs (bronze_job.py, silver_job.py, gold_job.py)
utils/              Postgres init scripts (create-dbs.sh, init.sql)
superset/           Superset config + dashboard export
drivers/            PostgreSQL JDBC driver for Spark
docker-compose.yml
.env.example
```

### SMTP Configuration

Add the following to your '.env' file:

AIRFLOW__SMTP__SMTP_HOST=smtp.gmail.com
AIRFLOW__SMTP__SMTP_PORT=587
AIRFLOW__SMTP__SMTP_STARTTLS=True
AIRFLOW__SMTP__SMTP_SSL=False
AIRFLOW__SMTP__SMTP_USER=your_gmail@gmail.com
AIRFLOW__SMTP__SMTP_PASSWORD=your_16_char_app_password
AIRFLOW__SMTP__SMTP_MAIL_FROM=your_gmail@gmail.com

Then restart with 'docker compose down && docker compose up -d' — no rebuild required.

### Alert Behavior

After each pipeline run, Airflow checks 'traffic_incidents' in PostgreSQL for newly detected accidents. If incidents are found, an email is sent containing:

- City and location of the incident
- Incident category (accident, road closure, jam)
- Estimated delay in seconds
- Detection timestamp

## Notes

- Single `docker compose up -d` workflow — no staged startup required.
- Bronze and silver layers append data (full history preserved); gold layer overwrites (latest aggregated view).
- Spark JDBC writes additionally persist gold output to PostgreSQL for dashboarding.
