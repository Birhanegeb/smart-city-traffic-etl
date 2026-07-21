# Smart City Traffic ETL Pipeline

A self-healing micro-batch ETL pipeline that ingests live traffic flow and incident data for multiple German cities (Berlin, Bremen, Frankfurt) from the TomTom Traffic API, processes it through a Bronze → Silver → Gold layered architecture using Apache Spark, orchestrates everything with Apache Airflow, persists curated results in PostgreSQL, and visualizes them in Superset. Infrastructure is containerized with Docker Compose and deployable to AWS EC2 via Terraform.

This repository is the implementation artifact for the master's thesis: "Self-Healing Micro-Batch ETL Pipeline for Smart City Traffic Data Using Airflow, PySpark, Docker, Terraform, PostgreSQL, and Superset."

---

## What It Does
## Architecture Overview

The system is a **near-real-time / periodic micro-batch pipeline** (not true streaming), built around two independent data domains:

1. **Traffic flow data** — average speed per road segment, sampled on a grid across each city's bounding box.
2. **Incident data** — accidents, road closures, congestion events, etc.

Both domains follow a **Bronze → Silver → Gold** medallion architecture:

| Layer  | Purpose                                                                 | Storage        |
|--------|--------------------------------------------------------------------------|----------------|
| Raw    | Untouched JSONL dumps from the TomTom API, one file per batch/city       | Local filesystem (`/opt/data/raw*`) |
| Bronze | Latest-batch filtering, null checks, date partitioning                   | Parquet |
| Silver | Cleaning, deduplication, derived fields (e.g. `speed_ratio`, `road_id`), append-only durable audit trail | Parquet |
| Gold   | Hourly aggregation, congestion classification, KPI computation           | Parquet + PostgreSQL |

PostgreSQL is the **persistent system of record** for anything Superset needs to query. Silver Parquet acts as the durable audit trail; Gold Parquet is treated as an ephemeral computation buffer that feeds PostgreSQL.

Raw JSONL files are **never deleted** — each Spark job filters to the latest `batch_ts` rather than removing older files, preserving a full audit trail for reproducibility and debugging.

---

## Data Flow

```
TomTom API (Flow + Incidents)
        │
        ▼
  Raw JSONL (per city, per batch, date)
        │
        ▼
   Bronze (Spark, Parquet, latest-batch filter)
        │
        ▼
   Silver (Spark, Parquet, cleaned + deduplicated + derived columns)
        │
        ▼
   Gold (Spark, hourly aggregation, congestion levels)
        │
        ├──► PostgreSQL (traffic_kpis / traffic_points / traffic_incidents)
        │
        └──► Superset (dashboards)

Incident pipeline additionally triggers:
   Gold (incidents) ──► incident_alert_dag ──► Email alert (SMTP, accidents/closures)
```

---

## Airflow DAGs

The system is split into **decoupled DAGs**, each triggering the next stage rather than one monolithic DAG, so failures in one stage don't block ingestion of the next batch.

| DAG ID | Schedule | Responsibility |
|---|---|---|
| `tomtom_api_ingestion` | `*/15 * * * *` | Fetches traffic flow grid data per city from TomTom, writes raw JSONL, logs per-city metrics, triggers `tomtom_spark_pipeline` |
| `tomtom_spark_pipeline` | Triggered | Runs `bronze_job.py` → `silver_job.py`; every 4th run (i.e. hourly) triggers `tomtom_gold_dag` via a `BranchPythonOperator` counter stored in an Airflow `Variable` |
| `tomtom_gold_dag` | Triggered (hourly) | Runs `gold_job.py`: aggregates the latest hour of silver data into KPIs and writes to Postgres + Parquet |
| `tomtom_incidents_pipeline` | `*/15 * * * *` | Fetches incidents per city, runs `incidents_bronze.py` → `incidents_silver.py` → `incidents_gold.py`, then triggers `incident_alert_dag` |
| `incident_alert_dag` | Triggered | Queries the latest incidents for Accident (category `1`) / Road Closed (category `8`), builds an HTML table, and emails an alert via Airflow's SMTP `send_email` if any are found |

**Why the every-4th-run branch for gold?** Ingestion runs every 15 minutes, but hourly aggregation is more meaningful for KPI trends, so `tomtom_spark_pipeline` counts its own runs via an Airflow `Variable` (`spark_pipeline_run_count`) and only triggers `tomtom_gold_dag` once per hour.

---

## Spark Jobs

| Script | Layer | Key logic |
|---|---|---|
| `bronze_job.py` | Bronze (flow) | Reads latest `batch_ts` from raw JSONL, filters nulls, partitions by `city`/`date` |
| `silver_job.py` | Silver (flow) | Deduplicates via a `road_id` hash (`sha2` of city/lat/lon/frc), computes `speed_ratio`, window-based dedup on `(city, batch_ts, road_id)` |
| `gold_job.py` | Gold (flow) | Filters to the latest hour, computes hourly KPIs (avg speed, delay, congestion %, congestion level), writes both point-level (`traffic_points`) and aggregated (`traffic_kpis`) tables to Postgres |
| `incidents_bronze.py` | Bronze (incidents) | Same latest-batch filtering pattern as flow bronze |
| `incidents_silver.py` | Silver (incidents) | Parses timestamps, adds `observed_at`/`processed_at`, partitions by `city`/`date` |
| `incidents_gold.py` | Gold (incidents) | Selects the latest batch and writes to `traffic_incidents` in Postgres |
| `ingestion_bronze.py` | (legacy/unused) | Early prototype bronze loader reading multi-line JSON — superseded by `bronze_job.py`/`incidents_bronze.py` |

Every job writes structured run metrics (`records_read`, `records_written`, `records_dropped`, execution time, status) to a `pipeline_metrics` table in PostgreSQL for observability.

---

## Architecture at a Glance

```
TomTom Traffic API
        |
        v
Airflow Ingestion DAGs (every 15 min)
        |
Raw JSONL --> Spark Bronze --> Spark Silver --> Spark Gold
                                                     |
                                                     v
                                              PostgreSQL
                                                /        \
                                    Apache Superset   Email Alerts
                                    (Dashboards)       (severe incidents)
```

---

## Tech Stack

Apache Airflow · Apache Spark (PySpark) · PostgreSQL · Apache Superset · Docker Compose · Terraform · AWS EC2 · TomTom Traffic API

## Quick start Prerequisites

- Docker & Docker Compose
- A TomTom API key ([developer.tomtom.com](https://developer.tomtom.com))
- An SMTP-capable email account (e.g. Gmail with an App Password) for incident alerts
- `postgresql-42.7.11.jar` placed under `./drivers` for Spark's JDBC writes to Postgres

## Setup & Running Locally
```bash
git clone https://github.com/Birhanegeb/smart-city-traffic-etl.git
cd smart-city-traffic-etl
cp .env.example .env      # fill in your TomTom API key and credentials
docker compose up -d --build
```

Then open:
- Airflow UI → http://localhost:8080
- Superset UI → http://localhost:8088
- Spark UI → http://localhost:8081
---
## Pipeline Monitoring and Analytics

### Pipeline Metrics (PostgreSQL)

The `pipeline_metrics` table stores execution-level information for every ETL stage.

It records:

- DAG ID
- Task ID
- Batch ID
- City
- Processing stage
- Records written
- Execution status
- Processing timestamps

This table enables monitoring of:

- ETL execution performance
- Data processing throughput
- Task success and failure rates
- City-level pipeline comparison

The metrics are visualized in Superset through pipeline performance dashboards.

# Automated Incident Alerts

The `incident_alert_dag` provides automated monitoring for critical traffic events.

Email notifications are generated when the latest incident batch contains:

- Accident events
- Road closure events

The alert mechanism allows rapid awareness of critical traffic situations without manually checking dashboards.

---

# Superset Dashboards

The project provides interactive dashboards for traffic analysis, incident monitoring, and pipeline evaluation.

---

## Traffic Flow Dashboard

The traffic dashboard is built from the `traffic_points` and `traffic_kpis` tables.

It provides:

### Congestion Map

A geospatial visualization using traffic point locations.

Displayed information:

- City location
- Latitude and longitude
- Congestion level
- Speed ratio

Traffic points are classified according to congestion severity:

- Free flow
- Moderate congestion
- Heavy congestion
- Severe congestion


### Traffic KPI Analysis

Based on the `traffic_kpis` table.

Visualizes:

- Average current speed
- Free-flow speed comparison
- Speed ratio distribution
- Delay statistics
- City-level traffic comparison


---

## Incident Dashboard

The incident dashboard uses the `traffic_incidents` table.

It provides:

### Incident Map

Displays incident locations using:

- Latitude
- Longitude
- City
- Incident category


### Incident Analysis

Includes:

- Incident type distribution
- Incident category comparison
- Incident count by city
- Incident delay information
- Temporal incident analysis


---

## Pipeline Performance Dashboard

The pipeline monitoring dashboard uses the `pipeline_metrics` table.

It visualizes:

- Task execution status
- Records processed per batch
- Processing performance by city
- Bronze/Silver/Gold execution comparison
- Pipeline reliability trends


These dashboards provide both operational monitoring and analytical insights into the performance of the smart city ETL platform.
---
---
## Self-Healing Behavior

- **Retries with backoff**: every task uses `DEFAULT_ARGS` (3 retries, exponential backoff, capped at 10 minutes).
- **Latest-batch filtering, not deletion**: every Spark job re-derives its working set from the max `batch_ts` in the source layer rather than assuming a clean input, so replays and backfills are safe.
- **Per-point failure isolation**: `fetch_api_data` catches exceptions per grid point so a single failed TomTom request doesn't fail the whole city's ingestion — it's counted in `records_dropped` instead.
- **Decoupled DAGs**: because ingestion, transformation, and aggregation are separate DAGs linked by `TriggerDagRunOperator`, a downstream Spark failure doesn't block the next 15-minute ingestion cycle.

## Thesis Research Questions Mapping

| RQ | Focus | Implemented via |
|---|---|---|
| RQ1 | Infrastructure reproducibility | `Dockerfile`, `docker-compose.yml`, Terraform (AWS EC2) |
| RQ2 | Self-healing DAG behavior | Retry/backoff config, latest-batch filtering, decoupled trigger-based DAGs, `pipeline_logs`/`pipeline_metrics` |
| RQ3 | Multi-city schema standardization | `common/config.py` (`CITY_CONFIG`), shared PySpark schemas across Berlin/Bremen/Frankfurt in `bronze_job.py`/`incidents_bronze.py` |

> 📖 For full architectural detail, schema documentation, setup instructions, terraform deployment and configuration references, see **[DETAILED_README.md](./DETAILED_README.md) and ****[Terraform Readme](./terraform/README.md)

---

*Author: Birhane - Master's Thesis, Data Engineering*