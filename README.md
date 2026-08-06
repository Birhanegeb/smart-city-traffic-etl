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
| Silver | Cleaning, deduplication, derived fields, append-only durable audit trail | Parquet |
| Gold   | Hourly aggregation, congestion classification, KPI computation           | Parquet + PostgreSQL |

PostgreSQL is the **persistent system of record** for anything Superset needs to query. Silver Parquet acts as the durable audit trail; Gold Parquet is treated as an ephemeral computation buffer that feeds PostgreSQL.

Raw JSONL files are **never deleted** - each Spark job filters to the latest ingested data rather than removing older files, preserving a full audit trail for reproducibility and debugging.

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

## Architecture at a Glance

```
                         TomTom Traffic APIs
                                 |
              +------------------+------------------+
              |                                     |
              v                                     v
     Traffic Flow API                       Incident API
 (speed, travel time, FRC)          (accidents, closures, events)
              |                                     |
              v                                     v
    Airflow Traffic DAG                  Airflow Incident DAG
    (every 15 minutes)                   (every ingestion cycle)
              |                                     |
              v                                     v
      Raw Flow JSONL                      Raw Incident JSONL
              |                                     |
              v                                     v
       Spark Bronze                       Spark Incident Bronze
              |                                     |
              v                                     v
       Spark Silver                       Spark Incident Silver
              |                                     |
              v                                     v
   Spark Gold (after 4 runs)          Spark Incident Gold
              |                                     |
              |                                     |
              +------------------+------------------+
                                 |
                                 v
                         PostgreSQL Warehouse
                                 |
                 +---------------+---------------+
                 |                               |
                 v                               v
          Apache Superset                 Email Alerts
          Dashboards                      Incident Notifications
                                          
                 

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

# Superset Dashboards

The project provides interactive dashboards for traffic analysis, incident monitoring, and pipeline evaluation. sample chartes are in superset forlder.

## Self-Healing Behavior

- **Retries with backoff**: every task uses `DEFAULT_ARGS` (3 retries, exponential backoff).
- **Latest-batch filtering, not deletion**: every Spark job re-derives its working set from the max `batch_ts` in the source layer rather than assuming a clean input, so replays and backfills are safe.
- **Per-point failure isolation**: `fetch_api_data` catches exceptions per grid point so a single failed TomTom request doesn't fail the whole city's ingestion — it's counted in `records_dropped` instead.
- **Decoupled DAGs**: because ingestion, transformation, and aggregation are separate DAGs linked by `TriggerDagRunOperator`, a downstream Spark failure doesn't block the next 15-minute ingestion cycle.

---
## Thesis Research Questions Mapping

| RQ | Focus | Implemented via |
|---|---|---|
| RQ1 | Infrastructure reproducibility | `Dockerfile`, `docker-compose.yml`, Terraform (AWS EC2) |
| RQ2 | Self-healing behavior | Retry/backoff config, latest-batch filtering, decoupled trigger-based Airflow DAGs, `pipeline_logs`/`pipeline_metrics` |
| RQ3 | Multi-city schema standardization | `common/config.py` (`CITY_CONFIG`), shared PySpark schemas across Berlin/Bremen/Frankfurt in `bronze_job.py`/`incidents_bronze.py` |

> 📖 For full architectural detail, schema documentation, setup instructions, terraform deployment and configuration references, see **[DETAILED_README.md](./DETAILED_README.md) and ****[Terraform Readme](./terraform/README.md)

---

*Author: Birhane - Master's Thesis, Data Engineering*