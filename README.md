# Smart City Traffic ETL Pipeline

A self-healing micro-batch ETL pipeline that ingests live traffic flow and incident data for multiple German cities (Berlin, Bremen, Frankfurt) from the TomTom Traffic API, processes it through a Bronze → Silver → Gold layered architecture using Apache Spark, orchestrates everything with Apache Airflow, persists curated results in PostgreSQL, and visualizes them in Superset. Infrastructure is containerized with Docker Compose and deployable to AWS EC2 via Terraform.

This repository is the implementation artifact for the master's thesis: *"Self-Healing Micro-Batch ETL Pipeline for Smart City Traffic Data Using Airflow, PySpark, Docker, Terraform, PostgreSQL, and Superset."*

---

## Pipeline Overview

The system is a **near-real-time / periodic micro-batch pipeline** (not true streaming), built around two independent data domains:

1. **Traffic flow data** - average speed per road segment, sampled on a grid across each city's bounding box.
2. **Incident data** - accidents, road closures, congestion events, etc.

Both domains follow a **Bronze → Silver → Gold** medallion architecture:

| Layer  | Purpose                                                                  | Storage                             |
| ------ | ------------------------------------------------------------------------ | ------------------------------------ |
| Raw    | Untouched JSONL dumps from the TomTom API, one file per batch/city       | Local filesystem (`/opt/data/raw*`) |
| Bronze | Latest-batch filtering, null checks, date partitioning                   | Parquet                             |
| Silver | Cleaning, deduplication, derived fields, append-only durable audit trail | Parquet                             |
| Gold   | Hourly aggregation, congestion classification, KPI computation           | Parquet + PostgreSQL                |

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

---

## Architecture Overview

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
```

---

## Tech Stack

Apache Airflow · Apache Spark (PySpark) · PostgreSQL · Apache Superset · Docker Compose · Terraform · AWS EC2 · TomTom Traffic API · SMTP Email (Airflow alerting)

## Quick Start Prerequisites

- Docker & Docker Compose
- A TomTom API key ([developer.tomtom.com](https://developer.tomtom.com))
- An SMTP-capable email account (e.g. Gmail with an App Password) for incident alerts
- `postgresql-42.7.11.jar` placed under `./drivers` for Spark's JDBC writes to Postgres

---

## Environment Variables (`.env.example`)

Copy `.env.example` to `.env` and fill in real values - `.env` is git-ignored so your secrets stay local. The sections below walk through where to get the TomTom key and SMTP credentials that go here.

```bash
# ── PostgreSQL ───────────────────────────────────────────────
PG_USER=airflow
PG_PASSWORD=change_me_strong_password
PG_DB=traffic_dw

# ── TomTom Traffic API ──────────────────────────────────────
TOMTOM_API_KEY=your_tomtom_key

# ── Airflow Admin ────────────────────────────────────────────
AIRFLOW_ADMIN_USERNAME=admin
AIRFLOW_ADMIN_PASSWORD=change_me_strong_password
AIRFLOW_ADMIN_FIRSTNAME=Admin
AIRFLOW_ADMIN_LASTNAME=User
AIRFLOW_ADMIN_EMAIL=admin@example.com

# ── Superset Admin ───────────────────────────────────────────
SUPERSET_SECRET_KEY=generate_a_random_secret_key
SUPERSET_ADMIN_USERNAME=admin
SUPERSET_ADMIN_PASSWORD=change_me_strong_password
SUPERSET_ADMIN_FIRSTNAME=Admin
SUPERSET_ADMIN_LASTNAME=User
SUPERSET_ADMIN_EMAIL=admin@example.com

# ── Email / SMTP ─────────────────────────────────────────────
AIRFLOW__SMTP__SMTP_HOST=smtp.gmail.com
AIRFLOW__SMTP__SMTP_PORT=587
AIRFLOW__SMTP__SMTP_STARTTLS=True
AIRFLOW__SMTP__SMTP_SSL=False
AIRFLOW__SMTP__SMTP_USER=your_gmail@gmail.com
AIRFLOW__SMTP__SMTP_PASSWORD=your_16_char_app_password
AIRFLOW__SMTP__SMTP_MAIL_FROM=your_gmail@gmail.com
```

`SUPERSET_SECRET_KEY` can be generated with:

```bash
openssl rand -base64 42
```
---

## Getting a TomTom API Key

The pipeline pulls traffic flow and incident data from the TomTom Traffic API, so you'll need a free API key before running anything.

1. **Create an account** - go to [developer.tomtom.com](https://developer.tomtom.com) and sign up (or sign in) for the TomTom Developer Portal.
2. **Create an app/project** - from your dashboard, click **My Apps** → **Add new app**, give it a name (e.g. `smart-city-traffic-etl`).
3. **Enable the required APIs** — for this project you need the **Traffic Flow API** and **Traffic Incidents API** enabled for the app.
4. **Copy your API key** — TomTom generates a key automatically when the app is created; copy it from the app's detail page.
5. **Check the free-tier quota** - the free plan gives a limited number of daily requests, which is enough for development but worth monitoring if you widen the grid sampling or add more cities.
6. **Add the key to your environment** - paste it into `TOMTOM_API_KEY` in your `.env` file (see above).

---

## Configuring Email (SMTP) Alerts

Airflow sends incident alert emails (accidents, road closures) through SMTP. The example configuration uses Gmail with an **App Password**, since Gmail no longer accepts plain account passwords for SMTP login.

1. **Enable 2-Step Verification** on the Google account you want to send from: [myaccount.google.com/security](https://myaccount.google.com/security) → **2-Step Verification** → turn it on.
2. **Generate an App Password**:
   - Go to [myaccount.google.com/apppasswords](https://myaccount.google.com/apppasswords).
   - Choose **Mail** as the app (or "Other" and name it `airflow-smtp`).
   - Google generates a 16-character password — copy it (no spaces needed, but it's fine either way).
3. **Fill in the SMTP block of your `.env`**:
   - `AIRFLOW__SMTP__SMTP_HOST=smtp.gmail.com`
   - `AIRFLOW__SMTP__SMTP_PORT=587`
   - `AIRFLOW__SMTP__SMTP_STARTTLS=True`
   - `AIRFLOW__SMTP__SMTP_SSL=False`
   - `AIRFLOW__SMTP__SMTP_USER=<your Gmail address>`
   - `AIRFLOW__SMTP__SMTP_PASSWORD=<the 16-character App Password>`
   - `AIRFLOW__SMTP__SMTP_MAIL_FROM=<your Gmail address>`
4. **Set the alert recipient** — unlike the sender, the recipient address is hardcoded in the DAG rather than read from `.env`. Open `dags/incident_alert_dag.py` and update the `ALERT_EMAIL` constant near the top of the file to the address that should receive incident alerts; it's passed as `to=ALERT_EMAIL` to the `EmailOperator` further down.
5. **Verify delivery after starting the stack** - once the pipeline is running (see below), trigger the incident DAG manually from the Airflow UI (or wait for a real incident) and confirm the alert email arrives. If it fails, check the Airflow task logs for an SMTP auth error, which almost always means the App Password wasn't generated correctly or 2-Step Verification isn't enabled.

> Using a provider other than Gmail? Swap `SMTP_HOST`/`SMTP_PORT` for your provider's values (e.g. Outlook: `smtp.office365.com:587`) and use that provider's regular or app-specific password.

---

## Setup & Running Locally

Once you've obtained your TomTom API key, configured SMTP, clone and start the full stack in one go:

```bash
git clone https://github.com/Birhanegeb/smart-city-traffic-etl.git
cd smart-city-traffic-etl
cp .env.example .env      # fill in your TomTom API key and credentials (see sections above)
docker compose up -d --build
```

Then open:

- Airflow UI → http://localhost:8080
- Superset UI → http://localhost:8088
- Spark UI → http://localhost:8081

---

## Superset Dashboards

The project provides interactive dashboards for traffic analysis, incident monitoring, and pipeline evaluation. Sample charts: [Sample Charts](https://github.com/Birhanegeb/smart-city-traffic-etl/blob/main/superset/sample_charts).

## Self-Healing Behavior

- **Retries with backoff**: every task uses `DEFAULT_ARGS` (3 retries, exponential backoff).
- **Latest-batch filtering, not deletion**: every Spark job re-derives its working set from the max `batch_ts` in the source layer rather than assuming a clean input, so replays and backfills are safe.
- **Per-point failure isolation**: `fetch_api_data` catches exceptions per grid point so a single failed TomTom request doesn't fail the whole city's ingestion — it's counted in `records_dropped` instead.
- **Decoupled DAGs**: because ingestion, transformation, and aggregation are separate DAGs linked by `TriggerDagRunOperator`, a downstream Spark failure doesn't block the next 15-minute ingestion cycle.

---

## Thesis Research Questions Mapping

| RQ  | Focus                              | Implemented via                                                                                                                    |
| --- | ----------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------ |
| RQ1 | Infrastructure reproducibility     | `Dockerfile`, `docker-compose.yml`, Terraform (AWS EC2)                                                                              |
| RQ2 | Self-healing behavior              | Retry/backoff config, latest-batch filtering, decoupled trigger-based Airflow DAGs, `pipeline_logs`/`pipeline_metrics`              |
| RQ3 | Multi-city schema standardization  | `common/config.py` (`CITY_CONFIG`), shared PySpark schemas across Berlin/Bremen/Frankfurt.

> 📖 For full architectural detail, schema documentation, setup instructions, Terraform deployment, and configuration references, see [DETAILED_README.md](https://github.com/Birhanegeb/smart-city-traffic-etl/blob/main/DETAILED_README.md) and the [Terraform README](https://github.com/Birhanegeb/smart-city-traffic-etl/blob/main/terraform/README.md).

---

*Author: Birhane — Master's Thesis, Data Engineering*