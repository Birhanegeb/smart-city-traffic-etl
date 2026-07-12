# Smart City Traffic ETL Pipeline

A near real-time traffic data engineering pipeline that ingests, processes, and visualizes traffic flow and incident data for three German cities — **Berlin**, **Bremen**, and **Frankfurt** — using the TomTom Traffic API. Built as a thesis project demonstrating reproducibility, self-healing orchestration, and multi-city schema standardization.

---

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Technology Stack](#technology-stack)
3. [Project Structure](#project-structure)
4. [Environment Variables](#environment-variables)
5. [Quick Start](#quick-start)
6. [Pipeline Deep Dive](#pipeline-deep-dive)
   - [Data Ingestion](#1-data-ingestion)
   - [Bronze Layer](#2-bronze-layer)
   - [Silver Layer](#3-silver-layer)
   - [Gold Layer](#4-gold-layer)
   - [Incidents Pipeline](#5-incidents-pipeline)
   - [Email Alerts](#6-email-alerts)
7. [Database Schema](#database-schema)
8. [Dashboards](#dashboards)
9. [Analytical Questions Answered](#analytical-questions-answered)
10. [Email Alert Setup](#email-alert-setup)
11. [Airflow Connection Setup](#airflow-connection-setup)

---

## Architecture Overview

```
TomTom Traffic API
        |
        v
[Airflow: tomtom_api_ingestion DAG]
        |
   +----+----+
   |         |
Flow Data  Incidents
(JSONL)    (JSONL)
   |         |
   v         v
[Spark: Bronze Jobs]
Raw JSONL -> Parquet (partitioned by city/date)
   |         |
   v         v
[Spark: Silver Jobs]
Cleaned, enriched, deduplicated Parquet
   |         |
   v         v
[Spark: Gold Jobs]
Aggregated KPIs -> PostgreSQL
                        |
                        v
              [Apache Superset]
              Interactive Dashboards
                        |
                        v
              [Airflow: incident_alert_dag]
              Email Alerts on Severe Incidents
```

**Medallion Architecture** (Bronze / Silver / Gold):
- **Bronze** — raw data preserved exactly as received from the API, converted to Parquet
- **Silver** — cleaned, validated, enriched with derived metrics
- **Gold** — aggregated KPIs written to PostgreSQL for dashboarding

**DAG Chain** (single trigger):
```
tomtom_api_ingestion
    -> trigger_spark_pipeline  (TriggerDagRunOperator)
    -> trigger_incidents_pipeline  (TriggerDagRunOperator)
        -> incident_alert_dag  (TriggerDagRunOperator)
```

---

## Technology Stack

| Component | Technology | Version |
|---|---|---|
| Orchestration | Apache Airflow | 2.8.1 |
| Processing | Apache Spark (PySpark) | 3.5.8 |
| Storage | PostgreSQL | 15 |
| Visualization | Apache Superset | Latest |
| Containerization | Docker Compose | - |
| Traffic Data | TomTom Traffic API | v4 (flow) / v5 (incidents) |

---

## Project Structure

```
smart-city-traffic-etl/
|
|-- dags/                          # Airflow DAG definitions
|   |-- tomtom_ingestion_api_dag.py    # Fetches flow + incident data from TomTom API
|   |-- tomtom_spark_pipeline_dag.py   # Orchestrates flow bronze -> silver -> gold
|   |-- tomtom_incidents_pipeline_dag.py  # Orchestrates incidents bronze -> silver -> gold
|   |-- incident_alert_dag.py          # Queries incidents, sends email alerts
|   |-- common/
|       |-- config.py              # API keys, city bounding boxes, PG connection, retry defaults
|       |-- extract.py             # TomTom API fetch functions (flow + incidents)
|       |-- paths.py               # Centralized data path definitions
|
|-- spark/                         # PySpark transformation jobs
|   |-- bronze_job.py              # Raw JSONL -> Bronze Parquet (flow data)
|   |-- silver_job.py              # Bronze -> Silver Parquet (flow data)
|   |-- gold_job.py                # Silver -> Gold Parquet + PostgreSQL (flow KPIs)
|   |-- incidents_bronze.py        # Raw JSONL -> Bronze Parquet (incidents)
|   |-- incidents_silver.py        # Bronze -> Silver Parquet (incidents)
|   |-- incidents_gold.py          # Silver -> PostgreSQL (severe incidents only)
|   |-- Dockerfile                 # Spark container image
|
|-- utils/
|   |-- init_airflow.sh            # Creates airflow_db and superset_db on first start
|   |-- init.sql                   # Creates pipeline tables (traffic_kpis, traffic_incidents, etc.)
|
|-- superset/
|   |-- Dockerfile                 # Superset container image
|   |-- superset_config.py         # Superset configuration
|
|-- drivers/
|   |-- postgresql-42.7.11.jar     # PostgreSQL JDBC driver for Spark
|
|-- data/                          # Runtime data directory (gitignored)
|   |-- raw/                       # Raw flow JSONL files from TomTom
|   |-- raw_incidents/             # Raw incident JSONL files from TomTom
|   |-- bronze/                    # Bronze Parquet (flow + incidents)
|   |-- silver/                    # Silver Parquet (flow + incidents)
|   |-- gold/                      # Gold Parquet (latest snapshot)
|
|-- Dockerfile                     # Airflow container image
|-- docker-compose.yml             # Full stack definition
|-- .env.example                   # Environment variable template
|-- .gitignore
|-- README.md
```

---

## Environment Variables

Copy `.env.example` to `.env` and fill in your values. Never commit `.env` to Git.

```env
# PostgreSQL
PG_USER=airflow
PG_PASSWORD=airflow
PG_DB=airflow_db
SUPERSET_DB=superset_db

# TomTom API — get a free key at developer.tomtom.com
TOMTOM_API_KEY=your_tomtom_api_key

# Airflow Admin User
AIRFLOW_ADMIN_USERNAME=admin
AIRFLOW_ADMIN_PASSWORD=admin
AIRFLOW_ADMIN_FIRSTNAME=Admin
AIRFLOW_ADMIN_LASTNAME=User
AIRFLOW_ADMIN_EMAIL=admin@airflow.com
AIRFLOW__CORE__FERNET_KEY=your_fernet_key
AIRFLOW__WEBSERVER__SECRET_KEY=your_secret_key

# Superset Admin User
SUPERSET_SECRET_KEY=your_superset_secret
SUPERSET_ADMIN_USERNAME=admin
SUPERSET_ADMIN_PASSWORD=admin
SUPERSET_ADMIN_FIRSTNAME=Admin
SUPERSET_ADMIN_LASTNAME=User
SUPERSET_ADMIN_EMAIL=admin@superset.com

# SMTP / Email Alerts (Gmail)
AIRFLOW__SMTP__SMTP_HOST=smtp.gmail.com
AIRFLOW__SMTP__SMTP_PORT=587
AIRFLOW__SMTP__SMTP_STARTTLS=True
AIRFLOW__SMTP__SMTP_SSL=False
AIRFLOW__SMTP__SMTP_USER=your_gmail@gmail.com
AIRFLOW__SMTP__SMTP_PASSWORD=your_16_char_app_password
AIRFLOW__SMTP__SMTP_MAIL_FROM=your_gmail@gmail.com
```

**Generate a Fernet key:**
```bash
python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

---

## Quick Start

**Prerequisites:** Docker and Docker Compose installed.

```bash
# 1. Clone the repository
git clone https://github.com/birhanegeb/smart-city-traffic-etl.git
cd smart-city-traffic-etl

# 2. Set up environment
cp .env.example .env
# Edit .env with your TomTom API key and credentials

# 3. Start all services
docker compose up -d --build

# 4. Wait ~60 seconds for all services to initialize, then access:
# Airflow UI    -> http://localhost:8080  (admin/admin)
# Superset UI   -> http://localhost:8088  (admin/admin)
# Spark UI      -> http://localhost:8081

# 5. Set up Airflow SMTP connection (for email alerts)
# Admin -> Connections -> + Add (see Email Alert Setup section below)

# 6. Trigger the pipeline
# Go to Airflow UI -> DAGs -> Enable all DAGs -> Trigger tomtom_api_ingestion
```

The full pipeline runs automatically in one click from the single ingestion DAG trigger.

---

## Pipeline Deep Dive

### 1. Data Ingestion

**File:** `dags/tomtom_ingestion_api_dag.py`
**Schedule:** `@hourly`
**DAG ID:** `tomtom_api_ingestion`

This DAG is the entry point of the entire pipeline. It fetches real-time traffic data from TomTom for three German cities using two endpoints: the Flow Segment API (speed/congestion) and the Incident Details API (accidents/closures).

**How it works:**

Each city has a defined bounding box and grid resolution in `dags/common/config.py`:

```python
CITY_CONFIG = {
    "berlin":    {"bbox": (13.28, 52.46, 13.57, 52.57), "grid_steps": 5},
    "bremen":    {"bbox": (8.72,  53.03, 8.95,  53.13), "grid_steps": 4},
    "frankfurt": {"bbox": (8.55,  50.05, 8.80,  50.18), "grid_steps": 5},
}
```

The `build_grid()` function in `dags/common/extract.py` generates a lat/lon grid of sampling points within each bounding box. Berlin and Frankfurt use a 5x5 grid (25 points), Bremen uses 4x4 (16 points).

For each grid point, `fetch_flow_segment()` calls the TomTom Flow API and records:
- Current speed (km/h)
- Free-flow speed (km/h, the speed under ideal conditions)
- Current travel time (seconds)
- Road class (FRC)
- Road closure status
- Confidence score

For each city bounding box, `fetch_incidents()` calls the TomTom Incident Details API and records:
- Incident type and category (numeric code)
- Geographic coordinates
- Delay in seconds
- Road names (from/to)
- Start and end times

All records are written as `.jsonl` files (one JSON object per line):
- Flow data: `/opt/data/raw/tomtom_{city}_{timestamp}.jsonl`
- Incident data: `/opt/data/raw_incidents/incidents_{city}_{timestamp}.jsonl`

After all six fetch tasks complete (3 cities x 2 data types, running in parallel), two Spark pipeline DAGs are triggered simultaneously:
- `tomtom_spark_pipeline` (flow data)
- `tomtom_incidents_pipeline` (incident data)

---

### 2. Bronze Layer

**Files:** `spark/bronze_job.py`, `spark/incidents_bronze.py`
**Purpose:** Convert raw JSONL to structured Parquet with minimal transformation

The bronze layer is the first persistence layer. It reads only the latest batch of JSONL files (filtered by `batch_ts`) and writes them to Parquet format partitioned by city and date.

**Flow bronze** (`bronze_job.py`):
- Reads `*.jsonl` from `/opt/data/raw/` with an explicit schema
- Filters to the latest `batch_ts` only (avoids reprocessing old files)
- Adds `ingested_at` timestamp
- Adds `date` column (extracted from `batch_ts`) for partitioning
- Drops rows where `city`, `lat`, or `lon` are null
- Writes to `/opt/data/bronze/tomtom_segments/city={city}/date={date}/`

**Incidents bronze** (`incidents_bronze.py`):
- Reads `*.jsonl` from `/opt/data/raw_incidents/`
- Same latest-batch filter
- Writes to `/opt/data/bronze/incidents/city={city}/date={date}/`

**Write mode:** `append` — each batch adds new Parquet files without touching previous data.

**Metrics:** Both jobs record `records_read`, `records_written`, `records_dropped`, and `spark_execution_time` to the `pipeline_metrics` table in PostgreSQL.

---

### 3. Silver Layer

**Files:** `spark/silver_job.py`, `spark/incidents_silver.py`
**Purpose:** Clean, enrich, and deduplicate bronze data

**Flow silver** (`silver_job.py`):

Reads all bronze Parquet and applies the following transformations:

1. **Strict null filtering** — drops rows missing `city`, `lat`, `lon`, `current_speed`, or `free_flow_speed`

2. **Road ID generation** — creates a stable `road_id` using SHA-256 hash of city + lat + lon + FRC:
   ```python
   sha2(concat_ws("|", col("city"), col("lat"), col("lon"), col("frc")), 256)
   ```
   This uniquely identifies each road segment consistently across all batches.

3. **Speed ratio computation** — the core congestion metric:
   ```
   speed_ratio = current_speed / free_flow_speed
   ```
   Values: 1.0 = free flow, 0.5 = heavy congestion, 0.0 = standstill

4. **Deduplication** — uses a window function to keep only the most recently processed record per `(city, batch_ts, road_id)` combination

5. **Date partitioning** — extracts `date` from `batch_ts` for efficient partition pruning

**Write mode:** `append` — accumulates full history for trend analysis.

**Incidents silver** (`incidents_silver.py`):
- Filters null coordinates
- Parses `start_time` and `end_time` from string to proper timestamps
- Converts `batch_ts` to `observed_at` timestamp
- Adds `date` column for partitioning
- Writes to `/opt/data/silver/incidents/`

---

### 4. Gold Layer

**File:** `spark/gold_job.py`
**Purpose:** Aggregate silver data into KPIs and write to PostgreSQL

The gold job reads silver Parquet, filters to the **latest batch only**, and computes two outputs:

**Output 1 — Traffic Points** (written to `traffic_points` table):
Point-level data for map/heatmap visualization:
- `city`, `lat`, `lon`, `speed_ratio`, `congestion_level`, `observation_ts`

Congestion level classification:
```
speed_ratio >= 0.9  -> "free"
speed_ratio >= 0.7  -> "moderate"
speed_ratio >= 0.5  -> "heavy"
otherwise           -> "severe"
```

**Output 2 — Traffic KPIs** (written to `traffic_kpis` table):
Aggregated per `(city, batch_ts, road_id, frc)`:

| Column | Description |
|---|---|
| `current_speed` | Average current speed (km/h) |
| `free_flow_speed` | Average free-flow speed (km/h) |
| `current_travel_time` | Average travel time (seconds) |
| `delay_seconds` | Extra travel time vs. free flow |
| `speed_ratio` | Congestion indicator (0–1) |
| `congestion_level` | Classified: free/moderate/heavy/severe |
| `congestion_percent` | % of road segments that are congested |
| `road_closure` | Whether the road is closed |
| `confidence` | Data quality score from TomTom |

**Gold Parquet** is written with `overwrite` — it always reflects the latest batch only (a recomputable snapshot). PostgreSQL uses `append` — accumulates full history for time-series dashboards.

---

### 5. Incidents Pipeline

**Files:** `spark/incidents_silver.py`, `spark/incidents_gold.py`
**DAG:** `tomtom_incidents_pipeline`

After silver processing, `incidents_gold.py`:
- Reads silver incidents Parquet
- Filters to latest batch only
- Keeps only **severe incident categories**:
  - `1` = Accident
  - `3` = Dangerous Conditions
  - `8` = Road Closed
  - `14` = Broken Down Vehicle
- Writes to `traffic_incidents` table in PostgreSQL
- Triggers `incident_alert_dag` on completion

TomTom iconCategory reference:
| Code | Category |
|---|---|
| 0 | Unknown |
| 1 | Accident |
| 2 | Fog |
| 3 | Dangerous Conditions |
| 4 | Rain |
| 5 | Ice |
| 6 | Jam |
| 7 | Road Works |
| 8 | Road Closed |
| 9 | Lane Closed |
| 14 | Broken Down Vehicle |

---

### 6. Email Alerts

**File:** `dags/incident_alert_dag.py`
**DAG:** `incident_alert_dag`

Triggered automatically after each incidents pipeline run. Queries `traffic_incidents` for the latest batch and selects the worst incident per city per category (highest delay), limited to accidents and road closures. Sends an HTML email with a table showing:

- City
- Incident category (human-readable name)
- Road location (from → to)
- Coordinates
- Delay in seconds
- Detection timestamp

---

## Database Schema

**Database:** `airflow_db`

**`traffic_kpis`** — aggregated traffic KPIs per road segment per batch
```sql
city, road_id, observation_ts, frc,
current_speed, free_flow_speed,
current_travel_time, free_flow_travel_time,
confidence, road_closure, delay_seconds,
speed_ratio, congestion_level
```

**`traffic_points`** — point-level coordinates for map visualization
```sql
city, lat, lon, speed_ratio, congestion_level, observation_ts
```

**`traffic_incidents`** — severe traffic incidents
```sql
city, incident_type, category, lat, lon,
delay_seconds, road_numbers, from_road, to_road,
start_time, end_time, observed_at, batch_ts
```

**`pipeline_metrics`** — pipeline performance tracking per Spark job
```sql
dag_id, task_id, batch_id, city,
ingestion_timestamp, records_read, records_written,
records_dropped, rows_processed,
spark_execution_time, total_execution_time,
status, error_message
```

**`pipeline_logs`** — DAG-level operational logs
```sql
dag_id, run_id, city, stage, status,
file_path, error_message, retry_count, logged_at
```

**Database:** `superset_db` — Superset internal metadata (dashboards, users, charts). Do not modify directly.

---

## Dashboards

After connecting Superset to `airflow_db`, the following charts are available:

**Real-time (latest batch):**
- Congestion hotspots heatmap (deck.gl, `traffic_points`)
- Current worst congestion by city (Big Number)
- Road closures count (Big Number)
- Average delay per city (Bar Chart)

**Historical (full accumulated data):**
- Average speed by city over time (Line Chart)
- Congestion level distribution (Pie Chart)
- Congestion percent by city (Bar Chart)

**To import the pre-built dashboard:**
1. Superset → Dashboards → Import
2. Upload `superset/dashboard_export.zip`
3. Map connection to `Traffic Analysis` (pointing to `airflow_db`)

---

## Analytical Questions Answered

**Real-time — `traffic_kpis`:**
1. Which city has the worst congestion right now?
2. How many road segments are currently closed across all cities?
3. What is the current average travel delay (in seconds) per city?

**Real-time — `traffic_points` (map):**
1. Where are the most severely congested points located right now?
2. Which areas show free-flow vs. heavy congestion at this exact moment?
3. Are congestion hotspots clustered around specific city zones?

**Historical — `traffic_kpis`:**
1. How does average speed change throughout the day (peak vs. off-peak)?
2. Has overall congestion trended up, down, or stayed stable across all batches?

**Historical — `traffic_points`:**
1. Which coordinates are chronically congested across most batches (bottlenecks)?
2. How does the geographic spread of congestion shift between times of day?

---

## Email Alert Setup

Gmail requires an App Password for SMTP access:

1. Go to `myaccount.google.com/security`
2. Enable 2-Step Verification
3. Go to `myaccount.google.com/apppasswords`
4. Create app password named `Airflow`
5. Add to `.env`:
```env
AIRFLOW__SMTP__SMTP_USER=your_gmail@gmail.com
AIRFLOW__SMTP__SMTP_PASSWORD=xxxx xxxx xxxx xxxx
AIRFLOW__SMTP__SMTP_MAIL_FROM=your_gmail@gmail.com
```

---

## Airflow Connection Setup

Due to a known issue in Airflow 2.8, SMTP credentials must be set via the UI in addition to `.env`.

**Spark connection** (required for Spark jobs):
- Admin → Connections → + Add
- Connection Id: `spark_default`
- Connection Type: `Spark`
- Host: `spark://spark-master`
- Port: `7077`

**SMTP connection** (required for email alerts):
- Admin → Connections → + Add
- Connection Id: `smtp_default`
- Connection Type: `Email`
- Host: `smtp.gmail.com`
- Login: `your_gmail@gmail.com`
- Password: your 16-character app password
- Port: `587`

**Superset database connection** (required for dashboards):
- Settings → Database Connections → + Database
- Database type: PostgreSQL
- Host: `postgres`, Port: `5432`
- Database: `airflow_db`
- Username: `airflow`, Password: `airflow`
- Display Name: `Traffic Analysis`