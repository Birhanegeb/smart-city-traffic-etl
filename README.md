# Smart City Traffic ETL Pipeline

A fault-tolerant, near real-time(every 15 minute incients and every 1 hour range traffic flow)data engineering pipeline that ingests, processes, and visualizes live traffic flow and incident data for three German cities — **Berlin**, **Bremen**, and **Frankfurt** - using the TomTom Traffic API.

Built as a master's thesis project, the pipeline demonstrates three research objectives: **infrastructure reproducibility**, **self-healing orchestration**, and **multi-city schema standardization**.

> 📖 For full architectural detail, schema documentation, setup instructions, and configuration reference, see **[DETAILED_README.md](./DETAILED_README.md)**.

---

## What It Does

- Pulls live traffic flow and incident data from the TomTom Traffic API every 15 minutes.
- Processes raw data through a **medallion architecture** (Bronze → Silver → Gold) using PySpark.
- Aggregates KPIs (speed ratios, congestion levels, delays) into PostgreSQL.
- Visualizes results in **Apache Superset**, including a live congestion.
- Sends **automated email alerts** for severe incidents (accidents, road closures).
- Runs as a fully containerized stack (Docker Compose) with a **Terraform**- based path to AWS EC2 deployment.

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

Full diagrams, DAG chains, and layer-by-layer breakdowns are in the [detailed README](./DETAILED_README.md#architecture-overview).

---

## Tech Stack

Apache Airflow · Apache Spark (PySpark) · PostgreSQL · Apache Superset · Docker Compose · Terraform · AWS EC2 · TomTom Traffic API

---

## Quick Start

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

Full setup steps (Airflow connections, SMTP config, Terraform deployment) are covered in [DETAILED_README.md](./DETAILED_README.md#quick-start--installation-guide).

---

See the [full evaluation](./DETAILED_README.md#evaluation-against-the-research-questions) for how each was addressed.

---

## Documentation

| Doc | Contents |
|---|---|
| [DETAILED_README.md](./DETAILED_README.md) | Full architecture, data schemas, DAG/Spark job catalog, database schema, dashboard setup, Terraform deployment, and troubleshooting |

---

*Author: Birhane - Master's Thesis, Data Engineering*