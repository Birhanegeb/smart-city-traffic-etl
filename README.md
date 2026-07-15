# Smart City Traffic ETL Pipeline

A near real-time traffic data engineering pipeline developed as a thesis project to demonstrate scalable data ingestion, processing, orchestration, and visualization for smart city applications.

The system collects traffic flow and incident data from the **TomTom Traffic API** for three German cities:

* Berlin
* Bremen
* Frankfurt

The pipeline implements automated workflows, distributed processing, and interactive dashboards using modern data engineering technologies.

---

## Project Overview

The pipeline follows a complete ETL architecture:

```text
TomTom Traffic API
        |
        v
Apache Airflow
        |
        v
Apache Spark (PySpark)
        |
        v
Bronze -> Silver -> Gold Layers
        |
        v
PostgreSQL
        |
        v
Apache Superset Dashboards
```

The system supports:

* Automated traffic data ingestion
* Multi-city schema standardization
* Distributed data processing
* Historical traffic analysis
* Real-time congestion monitoring
* Severe incident detection and email alerts

---

## Architecture Highlights

### Data Ingestion

Traffic flow and incident data are collected from TomTom APIs and stored as JSONL batches.

### Bronze Layer

* Preserves raw source data
* Converts JSONL data into Parquet format
* Maintains reproducible processing

### Silver Layer

* Data cleaning and validation
* Deduplication
* Road segment identification
* Congestion metric calculation

### Gold Layer

* Generates analytical KPIs
* Stores results in PostgreSQL
* Provides data for visualization and analysis

---

## Technology Stack

| Component              | Technology             |
| ---------------------- | ---------------------- |
| Workflow Orchestration | Apache Airflow         |
| Data Processing        | Apache Spark / PySpark |
| Storage                | Parquet + PostgreSQL   |
| Visualization          | Apache Superset        |
| Containerization       | Docker Compose         |
| Data Source            | TomTom Traffic API     |

---

## Key Features

* Automated ETL pipeline orchestration
* Medallion architecture (Bronze/Silver/Gold)
* Multi-city traffic processing
* Congestion classification
* Incident monitoring and email alerts
* Reproducible Docker deployment
* Interactive traffic dashboards

---

## Dashboard Capabilities

The Superset dashboards provide:

* Current congestion hotspots
* Traffic speed analysis
* Road closure monitoring
* City-level traffic comparison
* Historical congestion trends

---

## Quick Start

Requirements:

* Docker
* Docker Compose
* TomTom API key

Run:

```bash
git clone https://github.com/Birhanegeb/smart-city-traffic-etl.git

cd smart-city-traffic-etl

cp .env.example .env

docker compose up -d --build
```

Access services:

| Service  | URL                   |
| -------- | --------------------- |
| Airflow  | http://localhost:8080 |
| Superset | http://localhost:8088 |
| Spark UI | http://localhost:8081 |

---

## Repository Structure

```text
smart-city-traffic-etl/
|
|-- dags/                  # Airflow workflows
|-- spark/                 # PySpark processing jobs
|-- utils/                 # Database initialization scripts
|-- superset/              # Dashboard configuration
|-- data/                  # Runtime data (ignored by Git)
|-- docker-compose.yml
|-- .env.example
```

---

## Thesis Context

This project demonstrates the practical implementation of a scalable smart city traffic data pipeline, focusing on:

* Data engineering architecture
* Pipeline automation
* Distributed processing
* Reproducibility
* Operational monitoring

This README provides a high-level overview of the project.

For complete technical details, including:
- Architecture explanation
- Airflow DAG workflows
- Spark processing logic
- Database schemas
- Dashboard configuration
- Deployment instructions  
see :
[Detailed Technical Documentation](DETAILED_README.md)  
[AWS Deployment Guide](terraform/README.md)