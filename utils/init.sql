-- =====================================================
-- Pipeline Logs
-- =====================================================

CREATE TABLE IF NOT EXISTS pipeline_logs (
    id BIGSERIAL PRIMARY KEY,
    dag_id VARCHAR(128),
    run_id VARCHAR(256),
    city VARCHAR(64),
    stage VARCHAR(64),
    status VARCHAR(32),
    file_path VARCHAR(512),
    error_message TEXT,
    retry_count INT DEFAULT 0,
    logged_at TIMESTAMP DEFAULT NOW()
);


-- =====================================================
-- Traffic KPI Gold Table
-- =====================================================

CREATE TABLE IF NOT EXISTS traffic_kpis (
    id BIGSERIAL PRIMARY KEY,
    city VARCHAR(64),
    road_id VARCHAR(256),
    observation_ts TIMESTAMP,
    frc VARCHAR(16),
    current_speed FLOAT,
    free_flow_speed FLOAT,
    current_travel_time FLOAT,
    free_flow_travel_time FLOAT,
    confidence FLOAT,
    road_closure BOOLEAN,
    delay_seconds FLOAT,
    speed_ratio FLOAT,
    congestion_level VARCHAR(32),
    batch_file VARCHAR(512),
    created_at TIMESTAMP DEFAULT NOW()
);


CREATE INDEX IF NOT EXISTS idx_traffic_kpis_city
ON traffic_kpis(city);

CREATE INDEX IF NOT EXISTS idx_traffic_kpis_observation_ts
ON traffic_kpis(observation_ts);

CREATE INDEX IF NOT EXISTS idx_traffic_kpis_congestion
ON traffic_kpis(congestion_level);



-- =====================================================
-- Traffic Points Gold Table
-- =====================================================

CREATE TABLE IF NOT EXISTS traffic_points (
    id BIGSERIAL PRIMARY KEY,
    city VARCHAR(64),
    lat FLOAT,
    lon FLOAT,
    speed_ratio FLOAT,
    congestion_level VARCHAR(32),
    observation_ts TIMESTAMP,
    created_at TIMESTAMP DEFAULT NOW()
);


CREATE INDEX IF NOT EXISTS idx_traffic_points_city
ON traffic_points(city);

CREATE INDEX IF NOT EXISTS idx_traffic_points_observation_ts
ON traffic_points(observation_ts);



-- =====================================================
-- Traffic Incidents Gold Table
-- =====================================================

CREATE TABLE IF NOT EXISTS traffic_incidents (
    id BIGSERIAL PRIMARY KEY,
    city VARCHAR(64),
    incident_type VARCHAR(64),
    category VARCHAR(64),
    lat FLOAT,
    lon FLOAT,
    delay_seconds FLOAT,
    road_numbers VARCHAR(256),
    from_road VARCHAR(256),
    to_road VARCHAR(256),
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    observed_at TIMESTAMP,
    batch_ts VARCHAR(64),
    created_at TIMESTAMP DEFAULT NOW()
);


CREATE INDEX IF NOT EXISTS idx_traffic_incidents_batch_ts
ON traffic_incidents(batch_ts);

CREATE INDEX IF NOT EXISTS idx_traffic_incidents_city
ON traffic_incidents(city);

CREATE INDEX IF NOT EXISTS idx_traffic_incidents_category
ON traffic_incidents(category);



-- =====================================================
-- Pipeline Metrics Monitoring Table
-- =====================================================

CREATE TABLE IF NOT EXISTS pipeline_metrics (
    id BIGSERIAL PRIMARY KEY,
    dag_id VARCHAR(128),
    run_id VARCHAR(256),
    task_id VARCHAR(128),
    batch_id VARCHAR(256),
    city VARCHAR(64),
    ingestion_timestamp TIMESTAMP,
    records_read BIGINT,
    records_written BIGINT,
    records_dropped BIGINT,
    rows_processed BIGINT,
    api_execution_time FLOAT,
    spark_execution_time FLOAT,
    total_execution_time FLOAT,
    status VARCHAR(32),
    error_message TEXT,
    measured_at TIMESTAMP DEFAULT NOW()
);


CREATE INDEX IF NOT EXISTS idx_pipeline_metrics_dag
ON pipeline_metrics(dag_id);

CREATE INDEX IF NOT EXISTS idx_pipeline_metrics_status
ON pipeline_metrics(status);

CREATE INDEX IF NOT EXISTS idx_pipeline_metrics_time
ON pipeline_metrics(measured_at);



-- =====================================================
-- Pipeline Logs Indexes
-- =====================================================

CREATE INDEX IF NOT EXISTS idx_pipeline_logs_dag
ON pipeline_logs(dag_id);

CREATE INDEX IF NOT EXISTS idx_pipeline_logs_status
ON pipeline_logs(status);

CREATE INDEX IF NOT EXISTS idx_pipeline_logs_time
ON pipeline_logs(logged_at);