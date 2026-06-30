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