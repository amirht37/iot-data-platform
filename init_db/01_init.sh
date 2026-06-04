#!/bin/bash
set -e


psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "postgres" <<-EOSQL
    DO \$\$
    BEGIN
        IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'grafana') THEN
            CREATE USER grafana WITH PASSWORD '1234';
        END IF;
        
        IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'airflow') THEN
            CREATE USER airflow WITH PASSWORD 'airflow';
        END IF;
    END
    \$\$;

    SELECT 'CREATE DATABASE iot_db' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'iot_db')\gexec
    SELECT 'CREATE DATABASE airflow' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'airflow')\gexec
EOSQL


psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "iot_db" <<-EOSQL
    -- Initialize the TimescaleDB extension engine
    CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

    -- SCHEMAS
    CREATE SCHEMA IF NOT EXISTS iot_raw;
    CREATE SCHEMA IF NOT EXISTS iot_control;
    CREATE SCHEMA IF NOT EXISTS iot_clean;
    CREATE SCHEMA IF NOT EXISTS iot_quarantine;


    CREATE TABLE IF NOT EXISTS iot_raw.raw_events (
        raw_id SERIAL,
        payload JSONB,
        ingest_ts TIMESTAMPTZ DEFAULT now(),
        source CHARACTER VARYING(50),
        PRIMARY KEY (raw_id, ingest_ts)
    );
    CREATE INDEX IF NOT EXISTS idx_raw_ingest_ts ON iot_raw.raw_events (ingest_ts DESC);

  
    SELECT create_hypertable('iot_raw.raw_events', 'ingest_ts', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);


    CREATE TABLE IF NOT EXISTS iot_control.pipeline_runs (
        run_id SERIAL PRIMARY KEY,
        pipeline_name VARCHAR(100),
        start_time TIMESTAMPTZ,
        end_time TIMESTAMPTZ,
        status VARCHAR(20),
        rows_extracted INTEGER DEFAULT 0,
        rows_clean INTEGER DEFAULT 0,
        rows_quarantined INTEGER DEFAULT 0,
        error_message TEXT
    );

    CREATE TABLE IF NOT EXISTS iot_control.pipeline_metadata (
        pipeline_name TEXT PRIMARY KEY,
        last_ingest_ts TIMESTAMP WITHOUT TIME ZONE,
        last_batch_id SERIAL, 
        last_status TEXT,
        updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW()
    );


    CREATE TABLE IF NOT EXISTS iot_clean.clean_events (
        event_id SERIAL PRIMARY KEY,
        raw_id INTEGER NOT NULL,
        run_id INTEGER NOT NULL,
        device_id TEXT NOT NULL,
        event_ts_ms BIGINT,
        event_ts TIMESTAMPTZ NOT NULL,
        temperature DOUBLE PRECISION,
        humidity DOUBLE PRECISION,
        source TEXT,
        ingest_ts TIMESTAMPTZ NOT NULL,
        processed_at TIMESTAMPTZ DEFAULT NOW(),
        CONSTRAINT uk_raw_id_clean UNIQUE (raw_id),
        CONSTRAINT fk_pipeline_run FOREIGN KEY (run_id) REFERENCES iot_control.pipeline_runs(run_id)
    );
    CREATE INDEX IF NOT EXISTS idx_clean_events_event_ts ON iot_clean.clean_events (event_ts);
    CREATE INDEX IF NOT EXISTS idx_clean_events_run_id ON iot_clean.clean_events (run_id);

    CREATE TABLE IF NOT EXISTS iot_quarantine.quarantine_events (
        quarantine_id SERIAL PRIMARY KEY,
        raw_id INTEGER NOT NULL,
        run_id INTEGER NOT NULL,
        reject_reason TEXT NOT NULL,
        device_id TEXT,
        event_ts_ms BIGINT,
        event_ts TIMESTAMPTZ,
        temperature DOUBLE PRECISION,
        humidity DOUBLE PRECISION,
        source TEXT,
        ingest_ts TIMESTAMPTZ,
        CONSTRAINT uk_raw_id_quarantine UNIQUE (raw_id),
        CONSTRAINT fk_pipeline_run_q FOREIGN KEY (run_id) REFERENCES iot_control.pipeline_runs(run_id)
    );
    CREATE INDEX IF NOT EXISTS idx_quarantine_event_ts ON iot_quarantine.quarantine_events (event_ts);
    CREATE INDEX IF NOT EXISTS idx_quarantine_run_id ON iot_quarantine.quarantine_events (run_id);

    -- SECURITY GRANTS
    GRANT USAGE ON SCHEMA iot_raw, iot_control, iot_clean, iot_quarantine TO airflow;
    GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA iot_raw, iot_control, iot_clean, iot_quarantine TO airflow;
    GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA iot_raw, iot_control, iot_clean, iot_quarantine TO airflow;
    ALTER DEFAULT PRIVILEGES IN SCHEMA iot_raw, iot_control, iot_clean, iot_quarantine GRANT ALL ON TABLES TO airflow;

    GRANT USAGE ON SCHEMA iot_raw, iot_clean, iot_control, iot_quarantine TO grafana;
    GRANT SELECT ON ALL TABLES IN SCHEMA iot_raw, iot_clean, iot_control, iot_quarantine TO grafana;
    ALTER DEFAULT PRIVILEGES IN SCHEMA iot_raw, iot_clean, iot_control, iot_quarantine GRANT SELECT ON TABLES TO grafana;
EOSQL
