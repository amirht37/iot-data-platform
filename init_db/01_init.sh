#!/bin/bash
set -e

# ----------------------------------------------------------
# 1. GLOBAL SETUP (Users & DBs)
# ----------------------------------------------------------
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "postgres" <<-EOSQL
    DO \$\$
    BEGIN
        IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = '${DB_PASSWORD}') THEN
            CREATE USER grafana WITH PASSWORD '1234';
        END IF;
    END
    \$\$;

    CREATE DATABASE iot_db;
    CREATE DATABASE airflow;
EOSQL

# ----------------------------------------------------------
# 2. IOT_DB INFRASTRUCTURE (The Vault)
# ----------------------------------------------------------
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "iot_db" <<-EOSQL
    -- SCHEMAS
    CREATE SCHEMA IF NOT EXISTS iot_raw;
    CREATE SCHEMA IF NOT EXISTS iot_control;
    CREATE SCHEMA IF NOT EXISTS iot_clean;
    CREATE SCHEMA IF NOT EXISTS iot_quarantine;

    -- RAW TABLE
    CREATE TABLE IF NOT EXISTS iot_raw.raw_events (
        raw_id SERIAL PRIMARY KEY,
        payload JSONB,
        ingest_ts TIMESTAMPTZ DEFAULT now(),
        source CHARACTER VARYING(50)
    );
    CREATE INDEX IF NOT EXISTS idx_raw_ingest_ts ON iot_raw.raw_events (ingest_ts DESC);

    -- CONTROL TABLES
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

    -- CLEAN TABLE
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

    -- QUARANTINE TABLE
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

    -- PERMISSIONS
    GRANT USAGE ON SCHEMA iot_raw, iot_control, iot_clean, iot_quarantine TO airflow;
    GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA iot_raw, iot_control, iot_clean, iot_quarantine TO airflow;
    GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA iot_raw, iot_control, iot_clean, iot_quarantine TO airflow;

    GRANT USAGE ON SCHEMA iot_raw, iot_clean, iot_control, iot_quarantine TO grafana;
    GRANT SELECT ON ALL TABLES IN SCHEMA iot_raw, iot_clean, iot_control, iot_quarantine TO grafana;
    ALTER DEFAULT PRIVILEGES IN SCHEMA iot_raw, iot_clean, iot_control, iot_quarantine GRANT SELECT ON TABLES TO grafana;
EOSQL

