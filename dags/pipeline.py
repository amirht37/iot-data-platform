import os
import pandas as pd
from sqlalchemy import create_engine, text
from extract import extract_data
from transform import transform_data
from load import load_data
from Logger import logger

# 🏛️ INFRASTRUCTURE HANDSHAKE
# Pulling the Maserati keys directly from the environment (.env)
user = os.getenv("POSTGRES_USER", "airflow")
password = os.getenv("POSTGRES_PASSWORD", "airflow")
host = os.getenv("DB_HOST", "postgres")
port = os.getenv("DB_PORT", "5432")
db_name = os.getenv("IOT_DB", "iot_db")

# Unified Name to match your metadata table exactly
PIPELINE_NAME = "sensor_etl"

DB_URI = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db_name}"
engine = create_engine(DB_URI)

def run_pipeline():
    # 1. Start the Audit Trail
    # We use f-strings to inject the PIPELINE_NAME variable
    insert_run_query = text(f"""
        INSERT INTO iot_control.pipeline_runs (pipeline_name, start_time, status)
        VALUES ('{PIPELINE_NAME}', NOW(), 'RUNNING')
        RETURNING run_id;
    """)

    with engine.begin() as conn:
        run_id = int(conn.execute(insert_run_query).scalar())

    try:
        logger.info(f"--- {PIPELINE_NAME} RUN {run_id} STARTED ---")

        # 2. Extract
        df_raw = extract_data(engine)
        if df_raw is None or df_raw.empty:
            logger.info("Scan complete: No new data. Exiting.")
            update_status(run_id, 'SUCCESS', 0, 0, 0)
            return
            
        rows_extracted = len(df_raw)

        # 3. Transform
        df_clean, df_quarantine = transform_data(df_raw, run_id) 
        rows_clean = len(df_clean)
        rows_quarantined = len(df_quarantine)

        # 4. Load
        load_data(df_clean, df_quarantine, engine, run_id) 

        # 5. Final Operational Status Update
        update_status(run_id, 'SUCCESS', rows_extracted, rows_clean, rows_quarantined)
        logger.info(f"--- {PIPELINE_NAME} RUN {run_id} COMPLETED SUCCESSFULLY ---")

    except Exception as e:
        logger.exception(f"PIPELINE CRASHED: Run {run_id}")
        update_status(run_id, 'FAILED', error_message=str(e))
        
        # Ensure Metadata Table shows the failure for the Dashboard
        with engine.begin() as conn:
            conn.execute(text(f"""
                UPDATE iot_control.pipeline_metadata 
                SET last_status = 'FAILED', updated_at = NOW()
                WHERE pipeline_name = '{PIPELINE_NAME}'
            """))
        raise

def update_status(run_id, status, extracted=0, clean=0, quarantined=0, error_message=None):
    query = text("""
        UPDATE iot_control.pipeline_runs
        SET status=:status,
            end_time=NOW(),
            rows_extracted=:extracted,
            rows_clean=:clean,
            rows_quarantined=:quarantined,
            error_message=:error
        WHERE run_id=:run_id
    """)
    with engine.begin() as conn:
        conn.execute(query, {
            "status": status,
            "extracted": extracted,
            "clean": clean,
            "quarantined": quarantined,
            "error": error_message,
            "run_id": run_id
        })

if __name__ == "__main__":
    logger.info("Manual execution triggered")
    try:
        run_pipeline()
    except Exception as e:
        logger.error(f"Manual execution failed: {e}")
