import time
from sqlalchemy import text
from Logger import logger

def load_data(df_clean, df_quarantine, engine, run_id):
    """
    Handles Upserts and updates the FULL metadata watermark.
    """
    logger.info(f"Load started for Run ID: {run_id}")
    start = time.time()

    # --- Identify Watermarks from both paths ---
    all_raw_ids = []
    all_ts = []
    
    if not df_clean.empty: 
        all_raw_ids.extend(df_clean['raw_id'].tolist())
        all_ts.extend(df_clean['ingest_ts'].tolist())
    if not df_quarantine.empty: 
        all_raw_ids.extend(df_quarantine['raw_id'].tolist())
        all_ts.extend(df_quarantine['ingest_ts'].tolist())
    
    if not all_raw_ids:
        logger.warning("No data to load.")
        return
    
    new_max_id = max(all_raw_ids)
    new_max_ts = max(all_ts)

    try:
        with engine.begin() as conn:
            
            # 1. Load Clean Data with UPSERT
            if not df_clean.empty:
                conn.execute(text("""
                    INSERT INTO iot_clean.clean_events 
                    (raw_id, run_id, device_id, event_ts_ms, event_ts, temperature, humidity, source, ingest_ts)
                    VALUES (:raw_id, :run_id, :device_id, :event_ts_ms, :event_ts, :temperature, :humidity, :source, :ingest_ts)
                    ON CONFLICT (raw_id) DO NOTHING
                """), df_clean.to_dict('records'))
                logger.info(f"Loaded {len(df_clean)} clean records.")
    
            # 2. Load Quarantine Data with UPSERT
            if not df_quarantine.empty:
                conn.execute(text("""
                    INSERT INTO iot_quarantine.quarantine_events 
                    (raw_id, run_id, reject_reason, device_id, event_ts_ms, event_ts, temperature, humidity, source, ingest_ts)
                    VALUES (:raw_id, :run_id, :reject_reason, :device_id, :event_ts_ms, :event_ts, :temperature, :humidity, :source, :ingest_ts)
                    ON CONFLICT (raw_id) DO NOTHING
                """), df_quarantine.to_dict('records'))
                logger.info(f"Loaded {len(df_quarantine)} quarantined records.")

            # 3. COMPLETE WATERMARK UPDATE
            # Fills last_ingest_ts, last_batch_id, and last_status
            conn.execute(text("""
                INSERT INTO iot_control.pipeline_metadata 
                (pipeline_name, last_ingest_ts, last_batch_id, last_status, updated_at)
                VALUES ('sensor_etl', :max_ts, :max_id, 'SUCCESS', NOW())
                ON CONFLICT (pipeline_name) 
                DO UPDATE SET 
                    last_batch_id = EXCLUDED.last_batch_id,
                    last_ingest_ts = EXCLUDED.last_ingest_ts,
                    last_status = EXCLUDED.last_status,
                    updated_at = NOW();
            """), {"max_id": new_max_id, "max_ts": new_max_ts})
            
        logger.info(f"Watermark updated | ID: {new_max_id} | TS: {new_max_ts}")

    except Exception:
        logger.exception(f"Load failed for Run ID: {run_id}")
        raise
    
    runtime = time.time() - start
    logger.info(f"Load completed | runtime={runtime:.2f}s")
