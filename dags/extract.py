import pandas as pd
import time
from sqlalchemy import text
from Logger import logger

def extract_data(engine):
    start = time.time()
    logger.info("Extract started")
    
    try:
        with engine.connect() as conn:
            # 1. Get last ID watermark
            result = conn.execute(text("""
                SELECT last_batch_id 
                FROM iot_control.pipeline_metadata 
                WHERE pipeline_name = 'sensor_etl'
            """))
            last_id = result.scalar()
        
        # If first run, start from 0
        if last_id is None:
            last_id = 0
            logger.info("No watermark found. Starting from ID 0.")

        # 2. Extract data newer than last_id
        # We ORDER BY raw_id to ensure the watermark updates correctly later
        query = text("""
            SELECT * 
            FROM iot_raw.raw_events 
            WHERE raw_id > :last_id
            ORDER BY raw_id ASC
        """)
        
        with engine.connect() as conn:
            result = conn.execute(query, {"last_id": last_id})
            df = pd.DataFrame(result.fetchall(), columns=result.keys())
        
        if df.empty:
            logger.info("No new data found.")
            return None
            
        runtime = time.time() - start
        logger.info(f"Extract completed | rows={len(df)} | runtime={runtime:.2f}s")
        return df
        
    except Exception:
        logger.exception("Extract failed")
        raise
