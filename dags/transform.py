import pandas as pd
import numpy as np
import time
from Logger import logger  

def transform_data(df, run_id): 
    logger.info("Transform started")
    start = time.time()

    try:
        df_payload = pd.json_normalize(df["payload"])

       
        df = pd.concat(
            [df[["raw_id", "source", "ingest_ts"]], df_payload],
            axis=1
        )
        
      
        df["run_id"] = run_id 
        df["ingest_ts"] = pd.to_datetime(df["ingest_ts"], utc=True)

       
        if "event_ts_ms" not in df.columns:
            df["event_ts_ms"] = (df["ingest_ts"].astype('int64') // 10**6)
        else:
            
            df["event_ts_ms"] = df["event_ts_ms"].fillna(df["ingest_ts"].astype('int64') // 10**6)

      
        df["event_ts"] = pd.to_datetime(df["event_ts_ms"], unit="ms", utc=True, errors="coerce")

      
        df = df.sort_values("ingest_ts")

       
        df["temperature"] = pd.to_numeric(df["temperature"], errors="coerce")
        df["humidity"] = pd.to_numeric(df["humidity"], errors="coerce")

      
        df["reject_reason"] = None

     
        df.loc[df["device_id"].isna(), "reject_reason"] = "missing_device_id"
        df.loc[df["temperature"].isna(), "reject_reason"] = "invalid_temperature"
        df.loc[df["humidity"].isna(), "reject_reason"] = "invalid_humidity"

      
        df.loc[~df["temperature"].between(-40, 85), "reject_reason"] = "temp_out_of_range"
        df.loc[~df["humidity"].between(0, 200), "reject_reason"] = "humidity_out_of_range"

      
        valid_mask = df["reject_reason"].isna()

       
        clean_cols = [
            "raw_id",      
            "run_id",      
            "device_id",
            "event_ts_ms",
            "event_ts",
            "temperature",
            "humidity",
            "source",
            "ingest_ts",
        ]

     
        df_clean = df.loc[valid_mask, clean_cols].copy()
        
     
        df_quarantine = df.loc[~valid_mask].copy()

    except Exception:
        logger.exception("Transform failed")
        raise 

    runtime = time.time() - start
    logger.info(
        f"Transform completed | clean_rows={len(df_clean)} | quarantined_rows={len(df_quarantine)} | runtime={runtime:.2f}s"
    )
    return df_clean, df_quarantine


