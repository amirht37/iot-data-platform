import pandas as pd
import numpy as np
import time
from Logger import logger  

def transform_data(df, run_id): 
    logger.info("Transform started")
    start = time.time()

    try:
        # --- 1. Parse nested JSON payloads ---
        # This turns the JSON blob into real columns
        df_payload = pd.json_normalize(df["payload"])

        # Combine the metadata (id, source) with the actual sensor data
        df = pd.concat(
            [df[["raw_id", "source", "ingest_ts"]], df_payload],
            axis=1
        )
        
        # --- 2. Identity & Timing ---
        df["run_id"] = run_id 
        df["ingest_ts"] = pd.to_datetime(df["ingest_ts"], utc=True)

        # SAFETY CHECK: If event_ts_ms is missing from JSON, create it from ingest_ts
        if "event_ts_ms" not in df.columns:
            df["event_ts_ms"] = (df["ingest_ts"].view('int64') // 10**6)
        else:
            # Fill any individual missing values in the column
            df["event_ts_ms"] = df["event_ts_ms"].fillna(df["ingest_ts"].view('int64') // 10**6)

        # Convert the numeric MS back into a readable timestamp for 'event_ts'
        df["event_ts"] = pd.to_datetime(df["event_ts_ms"], unit="ms", utc=True, errors="coerce")

        # --- 3. Sorting & Cleaning ---
        df = df.sort_values("ingest_ts")

        # Convert sensor readings to numbers (Force 'coerce' to handle strings/junk)
        df["temperature"] = pd.to_numeric(df["temperature"], errors="coerce")
        df["humidity"] = pd.to_numeric(df["humidity"], errors="coerce")

        # --- 4. Validation Rules (The Bouncer) ---
        df["reject_reason"] = None

        # Logic: If crucial data is missing or physically impossible, mark it for Quarantine
        df.loc[df["device_id"].isna(), "reject_reason"] = "missing_device_id"
        df.loc[df["temperature"].isna(), "reject_reason"] = "invalid_temperature"
        df.loc[df["humidity"].isna(), "reject_reason"] = "invalid_humidity"

        # Shrimp Farm range checks (-40 to 85 is standard for these sensors)
        df.loc[~df["temperature"].between(-40, 85), "reject_reason"] = "temp_out_of_range"
        df.loc[~df["humidity"].between(0, 100), "reject_reason"] = "humidity_out_of_range"

        # --- 5. Routing (Medallion Splitting) ---
        valid_mask = df["reject_reason"].isna()

        # Columns exactly as they appear in your 'iot_clean.clean_events' table
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

        # Extract Clean data
        df_clean = df.loc[valid_mask, clean_cols].copy()
        
        # Extract Quarantine data (everything else)
        df_quarantine = df.loc[~valid_mask].copy()

    except Exception:
        logger.exception("Transform failed")
        raise 

    runtime = time.time() - start
    logger.info(
        f"Transform completed | clean_rows={len(df_clean)} | quarantined_rows={len(df_quarantine)} | runtime={runtime:.2f}s"
    )
    return df_clean, df_quarantine


