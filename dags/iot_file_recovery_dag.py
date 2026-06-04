import os
import csv
import json
import logging
import psycopg2
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

BUFFER_CSV_FILE = "/app/storage/buffer/sensor_backup.csv"

logger = logging.getLogger("airflow.task")

def recover_offline_buffer_data():
    """Reads local fallback buffer file, cleans payloads, streams to Postgres, and purges file."""
    
   
    if not os.path.exists(BUFFER_CSV_FILE):
        logger.info("Empty runway. No local fallback buffer file detected for processing.")
        return
    
    
    file_size = os.path.getsize(BUFFER_CSV_FILE)
    if file_size == 0:
        logger.info("Buffer file exists but is empty (0 bytes). Nothing to process.")
        return
    
    logger.info(f"Buffer file detected: {BUFFER_CSV_FILE} ({file_size} bytes)")

    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        dbname=os.getenv("IOT_DB")
    )
    
    records_to_insert = []
    total_rows_read = 0
    skipped_rows = 0
    
    logger.info(f"Opening local advisory stream named volume: {BUFFER_CSV_FILE}")
    with open(BUFFER_CSV_FILE, "r") as f:
        reader = csv.reader(f)
        for row in reader:
            total_rows_read += 1
            
            # Skip empty rows
            if not row or len(row) < 3:
                skipped_rows += 1
                logger.warning(f"Row {total_rows_read} skipped: insufficient columns")
                continue
                
            try:
               
                raw_payload_str = row[0]
                ingest_ts = row[1]
                source = row[2]
                
                # Parse JSON string into a python dictionary object
                payload_json = json.loads(raw_payload_str)
                
               
                payload_json.pop("ingest_ts", None)
                
                records_to_insert.append((
                    json.dumps(payload_json), 
                    ingest_ts,                
                    source
                ))
            except json.JSONDecodeError as json_err:
                skipped_rows += 1
                logger.error(f"Row {total_rows_read} skipped: invalid JSON - {json_err}")
            except Exception as parse_error:
                skipped_rows += 1
                logger.error(f"Row {total_rows_read} skipped: {parse_error}")

    logger.info(f"Parsing complete: {total_rows_read} rows read, {len(records_to_insert)} valid, {skipped_rows} skipped")

  
    if not records_to_insert:
        logger.warning("No valid records identified after payload parsing loop. File will NOT be cleared.")
        conn.close()
        return

    insert_sql = """
        INSERT INTO iot_raw.raw_events (payload, ingest_ts, source)
        VALUES (%s, %s, %s);
    """
    
    try:
       
        with conn.cursor() as cur:
            logger.info(f"Initiating bulk recovery loading loop for {len(records_to_insert)} payloads...")
            cur.executemany(insert_sql, records_to_insert)
            
        
            try:
               
                with open(BUFFER_CSV_FILE, "w") as f_clear:
                    f_clear.truncate(0)
                logger.info("Local storage buffer cleanly wiped via standard file descriptor handles.")
            except (PermissionError, IOError) as perm_err:
                logger.warning(f"Standard write handle blocked by container ACL locks: {perm_err}")
                logger.info("Deploying atomic drop-and-recreate sequence inside folder boundary...")
                
              
                os.remove(BUFFER_CSV_FILE)
                
               
                with open(BUFFER_CSV_FILE, "w") as f_new:
                    f_new.write("")
                    
                logger.info("Local storage buffer cleanly re-created and unlocked.")
            
          
            conn.commit()
            logger.info(f"✅ Database commit acknowledged. {len(records_to_insert)} payloads synchronized successfully.")
        
    except Exception as db_error:
        conn.rollback()
        logger.error(f"Recovery transaction rolled back due to fatal system event: {db_error}")
        raise db_error
    finally:
        conn.close()



default_args = {
    "owner": "airflow",
    "start_date": datetime(2026, 5, 1),
    "retries": 1
}

with DAG(
    dag_id="iot_offline_file_recovery",
    default_args=default_args,
    schedule="0 */6 * * *",
    max_active_runs=1,
    catchup=False,
    tags=["recovery", "fault-tolerance"]
) as dag:

    execute_recovery = PythonOperator(
        task_id="stream_buffer_to_bronze",
        python_callable=recover_offline_buffer_data
    )
