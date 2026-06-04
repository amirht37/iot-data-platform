from flask import Flask, request, jsonify
import psycopg2
from psycopg2 import pool
import json
import os
import csv
import time
import fcntl
from datetime import datetime
from multiprocessing import Value, Lock
from Logger import logger  

app = Flask(__name__)




DB_CONFIG = {
    "host": os.getenv("DB_HOST", "postgres"),
    "port": int(os.getenv("DB_PORT", 5432)),
    "dbname": os.getenv("IOT_DB"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD")
}




LOCAL_BUFFER_DIR = "/app/storage/buffer"
QUARANTINE_DIR = "/app/storage/quarantine"
BUFFER_CSV_FILE = os.path.join(LOCAL_BUFFER_DIR, "sensor_backup.csv")
QUARANTINE_CSV_FILE = os.path.join(QUARANTINE_DIR, "quarantine_dead_letter.csv")

os.makedirs(LOCAL_BUFFER_DIR, exist_ok=True)
os.makedirs(QUARANTINE_DIR, exist_ok=True)


SHARED_DB_ONLINE = Value('b', 1)
SHARED_LAST_CHECK = Value('d', 0.0)
SHARED_COOL_DOWN = Value('d', 30.0)
IPC_LOCK = Lock()  

db_pool = None

def initialize_connection_pool_safely():
    """Thread-safe and process-safe pool initialization checking global IPC circuit state"""
    global db_pool
    if db_pool is not None:
        return True
        
    if SHARED_DB_ONLINE.value == 0:
        return False
        
    with IPC_LOCK:
        if db_pool is not None:
            return True
        if SHARED_DB_ONLINE.value == 0:
            return False
            
        try:
            db_pool = psycopg2.pool.ThreadedConnectionPool(1, 20, **DB_CONFIG)
            logger.info("--- INDUSTRIAL CONNECTION POOL ONLINE ---")
            return True
        except Exception as e:
            logger.warning(f"⏳ [Pool Delay] PostgreSQL offline on boot. Locking Circuit Open: {e}")
            SHARED_DB_ONLINE.value = 0
            SHARED_LAST_CHECK.value = time.time()
            SHARED_COOL_DOWN.value = 30.0
            return False

def validate_iot_schema(data):
    if not data or not isinstance(data, dict):
        return False
    required_fields = ["device_id", "temperature", "humidity", "event_ts_ms"]
    if not all(field in data for field in required_fields):
        return False
    try:
        float(data['temperature'])
        float(data['humidity'])
        int(data['event_ts_ms'])
        return True
    except (ValueError, TypeError):
        return False

def append_to_csv_buffer(data):
    """Append sensor data to single CSV file with file locking for multi-worker safety"""
    csv_columns = ['payload', 'ingest_ts', 'source']
    row_data = {
        'payload': json.dumps(data),
        'ingest_ts': data.get('ingest_ts'),
        'source': data.get('source', 'raspberry_pi_pico')
    }
    
    file_exists = os.path.isfile(BUFFER_CSV_FILE)
    with open(BUFFER_CSV_FILE, 'a', newline='') as f:
        fcntl.flock(f.fileno(), fcntl.LOCK_EX) # Advisory Kernel Lock
        try:
            writer = csv.DictWriter(f, fieldnames=csv_columns)
            if not file_exists:
                writer.writeheader()
            writer.writerow(row_data)
        finally:
            fcntl.flock(f.fileno(), fcntl.LOCK_UN)

def append_to_quarantine_dlq(raw_data, ingest_ts, error_reason):
    """Funnels corrupted payloads into a single locked Dead-Letter CSV file"""
    csv_columns = ['raw_payload', 'quarantine_ts', 'rejection_reason']
    row_data = {
        'raw_payload': json.dumps(raw_data) if isinstance(raw_data, (dict, list)) else str(raw_data),
        'quarantine_ts': ingest_ts,
        'rejection_reason': error_reason
    }
    
    file_exists = os.path.isfile(QUARANTINE_CSV_FILE)
    with open(QUARANTINE_CSV_FILE, 'a', newline='') as f:
        fcntl.flock(f.fileno(), fcntl.LOCK_EX)
        try:
            writer = csv.DictWriter(f, fieldnames=csv_columns)
            if not file_exists:
                writer.writeheader()
            writer.writerow(row_data)
            logger.warning(f"⚠️ [DLQ Firewall] Corrupted packet securely vaulted to single quarantine registry.")
        finally:
            fcntl.flock(f.fileno(), fcntl.LOCK_UN) 

@app.route("/ingest", methods=["POST"])
def ingest():
    global db_pool
    conn = None
    now = datetime.utcnow()
    timestamp_marker = now.strftime("%Y%m%d_%H%M%S_%f")
    ingest_ts_iso = now.isoformat() + "Z"
    current_time = time.time()
    is_probe_thread = False 

    try:
        
        data = request.get_json(force=True)
        
       
        if not validate_iot_schema(data):
           
            append_to_quarantine_dlq(data, ingest_ts_iso, "invalid_pico_schema_or_datatype")
            return jsonify({"status": "quarantined", "reason": "invalid_pico_schema_or_datatype"}), 202

        data["ingest_ts"] = ingest_ts_iso
        source_val = data.get('source', 'raspberry_pi_pico')


        pool_active = initialize_connection_pool_safely()

        with IPC_LOCK:
            if not pool_active and SHARED_DB_ONLINE.value == 0:
                if current_time - SHARED_LAST_CHECK.value >= SHARED_COOL_DOWN.value:
                    is_probe_thread = True
                    should_attempt_db = True
                    SHARED_LAST_CHECK.value = current_time
                else:
                    should_attempt_db = False
            elif SHARED_DB_ONLINE.value == 1:
                should_attempt_db = True
            else:
                if current_time - SHARED_LAST_CHECK.value >= SHARED_COOL_DOWN.value:
                    should_attempt_db = True
                    is_probe_thread = True
                    SHARED_LAST_CHECK.value = current_time
                else:
                    should_attempt_db = False

        if not should_attempt_db:
            raise psycopg2.OperationalError("Circuit Open: Inter-Process Fast-Path Active.")

        if is_probe_thread:
            logger.info(f"🔄 [Circuit Breaker IPC] Master probe window open. Testing network link. Cooldown: {SHARED_COOL_DOWN.value}s")


        if db_pool is None:
            try:
                db_pool = psycopg2.pool.ThreadedConnectionPool(1, 20, **DB_CONFIG)
            except Exception as pool_err:
                raise psycopg2.OperationalError(f"Probe Failed: Pool initialization crashed: {pool_err}")

        conn = db_pool.getconn()
        cur = conn.cursor()
        

        db_ingest_ts = data.pop("ingest_ts", ingest_ts_iso)
        
        cur.execute(
            """
            INSERT INTO iot_raw.raw_events (payload, ingest_ts, source) 
            VALUES (%s, %s, %s)
            """,
            (json.dumps(data), db_ingest_ts, source_val)
        )
        conn.commit()
        cur.close()
        db_pool.putconn(conn)
        
        with IPC_LOCK:
            if SHARED_DB_ONLINE.value == 0:
                logger.info("✅ [Circuit Breaker IPC] Network recovered. Resetting all workers closed.")
            SHARED_DB_ONLINE.value = 1
            SHARED_COOL_DOWN.value = 30.0
            
        return jsonify({"status": "success", "msg": "Payload Vaulted into Columns"}), 200
        
    except (psycopg2.OperationalError, psycopg2.InterfaceError, Exception) as e:

        with IPC_LOCK:
            if SHARED_DB_ONLINE.value == 1:
                logger.error(f"🚨 [CIRCUIT TRIPPED IPC] Initial Network Failure Intercepted: {str(e)}")
                SHARED_DB_ONLINE.value = 0
                SHARED_LAST_CHECK.value = current_time
                SHARED_COOL_DOWN.value = 30.0
            else:
                if is_probe_thread:
                    SHARED_COOL_DOWN.value = min(SHARED_COOL_DOWN.value * 2, 300.0)
                    SHARED_LAST_CHECK.value = current_time
                    logger.warning(f"⏳ [IPC Backoff Scaled] Network dead. All worker nodes locked out for {SHARED_COOL_DOWN.value}s")

        if conn and db_pool:
            try: 
                db_pool.putconn(conn, close=True)
            except: 
                pass


        if 'data' in locals() and isinstance(data, dict):
            data["ingest_ts"] = ingest_ts_iso
            data["source"] = "raspberry_pi_pico"
            append_to_csv_buffer(data)
            
        return jsonify({
            "status": "buffered", 
            "message": "Circuit active. Data safely persisted to local disk storage."
        }), 202

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, threaded=True)
