from flask import Flask, request, jsonify
import psycopg2
from psycopg2 import pool
import json
import os
import sys
import traceback

app = Flask(__name__)

# 🏛️ PURE-ENV CONFIGURATION
# Pulling the 'Maserati Keys' from the .env via Docker environment
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "postgres"),
    "port": int(os.getenv("DB_PORT", 5432)),
    "dbname": os.getenv("IOT_DB"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD")
}

# 1. Initialize Industrial Connection Pool
try:
    # 1 min connection, 20 max to handle high-concurrency sensor bursts
    db_pool = psycopg2.pool.ThreadedConnectionPool(1, 20, **DB_CONFIG)
    print("--- DB CONNECTION POOL INITIALIZED ---", file=sys.stderr)
    
    # 2. INFRASTRUCTURE HANDSHAKE
    # Verify the vault (iot_raw) exists before accepting any traffic
    conn = db_pool.getconn()
    cur = conn.cursor()
    cur.execute("SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'iot_raw'")
    if not cur.fetchone():
        print("CRITICAL: Schema 'iot_raw' not found. Check init_db.sh", file=sys.stderr)
        sys.exit(1)
    cur.close()
    db_pool.putconn(conn)
    print("--- INFRASTRUCTURE HANDSHAKE SUCCESSFUL ---", file=sys.stderr)

except Exception as e:
    print(f"FAILED TO START API: {e}", file=sys.stderr)
    sys.exit(1)

@app.route("/ingest", methods=["POST"])
def ingest():
    conn = None
    try:
        # Force JSON parsing
        data = request.get_json(force=True)
        
        # 3. Fast-Path Connection Acquisition
        conn = db_pool.getconn()
        cur = conn.cursor()
        
        source_val = data.get('source', 'flask_api')
        
        # 4. Atomic Write to Bronze Layer (iot_raw)
        cur.execute(
            "INSERT INTO iot_raw.raw_events (payload, source) VALUES (%s, %s)",
            (json.dumps(data), source_val)
        )
        
        conn.commit()
        cur.close()
        db_pool.putconn(conn)
        return jsonify({"status": "success", "msg": "Payload Vaulted"}), 200
        
    except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
        # SELF-HEALING: If DB restarted or timed out, kill the dead pipe
        print(f"CONNECTION LOST: {str(e)}", file=sys.stderr)
        if conn:
            db_pool.putconn(conn, close=True) 
        return jsonify({"status": "error", "message": "DB Interrupted"}), 500

    except Exception as e:
        print(f"INGESTION ERROR: {traceback.format_exc()}", file=sys.stderr)
        if conn:
            try:
                conn.rollback()
            except:
                pass 
            db_pool.putconn(conn)
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == "__main__":
    # 🔥 THREADED=TRUE: Unlocks multi-core sensor ingestion
    app.run(host="0.0.0.0", port=5000, threaded=True)
