import time
import random
import requests
import json
import threading
import sys

TARGET_URL = "http://127.0.0.1:5001/ingest"

def run_isolated_sensor_node(sensor_id):
    """Simulates a completely independent physical Raspberry Pi Pico hardware node"""
    node_tag = f"📡 [Pico-00{sensor_id}]"
    print(f"{node_tag} Thread initialized. Handshake loop active...", file=sys.stderr)
    
    packet_counter = 0
    while True:
        packet_counter += 1
        current_epoch_ms = int(time.time() * 1000)
        
        is_corrupted = random.random() < 0.02
        
        is_out_of_range = random.random() < 0.05
        
        if is_corrupted:
            payload = {
                "device_id": f"pico_00{sensor_id}",
                "temperature": "MALFORMED_PROBE_SPIKE_XYZ",
                "humidity": 45.2,
                "event_ts_ms": current_epoch_ms
            }
        elif is_out_of_range:
            # Valid structure but physically impossible values
            out_of_range_type = random.choice([
                "temp_too_low",
                "temp_too_high", 
                "humidity_negative",
                "humidity_over_100"
            ])
            
            if out_of_range_type == "temp_too_low":
                temp = round(random.uniform(-60, -41), 2)  # Below -40°C
                hum = round(random.uniform(40.0, 60.0), 2)
            elif out_of_range_type == "temp_too_high":
                temp = round(random.uniform(86, 120), 2)  # Above 85°C
                hum = round(random.uniform(40.0, 60.0), 2)
            elif out_of_range_type == "humidity_negative":
                temp = round(random.uniform(22.0, 28.0), 2)
                hum = round(random.uniform(-15, -0.1), 2)  # Negative humidity
            else:  
                temp = round(random.uniform(22.0, 28.0), 2)
                hum = round(random.uniform(100.1, 150), 2)  # Over 100%
            
            payload = {
                "device_id": f"pico_00{sensor_id}",
                "temperature": temp,
                "humidity": hum,
                "event_ts_ms": current_epoch_ms
            }
        else:
            # Normal valid data
            payload = {
                "device_id": f"pico_00{sensor_id}",
                "temperature": round(random.uniform(22.0, 28.0), 2),
                "humidity": round(random.uniform(40.0, 60.0), 2),
                "event_ts_ms": current_epoch_ms
            }
            
        try:
            headers = {'Content-Type': 'application/json'}
            response = requests.post(TARGET_URL, data=json.dumps(payload), headers=headers, timeout=2)
            print(f"{node_tag} Transmitted frame #{packet_counter} -> HTTP {response.status_code} | Server Msg: {response.json().get('status')}")
            
        except requests.exceptions.RequestException as e:
            print(f"⚠️ {node_tag} Transmission blocked: {str(e)[:60]}")
            
        time.sleep(random.uniform(0.5, 1.5))

def launch_concurrency_refinery():
    print("================================================================")
    print("🔥 LAUNCHING HIGH-CONCURRENCY TELEMETRY STRESS TEST (v3.0.0)")
    print("🔥 TARGET ARCHITECTURE: 5 PARALLEL HARDWARE EDGES OVER PORT 5001")
    print("================================================================")
    
    sensor_nodes = [1, 2, 3, 4, 5]
    thread_pool = []
    
    for node_id in sensor_nodes:
        worker_thread = threading.Thread(
            target=run_isolated_sensor_node, 
            args=(node_id,), 
            name=f"PicoThread-{node_id}"
        )
        worker_thread.daemon = True
        thread_pool.append(worker_thread)
        worker_thread.start()
        
    print(f"✅ All {len(thread_pool)} hardware nodes online.")
    print("================================================================")
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n🛑 TERMINATION SIGNAL CAPTURED. Shutting down telemetry fleet cleanly.")
        print("================================================================")

if __name__ == "__main__":
    launch_concurrency_refinery()
