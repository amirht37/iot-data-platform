import time
import random
import json
import sys



def run_isolated_sensor_node(sensor_id):
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
            
        print(payload)

