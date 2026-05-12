import network
import urequests
import time
import json
import ntptime
from machine import Pin
import dht

SSID = "YOUR_WIFI_SSDID"
PASSWORD = "YOUR_WIFI_PASSWORD"
SERVER_URL = "http://YOUR_HOST_URL/ingest"   # ← change if IP is different

sensor = dht.DHT22(Pin(16))
led = Pin("LED", Pin.OUT)

def blink(n, delay=0.2):
    for _ in range(n):
        led.on()
        time.sleep(delay)
        led.off()
        time.sleep(delay)

# ---- WIFI CONNECT ----
wlan = network.WLAN(network.STA_IF)
wlan.active(True)
wlan.connect(SSID, PASSWORD)
print("Connecting to WiFi...")
while not wlan.isconnected():
    blink(1, 0.5)
print("WiFi connected:", wlan.ifconfig())
led.on()

# ---- NTP TIME SYNC (very important) ----
print("Syncing time with NTP...")
try:
    ntptime.settime()          # Sync with NTP server
    print("NTP sync successful")
except Exception as e:
    print("NTP sync failed:", e)

# ---- MAIN LOOP ----
while True:
    try:
        # Read sensor
        sensor.measure()
        temperature = sensor.temperature()
        humidity = sensor.humidity()
        print(f"Sensor OK → Temp: {temperature}°C | Humidity: {humidity}%")
        
        # Build payload
        data = {
            "device_id": "pico_001",
            "temperature": temperature,
            "humidity": humidity,
            "event_ts_ms": int(time.time() * 1000)
        }
        
        # SEND DATA with timeout
        print("Sending to Flask...")
        blink(1, 0.05)   # quick blink before sending
        
        r = urequests.post(
            SERVER_URL,
            data=json.dumps(data),
            headers={"Content-Type": "application/json"},
            timeout=5.0          # ← VERY IMPORTANT
        )
        
        print("Response status:", r.status_code)
        if r.status_code == 200:
            print("✅ SUCCESS sent to server")
            blink(2, 0.1)        # double blink = success
        else:
            print("Server returned bad status")
            blink(3)
        
        r.close()
        
    except Exception as e:
        print("❌ ERROR:", e)
        blink(5, 0.3)            # long blinks = problem
    
    time.sleep(5)   # send every 5 seconds	


