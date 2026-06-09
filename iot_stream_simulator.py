import json
import time
import random

print("📡 Step 2: Generating complex nested IoT telemetry batch...")

device_types = ["Drone-Alpha", "Truck-Eco", "Forklift-Heavy", "Sensor-Static"]
statuses = ["OK", "WARN", "CRITICAL", "UNKNOWN"]
simulated_logs = []

# Generate 500 records explicitly utilizing a nested "metrics" map
for i in range(500):
    device_id = int(random.randint(5000, 5015))
    dev_type = random.choice(device_types)
    
    # Introduce anomalies for Drone-Alpha
    if dev_type == "Drone-Alpha" and random.random() < 0.15:
        temperature = -999.0  
        status = "CRITICAL"
    else:
        temperature = round(random.uniform(20.0, 110.0), 2)
        status = random.choice(statuses)
        
    log_entry = {
        "device_id": device_id,
        "device_type": dev_type,
        "metrics": {  # <-- This is the nested object Auto Loader will map
            "temperature_c": temperature,
            "battery_percentage": round(random.uniform(15.0, 100.0), 1)
        },
        "status": status,
        "event_timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    }
    simulated_logs.append(log_entry)

# Write the data out cleanly to your personal user folder path
file_path = f"{landing_path}/telemetry_batch_v1.json"
with open(file_path, "w") as f:
    for entry in simulated_logs:
        f.write(json.dumps(entry) + "\n")

print(f"✅ Success! 500 clean, nested telemetry records dropped into: {file_name}")
print("👉 Proceed to Step 3.")