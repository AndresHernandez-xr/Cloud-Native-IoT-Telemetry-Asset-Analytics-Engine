# Cloud-Native-IoT-Telemetry-Asset-Analytics-Engine
This repository features an end-to-end production-grade data engineering pipeline deployed within Databricks. 

Utilizing a **Medallion Architecture (Bronze → Silver → Gold)** powered by **Spark Structured Streaming** and **Databricks Auto Loader**, the pipeline tracks telemetry streams, executes data-cleansing quality constraints, isolates operational anomalies, and exposes real-time aggregated metrics for business intelligence.

### ⚡ Key Engineering Achievements
* **Schema Enforcement & Evolution:** Leveraged Databricks Auto Loader (`cloudFiles`) to dynamically track incoming JSON data structures while enforcing a strict `StructType` schema grid to prevent runtime payload corruption.
* **Idempotent Micro-Batch Processing:** Integrated `.trigger(availableNow=True)` processing controls, enabling infinite incremental ingestion scales while capping serverless compute expenditures at near-zero credits.
* **Modern Cloud Lineage Auditing:** Bypassed restrictive Shared Unity Catalog compute barriers by utilizing advanced hidden metadata column mapping (`_metadata.file_path`) to capture unalterable file data origins.
* **High-Performance Volatile Caching:** Engineered an in-memory Delta-to-RAM streaming transaction matrix to support lightning-fast executive analytics without incurring cloud storage data-egress fees.
---
## 🏗️ Architecture & Data Flow Matrix

1. **Bronze Layer (Ingestion):** Raw, semi-structured JSON telemetry files are dropped into a landing folder. Auto Loader reads the directory incrementally, tracking process checkpoints to prevent duplicate records.
2. **Silver Layer (Cleanse & Transform):** * Flattens nested JSON structures (`metrics.temperature_c`, `metrics.battery_percentage`).
   * Generates systemic audit trails via `_metadata.file_path` and `current_timestamp()`.
   * Enforces data-quality rules: isolates out-of-bounds instrument spikes (e.g., `-999.0°C` telemetry testing failures) into a boolean tracking column (`is_anomalous`).
3. **Gold Layer (Business Intelligence):** Aggregates asset metrics globally by `device_type` to track fleet volume totals, average temperatures, critical status notifications, and battery degradation flags.
---
## 🛠️ Environment Setup & Execution Protocol

### Prerequisites
* **Platform:** Databricks Environment (Community, Trial, or Enterprise Workspace)
* **Compute Runtime:** Databricks Runtime (DBR) 14.3 LTS or higher (Supporting Spark Connect and Python 3.12)
* **Security Context:** Shared or Serverless Unity Catalog Compute Policy

### Step-by-Step Deployment
To execute this architecture on your own workspace instance, run the codebase files sequentially inside a Databricks Notebook environment:

#### 1. Environment Purge & State Reset
Execute the setup routine to clear background transactional loops and generate a pristine workspace tracking directory:

import os
import shutil

print("🧹 Step 1: Initiating complete environment wipe...")

# 1. Stop any background active stream contexts
for q in spark.streams.active: 
    q.stop()

# 2. Re-establish paths inside your open personal folder
user_email = spark.sql("SELECT current_user()").collect()[0][0]
base_dir = f"/Workspace/Users/{user_email}/sandbox_project_staged"

landing_path = f"{base_dir}/landing"
checkpoint_path = f"{base_dir}/checkpoints"
schema_tracking_path = f"{base_dir}/schema_tracking"

# 3. Physically delete old directory tracking files
if os.path.exists(landing_path): shutil.rmtree(landing_path)
if os.path.exists(checkpoint_path): shutil.rmtree(checkpoint_path)
if os.path.exists(schema_tracking_path): shutil.rmtree(schema_tracking_path)

# 4. Re-create pristine, completely empty tracking directories
os.makedirs(landing_path, exist_ok=True)
os.makedirs(checkpoint_path, exist_ok=True)
os.makedirs(schema_tracking_path, exist_ok=True)

print("✅ Success! All old system schemas and file histories have been purged.")
print("👉 Proceed to Step 2.")

2. Active Fleet Stream Simulator

Generate a highly realistic, messy, multi-batch JSON stream containing missing properties and extreme telemetry outliers:

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

3. Core Medallion Processing Pipeline

Compile the schema rules and initiate the transactional streaming engine to push records through Bronze, Silver, and Gold matrices:

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

# --- 1. DEFINE PRODUCTION SCHEMA ---
iot_schema = StructType([
    StructField("device_id", IntegerType(), True),
    StructField("device_type", StringType(), True),
    StructField("status", StringType(), True),
    StructField("event_timestamp", StringType(), True),
    StructField("metrics", StructType([
        StructField("temperature_c", DoubleType(), True),
        StructField("battery_percentage", DoubleType(), True)
    ]), True)
])

print("📥 Stage 1: Auto Loader picking up raw files with strict schema parsing (Bronze)...")
raw_stream = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .schema(iot_schema) 
    .option("cloudFiles.schemaLocation", schema_tracking_path)
    .load(landing_path)
)

print("🧹 Stage 2: Unpacking structures and enforcing quality rules (Silver)...")
silver_stream_df = (
    raw_stream
    .withColumn("ingestion_timestamp", F.current_timestamp())
    # Bypassing the UC block by using the secure, modern hidden metadata column mapping
    .withColumn("source_file_origin", F.col("_metadata.file_path")) # <-- Fixed here!
    .withColumn("temp_c", F.col("metrics.temperature_c"))
    .withColumn("battery_pct", F.col("metrics.battery_percentage"))
    .withColumn("status_cleaned", F.coalesce(F.col("status"), F.lit("UNKNOWN")))
    .withColumn("is_anomalous", F.when(F.col("temp_c") == -999.0, True).otherwise(False))
    .withColumn("event_time", F.to_timestamp(F.col("event_timestamp"), "yyyy-MM-dd'T'HH:mm:ss'Z'"))
    .drop("metrics", "status", "event_timestamp")
)

print("⚡ Saving streaming micro-batch into granular memory sink...")
silver_query = (
    silver_stream_df.writeStream
    .format("memory")
    .queryName("silver_fleet_records")
    .option("checkpointLocation", checkpoint_path)
    .outputMode("append")
    .trigger(availableNow=True) 
    .start()
)
silver_query.awaitTermination()
print("✅ Bronze -> Silver transaction committed successfully.")


print("\n📈 Stage 3: Compiling executive business views (Gold)...")
clean_silver_df = spark.read.table("silver_fleet_records")

gold_analytics_df = (
    clean_silver_df
    .filter(F.col("is_anomalous") == False) 
    .groupBy("device_type")
    .agg(
        F.count("device_id").alias("total_pings_processed"),
        F.round(F.avg("temp_c"), 2).alias("average_temperature_c"),
        F.round(F.min("battery_pct"), 1).alias("lowest_recorded_battery_pct"),
        F.sum(F.when(F.col("status_cleaned") == "CRITICAL", 1).otherwise(0)).alias("critical_alert_count")
    )
    .withColumn("last_refresh_update", F.current_timestamp())
)

# Register the aggregated report into cluster memory
gold_analytics_df.createOrReplaceTempView("gold_fleet_analytics")
print("✅ Gold analytics table compiled natively in memory!")
print("👉 Proceed to Step 4.")

4. Executive Analytical Verification

Query the live memory matrix to render business intelligence reports:

display(spark.read.table("gold_fleet_analytics").orderBy("average_temperature_c", descending=False))

<img width="1105" height="709" alt="image" src="https://github.com/user-attachments/assets/ed92e114-5ee9-49a6-92ca-7d12a705eb82" />


Architecture Diagram
┌────────────────────────────────────────────────────────────────────────┐
 │                      FLEET ASSET EDGE ENVIROMENT                      │
 └────────────────────────────────────┬───────────────────────────────────┘
                                      │
                         [ Python JSON Generation ]
                                      ▼
 ┌────────────────────────────────────────────────────────────────────────┐
 │                      BRONZE TIER: LANDING ZONE                         │
 │               Path: /Workspace/Users/.../landing/                      │
 │   - Raw, semi-structured, nested telemetry JSON log batches            │
 └────────────────────────────────────┬───────────────────────────────────┘
                                      │
                     [ Databricks Auto Loader Stream ]
                     [   .trigger(availableNow=True) ]
                                      ▼
 ┌────────────────────────────────────────────────────────────────────────┐
 │                 SILVER TIER: ENRICHMENT & QUALITY CONTROL              │
 │                   Target Memory Sink: silver_fleet_records             │
 │   - Flat maps metrics.temperature_c & metrics.battery_percentage       │
 │   - Captures metadata trace lineage via _metadata.file_path            │
 │   - Flags instrumentation test spikes (-999.0°C) as anomalous         │
 └────────────────────────────────────┬───────────────────────────────────┘
                                      │
                   [ High-Performance Batch Aggregation ]
                                      ▼
 ┌────────────────────────────────────────────────────────────────────────┐
 │                    GOLD TIER: EXECUTIVE METRICS                        │
 │                Target Temp View: gold_fleet_analytics                  │
 │   - Excludes outliers and groups metrics universally by device_type    │
 │   - Calculates fleet volume, rolling avgs, & critical alert counts     │
 └────────────────────────────────────┬───────────────────────────────────┘
                                      │
                       [ Analytical Reporting Layer ]
                                      ▼
 ┌────────────────────────────────────────────────────────────────────────┐
 │                 DATABRICKS NOTEBOOK VISUALIZATION                      │
 │            Output Grid: display(spark.read.table(...))                 │
 └────────────────────────────────────────────────────────────────────────┘


graph TD
    %% Styling Configuration
    classDef bronze fill:#b87333,stroke:#333,stroke-width:2px,color:#fff;
    classDef silver fill:#aaa,stroke:#333,stroke-width:2px,color:#000;
    classDef gold fill:#d4af37,stroke:#333,stroke-width:2px,color:#000;
    classDef process fill:#f4f4f4,stroke:#666,stroke-width:1px,stroke-dasharray: 5 5;
    classDef platform fill:#1f4e79,stroke:#111,stroke-width:2px,color:#fff;

    %% Workflow Nodes
    Edge[📡 Fleet Asset Edge Logs] -->|Python json.dumps Context| Bronze[📥 Bronze Tier: Landing Directory]
    
    subgraph Ingestion_Engine [Databricks Streaming Pipeline]
        Bronze -->|Auto Loader Ingestion| StreamProcess(🧹 Silver Parsing & Data Cleansing)
        StreamProcess -->|Incremental Checkpointing| Silver[🥈 Silver Tier: silver_fleet_records RAM Table]
    end

    subgraph Analytical_Engine [Batch Engine]
        Silver -->|Excludes Anomalous Rows| GoldProcess(📈 Metric Aggregation & Rounding)
        GoldProcess -->|CreateOrReplaceTempView| Gold[🥇 Gold Tier: gold_fleet_analytics View]
    end

    Gold -->|Spark SQL Client Execution| Visual[📊 BI Reporting Dashboard & display Matrix]

    %% Class Assignments
    class Edge platform;
    class Bronze bronze;
    class StreamProcess,GoldProcess process;
    class Silver silver;
    class Gold gold;
    class Visual platform;
