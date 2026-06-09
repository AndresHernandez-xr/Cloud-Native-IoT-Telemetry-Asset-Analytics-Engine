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

**2. Active Fleet Stream Simulator

3. Core Medallion Processing Pipeline

4. Executive Analytical Verification**

Query the live memory matrix to render business intelligence reports:

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



