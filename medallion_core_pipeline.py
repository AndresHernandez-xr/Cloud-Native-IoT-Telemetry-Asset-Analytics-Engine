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

#Display the final gold analytics report to confirm everything is working end-to-end
display(spark.read.table("gold_fleet_analytics").orderBy("average_temperature_c", ascending=False))