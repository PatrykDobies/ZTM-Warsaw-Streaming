# Databricks notebook source
from pyspark.sql.functions import col, from_json, to_timestamp, current_timestamp, current_date, date_sub
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

storage_account_name = "ztmstreaming"

# Load from secret
try:
    storage_account_key = dbutils.secrets.get(scope="ztm-scope", key="adls-key")
    print("ADLS Key retrieved successfully.")
except Exception as e:
    raise e

# Configurate access to Data Lake Storage
spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    storage_account_key
)
print("Data Lake Storage configuration complete.")

# Paths
bronze_path = f"abfss://ztm-datalake@{storage_account_name}.dfs.core.windows.net/ztm/bronze"
silver_path = f"abfss://ztm-datalake@{storage_account_name}.dfs.core.windows.net/ztm/silver"
checkpoint_path_silver = f"abfss://ztm-datalake@{storage_account_name}.dfs.core.windows.net/checkpoints/ztm/silver"

# ZTM Schema
ztm_schema = StructType([
    StructField("Lines", StringType(), True),
    StructField("Lon", DoubleType(), True),
    StructField("VehicleNumber", StringType(), True),
    StructField("Time", StringType(), True), 
    StructField("Lat", DoubleType(), True),
    StructField("Brigade", StringType(), True)
])

# Read stream frombronze layer
raw_bronze_df = spark.readStream \
    .format("delta") \
    .load(bronze_path)

# Transform to silver
silver_df = raw_bronze_df \
    .select(
        col("ingestion_time"),
        from_json(col("json_body"), ztm_schema, options={"mode": "PERMISSIVE"}).alias("data")
    ) \
    .select(
        col("data.Lines").alias("line_number"),
        col("data.VehicleNumber").alias("vehicle_id"),
        col("data.Brigade").alias("brigade"),
        col("data.Lat").alias("latitude"),
        col("data.Lon").alias("longitude"),
        to_timestamp(col("data.Time"), "yyyy-MM-dd HH:mm:ss").alias("signal_timestamp"),
        col("ingestion_time")
    ) \
    .filter(col("latitude").isNotNull() & col("longitude").isNotNull()) \
    .filter(
        (col("signal_timestamp") >= date_sub(current_date(), 3)) & 
        (col("signal_timestamp") <= current_timestamp())
    )
    .dropDuplicates(["vehicle_id", "signal_timestamp"])

# Stream to silver layer
print("Starting Silver Stream (Batch Mode)...")
silver_stream = silver_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", checkpoint_path_silver) \
    .option("path", silver_path) \
    .option("delta.autoOptimize.optimizeWrite", "true") \
    .trigger(availableNow=True) \
    .start()

silver_stream.awaitTermination()

print(f"Silver Batch processing completed.")

# COMMAND ----------

