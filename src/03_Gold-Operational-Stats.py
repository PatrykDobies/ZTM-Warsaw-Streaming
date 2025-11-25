# Databricks notebook source
from pyspark.sql.functions import col, to_date, hour, count, approx_count_distinct

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
silver_path = f"abfss://ztm-datalake@{storage_account_name}.dfs.core.windows.net/ztm/silver"
gold_operational_path = f"abfss://ztm-datalake@{storage_account_name}.dfs.core.windows.net/ztm/gold/operational_stats"

# Loading data from silver Layer
print("Reading data from Silver Layer...")
silver_df = spark.read.format("delta").load(silver_path)

# Date filter
target_date = "2025-11-20"

filtered_silver_df = silver_df.filter(
    to_date(col("signal_timestamp")) == target_date
)

# Aggregation
gold_operational_df = filtered_silver_df \
    .withColumn("date", to_date(col("signal_timestamp"))) \
    .withColumn("hour_of_day", hour(col("signal_timestamp"))) \
    .groupBy("date", "hour_of_day", "line_number") \
    .agg(
        count("*").alias("total_signals_count"),
        approx_count_distinct("vehicle_id").alias("active_vehicles_count")
    ) \
    .orderBy("date", "hour_of_day", "line_number")

# Write to gold operational layer
print(f"Writing aggregated data.")

gold_operational_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(gold_operational_path)

print("Operational Analysis completed.")
display(gold_operational_df.limit(10))
