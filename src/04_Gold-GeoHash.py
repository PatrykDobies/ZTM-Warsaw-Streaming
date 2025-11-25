# Databricks notebook source
from pyspark.sql.functions import col, round, count, hour, approx_count_distinct, to_date


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
gold_spatial_path = f"abfss://ztm-datalake@{storage_account_name}.dfs.core.windows.net/ztm/gold/spatial_heatmap"

# Loading data from silver Layer
silver_df = spark.read.format("delta").load(silver_path)

# Date filter
target_date = "2025-11-20"

filtered_silver_df = silver_df.filter(
    to_date(col("signal_timestamp")) == target_date
)

# Spatial aggregattion (GRID)
# Rounding lat/lon to 3 decimal places creates a grid of approx. 110m x 70m
gold_spatial_df = filtered_silver_df \
    .withColumn("grid_lat", round(col("latitude"), 3)) \
    .withColumn("grid_lon", round(col("longitude"), 3)) \
    .withColumn("hour_of_day", hour(col("signal_timestamp"))) \
    .groupBy("grid_lat", "grid_lon", "hour_of_day") \
    .agg(
        count("*").alias("traffic_density"),
        approx_count_distinct("line_number").alias("distinct_lines_count")
    )

# Write to gold spatial layer
print(f"Writing spatial data.")

gold_spatial_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(gold_spatial_path)

print("Spatial Analysis completed.")
display(gold_spatial_df.limit(10))