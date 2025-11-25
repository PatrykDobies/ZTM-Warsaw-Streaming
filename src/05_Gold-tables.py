# Databricks notebook source
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

gold_operational_path = f"abfss://ztm-datalake@{storage_account_name}.dfs.core.windows.net/ztm/gold/operational_stats"
gold_spatial_path = f"abfss://ztm-datalake@{storage_account_name}.dfs.core.windows.net/ztm/gold/spatial_heatmap"

# Drop existing tables
spark.sql("DROP TABLE IF EXISTS hive_metastore.default.gold_operational_stats")
spark.sql("DROP TABLE IF EXISTS hive_metastore.default.gold_spatial_heatmap")

# Registering the Gold table in Metastore
spark.sql(f"""
CREATE TABLE IF NOT EXISTS hive_metastore.default.gold_operational_stats
USING DELTA
LOCATION '{gold_operational_path}'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS hive_metastore.default.gold_spatial_heatmap
USING DELTA
LOCATION '{gold_spatial_path}'
""")

print("Tables registered in hive metastore.default")