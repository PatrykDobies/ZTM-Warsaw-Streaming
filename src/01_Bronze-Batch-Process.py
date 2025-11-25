# Databricks notebook source
from pyspark.sql.functions import col, current_timestamp
import re

storage_account_name = "ztmstreaming"
eh_hub_name = "ztm-raw"

# Load from secret
try:
    storage_account_key = dbutils.secrets.get(scope="ztm-scope", key="adls-key")
    connection_string = dbutils.secrets.get(scope="ztm-scope", key="eventhub-conn-string")
    print("Credentials retrieved successfully from Azure Key Vault.")
except Exception as e:
    print("Error retrieving secrets.")
    raise e

# Paths
bronze_path = "abfss://ztm-datalake@ztmstreaming.dfs.core.windows.net/ztm/bronze" 
checkpoint_path_bronze = "abfss://ztm-datalake@ztmstreaming.dfs.core.windows.net/checkpoints/ztm/bronze"

# Configurate access to Data Lake Storage
spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    storage_account_key
)
print("Data Lake Storage configuration complete.")

# Extract Kafka server address from connection string
server_match = re.search(r'Endpoint=sb://(.*?);', connection_string)

if server_match:
    server_address = server_match.group(1)
    kafka_server = f"{server_address}:9093"
    print(f"Target Kafka Server: {kafka_server}")
else:
    raise ValueError("ERROR: Could not extract server address from Connection String.")

# 3. Configurate Kafka connecion
sasl_config = (
    'org.apache.kafka.common.security.plain.PlainLoginModule required '
    'username="$ConnectionString" '
    f"password='{connection_string}';" 
)

kafka_conf = {
    'kafka.bootstrap.servers': kafka_server,
    'subscribe': eh_hub_name,
    'kafka.sasl.mechanism': 'PLAIN',
    'kafka.security.protocol': 'SASL_SSL',
    'kafka.sasl.jaas.config': sasl_config,
    'startingOffsets': 'earliest',
    'failOnDataLoss': 'false'
}

# 4. Ingestion
print("Starting stream ingestion from Azure Event Hubs...")
raw_stream_df = spark \
    .readStream \
    .format("kafka") \
    .options(**kafka_conf) \
    .load()

# Transform to bronze
bronze_df = raw_stream_df \
    .withColumn("json_body", col("value").cast("string")) \
    .withColumn("ingestion_time", current_timestamp()) \
    .select("json_body", col("timestamp").alias("event_timestamp"), "ingestion_time")

# Stream to bronze layer
print("Starting Bronze Stream (Batch Mode)...")
bronze_stream = bronze_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", checkpoint_path_bronze) \
    .option("path", bronze_path) \
    .option("delta.autoOptimize.optimizeWrite", "true") \
    .trigger(availableNow=True) \
    .start()

bronze_stream.awaitTermination()

print(f"Bronze Batch processing completed.")