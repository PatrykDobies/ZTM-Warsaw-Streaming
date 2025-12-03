# ZTM Warsaw Streaming - Step-by-Step Guide
This document provides step-by-step instructions to replicate the project.

## 1. Get API Access
Warsaw Public Transport API (Open Data):

- Register at: https://api.um.warszawa.pl/
- Obtain your apikey.
- Paste your apikey here: https://api.um.warszawa.pl/api/action/busestrams_get/?resource_id=f2e5503e927d-4ad3-9500-4ab9e55deb59&apikey={API_KEY}&type=1

## 2. Set up Azure Infrastructure
Create Resource Group: rg-ztm-warsaw-streaming 
Resources:
- Event Hubs Namespace: Standard Tier. Create an Event Hub instance (Topic) named ztm-raw.
- ADLS Gen2: Storage Account with Hierarchical Namespace enabled. Create a container: ztm-datalake.
- Key Vault: Store your ADLS Key and Event Hub Connection String as secrets.
- Databricks Workspace: Create a compute cluster and install the Maven library: org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.0-preview3.

Folder structure in ADLS (Auto-created by Spark):
- ztm/bronze/ → Raw JSON data (Delta format)
- ztm/silver/ → Cleansed, parsed, and deduplicated data
- ztm/gold/ → Aggregated datasets for reporting

## 3. Run Ingestion Layer (NiFi)
Tool: Apache NiFi (Docker)

- Import the flow template: nifi/ztm_flow.json.
- Configure InvokeHTTP: Insert your ZTM apikey.
- Configure PutAzureEventHub: Insert your Event Hub Namespace and Shared Access Key.

Start the processors to begin streaming data to Azure.

## 4. Run Processing Jobs (Databricks)
Mode: Incremental Batch (Trigger: availableNow)

a. Bronze Layer (Ingestion)

Notebook: src/01_Bronze-Batch-Process.py

Input: Azure Event Hubs (Kafka Protocol)

Output: Raw Delta tables in ztm/bronze/

Action: Reads streaming data and saves history.

b. Silver Layer (Transformation)

Notebook: src/02_Silver-Transform.py

Input: Bronze Delta tables

Output: Cleaned Delta tables in ztm/silver/

Action: Parses JSON schema, filters out GPS errors (future dates), converts types.

## 5. Run Aggregation Jobs (Gold Layer)
Notebooks: src/03_Gold-Operational-Stats.py & src/04_Gold-GeoHash.py & 05_Gold-tables.py

Aggregations:

Operational-Stats → Max active vehicles per line/hour.

GeoHash → Lines density on a 100m grid (GeoHash logic).

tables → registered Tables in Hive Metastore for Power BI consumption.

## 6. Visualize Data (Power BI)
File: powerbi/ztm_warsaw.pbix

- Open Power BI Desktop.
- Connect to Azure Databricks (use Server Hostname & HTTP Path from Cluster settings).
- Authenticate using a Personal Access Token (PAT).
- Refresh data to load the latest Gold tables.
- Interact with the dashboard:
  Use the "Play Axis" to animate buses flow over 24 hours.
  Filter fleet statistics by specific bus lines.

## 7. Run Machine Learning Pipeline (MLOps)
Notebook: src/06_ML_training.py

Goal: Predict traffic density (bus density) based on geolocation (Grid) and time of day.

Input: Gold Layer table (spatial_heatmap).

Tech Stack: Spark MLlib (Random Forest Regressor) & MLflow.

Action:
- Feature Engineering: Vectorizes spatial (grid_lat, grid_lon) and temporal (hour) features.
- Training: Trains a regression model using K-Fold Cross-Validation to tune hyperparameters.
- Tracking: Logs experiments, performance metrics (RMSE, R2), and model artifacts to Databricks MLflow.
- Inference: Generates predictions for specific city zones.
- Verification: Go to the "Experiments" tab in Databricks to view the logged runs, metrics, and the saved model.
