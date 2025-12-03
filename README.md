# ZTM-Warsaw-Streaming
End-to-end data engineering project that processes real-time GPS data from Warsaw public transport (ZTM).

## Data Source
Warsaw Public Transport Open Data (API):

https://api.um.warszawa.pl/

Real-time vehicle positions (Buses & Trams).

---

## Project Description

This project demonstrates a scalable streaming pipeline built on Azure Cloud. It ingests live GPS data, processes it using Spark Structured Streaming, visualizes in Power BI and creates ML model.

It consists of four main stages:

1. **Ingestion (Apache NiFi & Azure Event Hubs)**
Apache NiFi: Cyclically fetches data from the ZTM API and splits JSON arrays into individual events.
Azure Event Hubs: Acts as a scalable buffer (Kafka protocol) for high-throughput data ingestion.

2. **Processing (Azure Databricks)**
Implemented using PySpark and Structured Streaming with cost-optimized "micro-batch" triggers (availableNow).

- Bronze Layer (Raw):
Ingest streaming data from Event Hubs.
Save raw JSON payloads to ADLS.

- Silver Layer (Cleansed):
Filter out GPS errors (future dates/stale data), null coordinates and duplicates.
Convert data types (Timestamp, Double).

- Gold Layer (Aggregated):
Operational-Stats: Aggregates buses usage per line and hour (Max active vehicles).
GeoHash: Spatial aggregation using Grid/GeoHash logic to map line density.

3. **Data Visualization (Power BI)**
Interactive dashboard connected to Databricks Tables.

Dynamic Time-Lapse: Animation showing bus line density over 24 hours.
![ztm_heatmap-ezgif com-optimize](https://github.com/user-attachments/assets/faee244f-db04-407d-8570-3961d452a170)



Fleet Analysis: Bar chart and line graph showing busiest lines and count of vehicles.
<img width="1425" height="791" alt="peak_bus_lines" src="https://github.com/user-attachments/assets/0d2b08e7-1cdc-4c20-ba50-f24e38441bf7" />


<img width="1423" height="795" alt="active-vehicle-count" src="https://github.com/user-attachments/assets/4c7c3386-2196-46f6-bac8-665aea144de4" />




5. **Machine Learning module**
Machine Learning module to forecast traffic density (bus density).
Proof of Concept (PoC) - It was trained on a 24-hour sample dataset.

<img width="1914" height="841" alt="ml" src="https://github.com/user-attachments/assets/dc231847-f80d-43d7-a365-0f90fb7b86ca" />



Tech Stack & Security

Cloud: Microsoft Azure (ADLS Gen2, Event Hubs, Key Vault).

Compute: Azure Databricks (Spark 3.5).

Security: Credentials managed via Azure Key Vault and Databricks Secret Scopes (no hardcoded keys).

---

Note: Full datasets are not included. The pipeline is designed to run on a schedule or continuously to build historical data.
