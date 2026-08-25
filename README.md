RideOps AI

A real-time ride operations platform built on a medallion lakehouse, with a deliberately broken data source.
Three heterogeneous sources feed a governed Databricks lakehouse, surfaced through a five-page Power BI dashboard and a natural-language analytics assistant. Every data quality defect it catches was injected on purpose, and every number it reports is reconciled row-by-row back to source.


![Databricks](https://img.shields.io/badge/Databricks-Lakeflow%20Declarative%20Pipelines-FF3621?logo=databricks&logoColor=white)
![Azure Event Hubs](https://img.shields.io/badge/Azure%20Event%20Hubs-Kafka%20Protocol-0078D4?logo=microsoftazure&logoColor=white)
![Azure Data Factory](https://img.shields.io/badge/Azure%20Data%20Factory-Metadata--Driven-0078D4?logo=microsoftazure&logoColor=white)
![ADLS Gen2](https://img.shields.io/badge/ADLS%20Gen2-Landing%20Zone-0078D4?logo=microsoftazure&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta%20Lake-Unity%20Catalog-00ADD4?logo=delta&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-Structured%20Streaming-E25A1C?logo=apachespark&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-4169E1?logo=postgresql&logoColor=white)
![Python](https://img.shields.io/badge/Python-3.12-3776AB?logo=python&logoColor=white)
![Power BI](https://img.shields.io/badge/Power%20BI-DirectQuery-F2C811?logo=powerbi&logoColor=black)
![LangGraph](https://img.shields.io/badge/LangGraph-ReAct%20Agent-1C3C3C?logo=langchain&logoColor=white)
![Streamlit](https://img.shields.io/badge/Streamlit-Chat%20UI-FF4B4B?logo=streamlit&logoColor=white)
![Data Quality](https://img.shields.io/badge/Data%20Quality-8%20Injected%20Defects-success)
![Reconciliation](https://img.shields.io/badge/Reconciliation-Row%20Level-success)

## Architecture

<p align="center">
  <img 
    src="docs/images/data-and-analytics-architecture.png" 
    alt="RideOps AI — End-to-End Data & Analytics Architecture"
    width="100%"
  />
</p>


## Why this project exists
Most portfolio pipelines move clean data from A to B and call it done. Real pipelines spend most of their life dealing with data that is late, duplicated, malformed, or simply missing.
So this one generates its own bad data on purpose. A Python simulator emits realistic ride lifecycle events with eight distinct defect types injected at configurable rates. The pipeline's job is to catch each one at the layer that can actually detect it, and to prove it caught them with a measured scorecard rather than an assertion.
The result is a platform where you can point at any figure on the dashboard and trace it back to source, and where "data quality" is a number you can query, not a claim in a README.

## 📥 Data Sources

RideOps AI combines three heterogeneous data sources:

### ⚡ Operational Events — Streaming

A Python simulator generates realistic ride lifecycles and streams approximately **50 events/sec** to Azure Event Hubs.

`requested → assigned → accepted → arrived → started → completed`

Cancellation can occur at any pre-completion stage.

Passengers, drivers, vehicles, and zones are sampled from live PostgreSQL master data, ensuring that generated events maintain valid foreign-key relationships.

The simulator includes:

- Realistic lifecycle timing and trip durations
- NYC-style fare calculation
- Configurable data-quality defect injection
- Batched event delivery to Azure Event Hubs

### 📊 Historical Market Data — Batch

Public NYC TLC HVFHV trip records provide a historical market benchmark.

The dataset is scoped to **HV0003 (Uber)**, reducing approximately **22M source records to 15.35M records**. Data quality issues identified during profiling follow an explicit **flag, don't drop** strategy.

### 🗄️ Master & Reference Data — Batch Snapshot

Nine PostgreSQL tables are ingested through a metadata-driven Azure Data Factory pipeline:

`Get Metadata → ForEach → Copy`

The pipeline runs through a Self-Hosted Integration Runtime and is configuration-driven, allowing additional source tables to be added without creating new pipeline logic.

---

## 🏗️ Medallion Architecture

RideOps AI follows a **Bronze → Silver → Gold lakehouse architecture**.

### 🥉 Bronze — Raw Ingestion

Raw streaming, historical, and master data is landed with schema validation, quarantine handling, and full source lineage.

- Event Hubs → Structured Streaming
- ADLS Gen2 → Auto Loader
- PostgreSQL snapshots → Metadata-driven batch ingestion
- Invalid records → Quarantine tables
- Source and ingestion metadata attached to every record

### 🥈 Silver — Cleansed & Conformed

The Silver layer handles data quality, conformance, and entity reconstruction.

- SCD Type 2 history tracking for master data
- Referential integrity validation
- Watermark-based streaming deduplication
- Duplicate detection by both `event_id` and lifecycle stage
- Ride lifecycle reconstruction into one row per `ride_id`
- Auto CDC with `ignore_null_updates=True`
- Duration metrics, cancellation analysis, and stale ride detection

### 🥇 Gold — Analytics Star Schema

The Gold layer provides analytics-ready dimensional models.

| Type | Tables |
|---|---|
| **Dimensions** | 8 |
| **Facts** | 3 |
| **Analytics Marts** | 3 |

Key models include:

`dim_date` · `dim_zone` · `dim_passenger` · `dim_driver` · `dim_vehicle`

`fact_ride` · `fact_ride_event` · `fact_fhvhv_trip`

`agg_zone_hourly` · `agg_driver_daily` · `agg_revenue_daily`

Surrogate key generation is selected based on table characteristics, using rebuild-safe keys for stable dimensions and deterministic SHA-256 fingerprints for continuously growing historical fact data.


