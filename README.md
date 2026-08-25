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

### 1. Operational Events — Streaming

A Python-based simulator generates realistic ride lifecycles and streams them to Azure Event Hubs at approximately **9 rides/sec (~50 events/sec)**.

**Ride lifecycle:**

`requested → assigned → accepted → arrived → started → completed`

Cancellation can occur at any pre-completion stage.

Passengers, drivers, vehicles, and zones are drawn from live PostgreSQL master data, ensuring that every foreign key in the event stream is genuinely resolvable.

#### Core Modules

| Module | Responsibility |
|---|---|
| `entity_pool.py` | Loads real entities from PostgreSQL |
| `timing_model.py` | Generates realistic inter-stage delays and trip durations |
| `fare_calculator.py` | Calculates NYC-style fares including base fare, distance, time, surcharges, and tips |
| `ride_event_generator.py` | Assembles complete ride lifecycles |
| `defect_injector.py` | Introduces a configurable share of data defects |
| `event_hub_emitter.py` | Batches and sends events to Azure Event Hubs |

---

### 2. Historical Market Data — Batch

Public **NYC TLC High Volume For-Hire Vehicle (HVFHV)** records are used as a historical market benchmark.

The dataset is scoped to **HV0003 (Uber)** according to **ADR-004**, reducing the original dataset from approximately **22 million rows to 15.35 million rows**.

The data was profiled before ingestion, and four data quality findings were documented and handled using explicit **"flag, don't drop"** decisions rather than silently filtering problematic records.

---

### 3. Master & Reference Data — Batch Snapshot

Nine PostgreSQL master and reference tables are extracted through a parameterized Azure Data Factory pipeline using the following pattern:

**`Get Metadata → ForEach → Copy`**

The pipeline runs through a **Self-Hosted Integration Runtime**.

## 🏗️ The Medallion Architecture

The RideOps data platform follows a **Bronze → Silver → Gold medallion architecture**, separating raw ingestion, data quality and conformance, and analytics-ready consumption.

### 🥉 Bronze — Raw Ingestion

> **Rule:** Land data as-is, validate structure only, quarantine what fails, and attach lineage. **No business logic.**

| Tables | Source | Read Pattern |
|---|---|---|
| `ride_events_raw`, `ride_events_quarantine` | Azure Event Hubs | Structured Streaming (Kafka) |
| `fhvhv_trips_raw` | ADLS Gen2 | Auto Loader with schema evolution |
| `9 × *_raw` reference tables | ADLS Gen2 | Batch read generated from a control list |

A single internal view parses each event once and branches records into **valid** and **quarantined** outputs.

Validation is performed strictly at the **single-row level**:

- Malformed JSON that cannot be parsed against the expected schema
- Null always-required fields: `event_id`, `ride_id`, `event_type`, and `event_timestamp`
- Null stage-required fields, such as `driver_id` on an `assigned` event
- Invalid `event_type` values outside the seven legal lifecycle events
- Out-of-range fare and distance values, including both **negative** and **absurdly high** values

Every row carries ingestion and source lineage through `_ingested_at`, `_source`, Kafka metadata (`_kafka_topic`, `_partition`, `_offset`), or file metadata (`_source_file`, `_file_modified_at`).

---

### 🥈 Silver — Cleansed & Conformed

> **Rule:** Deduplicate, standardize, track history, enforce referential integrity, and reconstruct business entities.**

#### Master Data — SCD Type 2

Master data:

- `sv_passengers`
- `sv_drivers`
- `sv_vehicles`

uses **SCD Type 2** through `create_auto_cdc_from_snapshot_flow`, chosen because PostgreSQL arrives as a **full snapshot rather than a change feed**.

Referential integrity is validated **before** history tracking, ensuring that only resolvable records enter the SCD Type 2 pipeline.

#### Streaming Event Deduplication

Streaming events flow through a **45-minute watermark** and two separate deduplication passes:

```python
.withWatermark("event_timestamp", "45 minutes")
.dropDuplicatesWithinWatermark(["event_id"])
.dropDuplicatesWithinWatermark(["ride_id", "event_type"])
```

The two rules handle different failure modes:

- **Exact duplicate events** with the same `event_id`
- **Retry events with a new ID** but the same `ride_id` and `event_type`

`dropDuplicatesWithinWatermark` is used instead of standard `dropDuplicates` to prevent unbounded state growth.

#### Ride State Reconstruction

Individual lifecycle events are reconstructed into **one row per `ride_id`** using Auto CDC with:

```python
ignore_null_updates=True
```

This preserves milestone values from earlier events when later events only contain newly updated fields.

Each reconstructed ride includes:

- Lifecycle milestone timestamps
- `final_status`
- `cancellation_stage`
- Four duration measures
- `is_stale` flag for rides remaining in-flight for more than two hours

---

### 🥇 Gold — Analytics Star Schema

> **Rule:** Define grain, assign surrogate keys, and shape data for consumption. **Business logic ends here.**

| Type | Tables |
|---|---|
| **Dimensions (8)** | `dim_date`, `dim_zone`, `dim_passenger`, `dim_driver`, `dim_vehicle`, `dim_payment_method`, `dim_ride_status`, `dim_cancellation_reason` |
| **Facts (3)** | `fact_ride`, `fact_ride_event`, `fact_fhvhv_trip` |
| **Analytics Marts (3)** | `agg_zone_hourly`, `agg_driver_daily`, `agg_revenue_daily` |

#### Surrogate Key Strategy

Surrogate keys are sized according to the characteristics of each table.

Stable dimensions that can be fully rebuilt use:

```python
row_number()
```

`fact_fhvhv_trip` uses a **SHA-256 hash of a composite fingerprint** because the table continuously grows as new monthly data arrives. This prevents existing surrogate keys from changing during future rebuilds, which would happen if `row_number()` were recalculated.


**Extracted tables:**

`passengers` · `drivers` · `vehicles` · `vehicle_types` · `membership_tiers` · `payment_methods` · `ride_status` · `cancellation_reasons` · `zone_lookup`

Adding a new table does not require creating a new pipeline. It is handled as a **configuration change rather than a code change**.
