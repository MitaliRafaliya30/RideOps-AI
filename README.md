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


## Data sources
# 1. Operational events (streaming)
A Python simulator generates realistic ride lifecycles and streams them to Azure Event Hubs at roughly 9 rides/sec (~50 events/sec).
Lifecycle: requested → assigned → accepted → arrived → started → completed, with cancellation possible at any pre-completion stage.
Entity pool: passengers, drivers, vehicles and zones are drawn from the live PostgreSQL master data, so every foreign key in the event stream is genuinely resolvable. Nothing is fabricated.

Modules:

Module	Responsibility
entity_pool.py	Loads real entities from PostgreSQL
timing_model.py	Realistic inter-stage delays and trip durations
fare_calculator.py	NYC-style fare breakdown (base, distance, time, surcharges, tips)
ride_event_generator.py	Assembles clean ride lifecycles
defect_injector.py	Corrupts a configurable share of them
event_hub_emitter.py	Batches and sends to Event Hubs

# 2. Historical market data (batch)
Public NYC TLC High Volume For-Hire Vehicle records, used as a market benchmark. Scoped to HV0003 (Uber) per ADR-004, reducing 22M rows to 15.35M.
Profiled before ingestion. Four data quality findings were documented and resolved as explicit flag, don't drop decisions rather than silent filtering.

#3. Master and reference data (batch snapshot)
Nine PostgreSQL tables extracted through a parameterized ADF Get Metadata → ForEach → Copy pattern over a Self-Hosted Integration Runtime.
passengers · drivers · vehicles · vehicle_types · membership_tiers · payment_methods · ride_status · cancellation_reasons · zone_lookup
Adding a tenth table is a config change, not a code change.
