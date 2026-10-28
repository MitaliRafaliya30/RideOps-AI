# RideOps AI: Project Brief

**Version:** 2.0 (adds real-time rider safety)  
**Status:** v1 operations platform built · v2 safety layer in progress

---

## 1. Problem

A ride-hailing company faces two problems:

1. **Untrusted data.** Ride data arrives from many sources and is messy: duplicates, late and out-of-order events, missing fields, absurd values, rides that never finish. Managers cannot trust the numbers they act on.
2. **Late safety response.** When a driver takes a strange route, stops for a long time mid-trip, or drops a rider in the wrong place, the company usually learns about it only after a complaint, when it is too late to help.

## 2. Goal

**One platform that delivers trusted operations data and detects unsafe rides while they happen.**

## 3. Users

| User | Question they need answered | Delivered through |
|---|---|---|
| Operations team | How are rides performing? Where do they fail? | Power BI: operations pages |
| Safety team | Which rides look unsafe right now, and why? | Power BI: Safety Control Tower |
| Managers | Can we trust these numbers? How do we compare to the market? | Power BI: data quality + market pages |
| Non-technical staff | Plain-English questions without writing SQL | AI assistant |

## 4. Scope

**In scope**
- Ride lifecycle events and GPS pings streamed through Azure Event Hubs
- Master data from PostgreSQL and NYC TLC trip data, loaded by Azure Data Factory into ADLS Gen2
- Bronze → Silver → Gold lakehouse on Databricks (Lakeflow, Delta, Unity Catalog)
- Data quality: 8 injected event defects, quarantine, scorecard, row-level reconciliation
- Real-time safety detection for 5 incident types (below), with ML models tracked in MLflow
- Power BI dashboards and a natural-language SQL assistant

**Safety incidents detected**

| # | Incident | Signal in GPS |
|---|---|---|
| 1 | Route deviation | Vehicle far from the expected route |
| 2 | Unexpected long stop | Stationary for minutes mid-trip |
| 3 | Wrong or early drop-off | Trip ends far from the destination |
| 4 | Possible crash | Sudden stop from speed, then no movement |
| 5 | GPS spoofing | Physically impossible jump in position |

**Out of scope**
- Phone sensor data (accelerometer, audio)
- Live intervention (contacting riders, emergency services)
- Real production traffic: events and GPS are simulated

## 5. Data Sources

| Source | Type | Ingestion | Volume |
|---|---|---|---|
| Ride event simulator | Streaming | Event Hubs (Kafka protocol) | ~50 events/sec |
| GPS ping simulator | Streaming | Event Hubs | 1 ping / 10 s per active ride |
| PostgreSQL master data | Batch snapshot | ADF + Self-Hosted IR | 9 tables (200k passengers, 20k drivers, 20k vehicles) |
| NYC TLC FHVHV trips | Batch files | ADF → ADLS → Auto Loader | 15.35M Uber trips (May 2026) |
| NYC zones + road network | Reference | One-time load | 265 zones |

## 6. Success Metrics

| Area | Metric | Target |
|---|---|---|
| Data quality | Rows accounted for at every layer (reconciliation) | 100% |
| Data quality | Injected defects detected and counted | 8 of 8 types |
| Safety model | Recall (incidents caught) | ≥ 85% |
| Safety model | False alarms per 1,000 trips | ≤ 5 |
| Safety model | Median detection delay (incident start → alert) | ≤ 60 seconds |
| Platform | End-to-end latency (GPS ping → alert in Gold) | ≤ 2 minutes |
| AI assistant | Correct answers on a 20-question test set | ≥ 18 / 20 |

Targets are set before building and are reported honestly in the results, met or not.

## 7. Assumptions and Limitations

- Ride events and GPS are **simulated** with known ground truth. Detection is verified against that truth, not against real incidents.
- The NYC data covers one month and contains completed trips only (no cancellations, drivers, or riders).
- Operational and NYC data are **never merged**: they share no entities or dates and are compared only by zone and date.
- The pipeline runs in triggered mode to control cost; continuous mode is a configuration change.

## 8. Deliverables

| Layer | Deliverable |
|---|---|
| Data Engineering | Streaming + batch ingestion, medallion lakehouse, data quality scorecard |
| Data Science | Unsupervised and supervised safety models, evaluation report, live scoring |
| Data Analysis | EDA of normal vs unsafe trips, Power BI dashboards (operations, quality, safety) |
| AI | Natural-language assistant over Gold, including safety alerts |
| Documentation | Architecture diagram, results, README, demo |

## 9. Risks

| Risk | Mitigation |
|---|---|
| Streaming scoring takes longer to build than planned | Fallback: micro-batch replay of GPS pings |
| Simulated GPS looks unrealistic | Routes follow the real NYC road network |
| Model overfits the injected patterns | Noisy and borderline incidents, test set split by time |
| Azure / Databricks cost | Develop locally first; run cloud jobs in short triggered runs |
