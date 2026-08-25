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

Contents
Why this project exists
Architecture
Data sources
The medallion layers
Data quality framework
Consumption
Reconciliation
Engineering challenges
Design decisions
Repository structure
Running it
Known limitations
Tech stack
Why this project exists

Most portfolio pipelines move clean data from A to B and call it done. Real pipelines spend most of their life dealing with data that is late, duplicated, malformed, or simply missing.

So this one generates its own bad data on purpose. A Python simulator emits realistic ride lifecycle events with eight distinct defect types injected at configurable rates. The pipeline's job is to catch each one at the layer that can actually detect it, and to prove it caught them with a measured scorecard rather than an assertion.

The result is a platform where you can point at any figure on the dashboard and trace it back to source, and where "data quality" is a number you can query, not a claim in a README.

Architecture

Show Image

Three ingestion paths converge on one lakehouse:

Streaming — simulated ride events through Azure Event Hubs via the Kafka protocol
Batch files — 15M+ historical NYC trip records landed to ADLS Gen2 by Azure Data Factory
Batch snapshot — PostgreSQL master and reference data, extracted by a parameterized ADF pipeline

All three land in Bronze, are cleansed and conformed in Silver, and are modelled into a star schema in Gold. Both consumers, the dashboard and the AI assistant, read Gold only.

Data sources
1. Operational events (streaming)

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
2. Historical market data (batch)

Public NYC TLC High Volume For-Hire Vehicle records, used as a market benchmark. Scoped to HV0003 (Uber) per ADR-004, reducing 22M rows to 15.35M.

Profiled before ingestion. Four data quality findings were documented and resolved as explicit flag, don't drop decisions rather than silent filtering.

3. Master and reference data (batch snapshot)

Nine PostgreSQL tables extracted through a parameterized ADF Get Metadata → ForEach → Copy pattern over a Self-Hosted Integration Runtime.

passengers · drivers · vehicles · vehicle_types · membership_tiers · payment_methods · ride_status · cancellation_reasons · zone_lookup

Adding a tenth table is a config change, not a code change.

The medallion layers
Bronze — raw ingestion

Rule: land it as-is, validate structure only, quarantine what fails, attach lineage. No business logic.

Tables	Source	Read pattern
ride_events_raw, ride_events_quarantine	Event Hubs	Structured Streaming (Kafka)
fhvhv_trips_raw	ADLS Gen2	Auto Loader, schema evolution enabled
9 × *_raw reference tables	ADLS Gen2	Batch read, generated from a control list

A single internal view parses each event once, then branches into valid and quarantined outputs. Validation is strictly single-row:

Malformed JSON (unparseable against schema)
Null always-required fields (event_id, ride_id, event_type, event_timestamp)
Null stage-required fields (e.g. driver_id on an assigned event)
Invalid event_type outside the seven legal values
Out-of-range fare and distance values, both negative and absurdly high

Every row carries lineage: _ingested_at, _source, plus _kafka_topic/_partition/_offset for streams or _source_file/_file_modified_at for files.

Silver — cleansed and conformed

Rule: deduplicate, standardise, track history, enforce referential integrity, reconstruct entities.

Master data (sv_passengers, sv_drivers, sv_vehicles) uses SCD Type 2 via create_auto_cdc_from_snapshot_flow, chosen because PostgreSQL arrives as a full snapshot rather than a change feed. Referential integrity is validated before history tracking, so only resolvable rows enter the SCD2 chain.

Streaming events flow through a 45-minute watermark and two distinct deduplication passes:

python
.withWatermark("event_timestamp", "45 minutes")
.dropDuplicatesWithinWatermark(["event_id"])              # defect 3
.dropDuplicatesWithinWatermark(["ride_id", "event_type"])  # defect 4

Two rules, not one, because an exact duplicate and a retry-with-new-id are different failure modes. dropDuplicatesWithinWatermark is used rather than plain dropDuplicates, which retains unbounded state.

Ride state reconstruction collapses a 5–7 event lifecycle into one row per ride_id using Auto CDC with ignore_null_updates=True. That flag is load-bearing: a started event only knows started_at, and without it each new event would blank the milestones earlier events had already set.

Derived per ride: milestone timestamps, final_status, cancellation_stage, four duration measures, and is_stale (in-flight beyond two hours).

Gold — star schema

Rule: decide grain, assign surrogate keys, shape for consumption. Business logic ends here.

Type	Tables
Dimensions (8)	dim_date, dim_zone, dim_passenger, dim_driver, dim_vehicle, dim_payment_method, dim_ride_status, dim_cancellation_reason
Facts (3)	fact_ride, fact_ride_event, fact_fhvhv_trip
Marts (3)	agg_zone_hourly, agg_driver_daily, agg_revenue_daily

Surrogate keys are sized to the table. Stable dimensions that fully rebuild use row_number(). fact_fhvhv_trip uses a SHA-256 hash of a composite fingerprint, because it grows as new months land and row_number() would renumber every existing key on rebuild.

Data quality framework

Eight defect types, injected at configurable rates, each caught at the layer that can actually detect it.

#	Defect	Detected at	Why there
1	Out-of-order events	Silver	Needs comparison against arrival order
2	Late-arriving events	Silver	Needs event-time vs processing-time
3	Exact duplicates	Silver	Needs cross-row comparison
4	Near-duplicates	Silver	Needs cross-row comparison
5	Null required fields	Bronze	Detectable from one row alone
6	Invalid event_type	Bronze	Detectable from one row alone
7	Out-of-range values	Bronze	Detectable from one row alone
8	Incomplete rides	Silver	Needs the passage of time

The split is the whole point. Bronze catches what a single row reveals; Silver catches what only context reveals. Getting this boundary right is what keeps Bronze fast and Silver meaningful.

Two quarantine tables, two purposes
Table	Catches
bronze.ride_events_quarantine	Schema and contract violations (defects 5, 6, 7)
silver.sv_quarantine	Cross-table referential integrity failures

They are deliberately not merged, because a malformed record and an unresolvable foreign key are different problems requiring different investigation.

The scorecard

silver.dq_defect_scorecard reports one row per defect type with a measured count, making detection provable rather than asserted.

sql
SELECT * FROM rideops_ai.silver.dq_defect_scorecard ORDER BY defect_id;
defect_id	defect_name	layer	count
1	out_of_order_events	Bronze/Silver	801
3	exact_duplicates	Bronze	250
4	near_duplicates	Bronze	341
5	null_required_fields	Bronze	186
6	invalid_event_type	Bronze	137
7	out_of_range_values	Bronze	353
8	incomplete_rides	Silver	427
Consumption
Power BI dashboard

Five pages over DirectQuery, so figures reflect live pipeline state rather than a stale import. 25 measures, 25 relationships, persistent page navigation, cross-filtering slicers.

Page	Answers
Executive Overview	How is the operation performing right now?
Ride Lifecycle & Cancellations	Where in the funnel do rides fail, and why?
Driver Performance	Who is performing well, and on what volume?
Two-World Comparison	How does our demand compare to the NYC market?
Data Quality & Pipeline Health	Are the numbers trustworthy?

Aggregate marts back the summary visuals; the large fact tables are reserved for detail views, keeping DirectQuery responsive.

A finding the dashboard surfaced: 70% of cancellations occur at the arrived stage, after the driver has already reached the pickup point. That is the most expensive point in the lifecycle to lose a trip, and it is invisible without reconstructing ride state from raw events.

AI assistant

A natural-language interface over the same Gold layer. Ask a question in plain English, get an answer plus the SQL that produced it.

Q: What is the cancellation rate?
A: The cancellation rate is 14.66%.
   Out of 22,108 total rides, 3,240 were cancelled.

   [View SQL]
   SELECT COUNT(CASE WHEN final_status = 'cancelled' THEN 1 END) AS cancelled,
          COUNT(*) AS total,
          ROUND(100.0 * COUNT(CASE WHEN final_status = 'cancelled' THEN 1 END)
                / COUNT(*), 2) AS rate_pct
   FROM fact_ride

Governance is layered, not assumed. Schema scoping means the agent never sees Bronze or Silver. Execution-time guardrails mean it cannot reach them even with a fully-qualified query. This distinction was verified empirically — schema scoping alone was demonstrated insufficient.

Four guardrail layers: read-only enforcement, table whitelist, row limits, and comment-aware keyword scanning.

Deliberately lightweight. No vector store, no RAG, no multi-agent orchestration. The schema is 14 tables and fits comfortably in context; retrieval infrastructure would add complexity without adding capability at this size.

Reconciliation

Every row is accounted for at each layer boundary, with a documented reason for every transition. This is re-run after every load.

Simulator emits                              3,081 events
    │
    ├─ 39 quarantined at Bronze (defects 5, 6, 7)
    ▼
ride_events_raw                              3,042 events
    │
    ├─ 24 removed by Silver dedup (defects 3, 4)
    ▼
sv_ride_events                               3,018 events
    │
    ▼
sv_ride_state                                  531 rides
    ├─ completed                                413
    ├─ cancelled                                  75
    └─ in_flight                                  43
    │
    ▼
fact_ride  531 rides   ·   fact_ride_event  3,018 events

Losses are never assumed benign. Each one maps to a specific, intentional rule.

Engineering challenges

Five production-grade defects found and fixed, each traced to root cause with SQL evidence before and after. Three of them were silent: the pipeline reported success while producing wrong data.

1. Every completed ride silently quarantined

Symptom: fact_ride showed zero completed rides. Rides accumulated indefinitely at started, one sitting open for 5.5 days against a lifecycle designed to finish in ~8 minutes.

Investigation: Traced through the simulator, ruled out an initially plausible randint() bounds bug by checking the arithmetic (randint(0, 0) is valid Python, not an error). Pulled the raw JSON directly from Event Hubs Data Explorer and compared it against the Bronze schema.

Root cause: _create_event() wrapped every fare field in str() before serialization. Against a DoubleType schema, Spark's from_json rejected the entire record into _corrupt_record. Since fare fields are only populated on completed events, exactly one event type was affected, and it was affected 100% of the time.

Fix: float() instead of str().

2. A dictionary lookup returning labels instead of keys

Symptom: 1,234 records in Bronze quarantine tagged malformed_json, all cancelled events.

Root cause: CANCELLATION_REASONS.get(4, 4) returns the dict value ("Payment Failed") rather than the key (4). Against an IntegerType column, that's the same failure mode as bug #1, in a different field. Only the hardcoded no-show branch was affected; the other cancellation paths used .keys() correctly.

Fix: cancellation_reason_id = 4.

3. Revenue inflated 26x by an unbounded validation rule

Symptom: Avg Fare reported $960.73 on a dataset where the fare formula caps realistic trips around $40.

Investigation: Rather than accept a plausible-looking number, queried the distribution directly. 84 rides out of 8,926 accounted for $8.4M of an $8.7M total, 96% of revenue from 0.9% of rides.

Root cause: Bronze's out-of-range check tested only for negative values. The defect injector produces both negative and absurdly high (99999.99) corruption via a coin flip. Half the defect had never been caught, and had been silently inflating revenue since the pipeline was built.

Fix: Added upper bounds to the validation rule. Avg Fare corrected to $37.18.

4. A defect that fired but did nothing

Root cause: The injector's nullable_fields map nulled payment_method_id on requested events, a field Bronze's contract never checks. Defect 5 was firing at its configured rate and producing zero quarantined records for that stage.

Fix: Aligned the injector's field list with Bronze's actual STAGE_REQUIRED_FIELDS contract.

5. DirectQuery query-folding failure

Symptom: Two Power BI cards showed a generic error under any filter, while three others worked fine.

Root cause: COUNTROWS(fact_ride) cannot be translated to SQL by the Databricks connector once a filter crosses a relationship. The error surfaced only after clicking through to the underlying detail: "We couldn't fold the expression to the data source."

Fix: CALCULATE(COUNT(fact_ride[ride_id])). A bare row count doesn't fold; a named-column aggregate does. Verified against the exact failing filter scenario rather than an unfiltered one.

Design decisions
ID	Decision	Reasoning
ADR-001	Bronze catches only single-row defects	Cross-row detection needs state Bronze doesn't have
ADR-002	Two worlds never merged	Operational and historical share no entities and no time range. A union would fabricate a relationship that doesn't exist
ADR-003	Flag, don't drop	Soft data quality signals become boolean columns. Only hard contract violations are quarantined
ADR-004	HV0003 only, filtered in Silver	Business scope, not data quality. Bronze keeps all 22M rows so scope can widen without re-ingesting
ADR-005	SCD2 in Silver, current-only in Gold	History is preserved where it's cheap; the presentation layer stays simple
ADR-006	45-minute watermark	Sized against the defect injector's own worst-case hold-back window
ADR-007	No dbt	Value comes from portability and team handoff boundaries, neither of which applies to a single-platform, single-developer project
ADR-008	Minimal orchestration	Lakeflow resolves Bronze→Silver→Gold dependencies natively. Scheduling was added only where a genuine gap existed
Repository structure
rideops-ai/
├── database/
│   ├── generators/
│   │   ├── entity_pool.py           # Real entities from PostgreSQL
│   │   ├── timing_model.py          # Realistic delays and durations
│   │   ├── fare_calculator.py       # NYC-style fare breakdown
│   │   ├── ride_event_generator.py  # Clean lifecycle generation
│   │   ├── defect_injector.py       # 8 injected defect types
│   │   └── event_hub_emitter.py     # Azure Event Hubs sink
│   ├── ddl/                         # PostgreSQL schema
│   └── seed/                        # Master data seeding
├── config/
│   └── simulator_config.yaml        # Rates, defect probabilities, duration
├── transformations/
│   ├── bronze/
│   │   ├── read_eventhub.py         # Streaming ingest + quarantine split
│   │   ├── read_adls_historical.py  # Auto Loader for FHVHV
│   │   └── read_postgres_reference.py
│   ├── silver/
│   │   ├── build_reference_tables.py
│   │   ├── build_master_scd2.py
│   │   ├── build_fhvhv_trips.py
│   │   ├── build_ride_events.py
│   │   ├── build_ride_state.py
│   │   └── build_dq_observability.py
│   └── gold/
│       ├── build_dim_date_zone.py
│       ├── build_dim_entities.py
│       ├── build_dim_small_refs.py
│       ├── build_fact_ride.py
│       ├── build_fact_ride_event.py
│       ├── build_fact_fhvhv_trip.py
│       └── build_agg_marts.py
├── simulator.py                     # Entry point
└── docs/

rideops-assistant/
├── src/
│   ├── config.py                    # Env + Gold table whitelist
│   ├── connection.py                # SQLAlchemy → Databricks, Gold-scoped
│   ├── agent.py                     # LangGraph ReAct agent
│   ├── guardrails.py                # Read-only, whitelist, row limit
│   └── prompts.py                   # Domain context + demo questions
├── app.py                           # Streamlit chat UI
└── requirements.txt
Running it
Prerequisites
Azure subscription (Event Hubs, Data Factory, ADLS Gen2)
Databricks workspace with Unity Catalog
PostgreSQL instance
Python 3.10+
1. Seed the source database
bash
psql -f database/ddl/schema.sql
python database/seed/seed_master_data.py
2. Configure the simulator

Copy .env.example to .env and fill in:

POSTGRES_HOST=localhost
POSTGRES_DB=rideops_ai_db
EVENT_HUBS_CONNECTION_STRING=Endpoint=sb://...
EVENT_HUBS_HUB_NAME=ride-events
3. Run the pipeline

Create a Lakeflow Declarative Pipeline pointed at transformations/, target catalog rideops_ai. Set pipeline configuration:

RideOps-Eventhub-Namespace = <your-namespace>
RideOps-Eventhub           = ride-events
connection_string          = <event-hubs-connection-string>

Start the pipeline in Continuous mode.

4. Generate data
bash
python simulator.py --duration=1200
5. Refresh the scorecard

dq_defect_scorecard is a batch table downstream of streaming sources and does not reliably re-trigger. Full-refresh it after each load, or schedule it via Databricks Workflows.

6. Run the assistant
bash
cd rideops-assistant
pip install -r requirements.txt
streamlit run app.py
Known limitations

Documented rather than hidden.

dq_defect_scorecard needs a manual refresh. As a batch table downstream of streaming sources, it does not reliably re-trigger in Continuous pipeline mode.
Assume Referential Integrity drops NULL-key rows. Enabled on Power BI relationships for DirectQuery performance. Rides whose requested event was quarantined have a NULL date_key and are excluded from date-sliced visuals. Deliberate trade-off, small and quantified.
Non-overlapping time ranges between worlds. Operational and historical data cover different periods, so cross-world comparison is volumetric and geographic, not temporal. Surfaced explicitly in the dashboard rather than papered over.
Metric definitions are explicit. Cancellation Rate divides by all rides including in-flight; Completion Rate (Settled) divides only by concluded rides. Both are exposed so the distinction is visible.
The simulator stands in for a production event source. In a real system these events come from the ride-hailing application itself.
Tech stack
Layer	Technology
Generation	Python, PostgreSQL, azure-eventhub
Ingestion	Azure Event Hubs (Kafka protocol), Azure Data Factory, ADLS Gen2, Self-Hosted Integration Runtime
Processing	Databricks, Lakeflow Declarative Pipelines, PySpark Structured Streaming, Auto Loader, Auto CDC, Delta Lake
Governance	Unity Catalog, Databricks Secret Scopes, SAS tokens, scoped PATs
Modelling	Medallion architecture, star schema, SCD Type 2
Consumption	Power BI (DirectQuery, DAX), LangChain, LangGraph, Streamlit
What I'd do next
Airflow or Databricks Workflows for the batch ingestion paths, closing the scorecard refresh gap properly
Overlapping time ranges between operational and historical data, making the two-world comparison temporal as well as geographic
Point-in-time joins against the SCD2 history already retained in Silver, currently unused by Gold
A semantic layer so the assistant uses canonical metric definitions rather than deriving them per query
Content
taxi_zone_lookup.csv

CSV

final_Data_Profiling_Report (1).ipynb

IPYNB

01_generate_reference_data.py

241 lines

PY

02_generate_drivers.py

300 lines

PY

03_generate_passengers.py

321 lines

PY

04_generate_vehicles.py

322 lines

PY

.venv\Lib\site-packages\psycopg\cursor.py:117: UndefinedTable __________________________ ERROR at setup of TestEntityPoolLoading.test_payment_methods_loaded __________________________ postgres_conn = <database.utils.database.PostgresConnection object at 0x000002071A48F750> @pytest.fixture

PASTED

(RideOps-AI) PS C:\Users\win-11\Desktop\Data Engineering\RideOps-AI> pytest tests/test_config_loader.py::TestScenarios::test_scenario_clean -v -s ================================================== test session starts ================================================== platform win32 -- Python 3.11.

PASTED

ride_event_generator.py

381 lines

PY

test_generator.py

292 lines

PY

(RideOps-AI) PS C:\Users\win-11\Desktop\Data Engineering\RideOps-AI> pytest tests/test_generator.py -v -s =============================================== test session starts =============================================== platform win32 -- Python 3.11.15, pytest-9.1.1, pluggy-1.6.0 -- C:\Users\win

PASTED

test_generator.py

307 lines

PY

# ============================================================================ # SILVER LAYER: Master Data with SCD Type 2 History # ---------------------------------------------------------------------------- # Covers sv_drivers, sv_vehicles, sv_passengers, and sv_quarantine. # # Why SCD2, and

PASTED

defect_injector.py

367 lines

PY

entity_pool.py

379 lines

PY

event_hub_emitter.py

126 lines

PY

ride_event_generator.py

382 lines

PY

fare_calculator.py

153 lines

PY

timing_model.py

137 lines

PY

main.py

7 lines

PY

simulator.py

228 lines

PY

read_adls_historical.py

189 lines

PY

read_eventhub.py

242 lines

PY

read_postgres_reference.py

97 lines

PY

build_dq_observability.py

125 lines

PY

build_fhvhv_trips.py

162 lines

PY

build_master_scd2.py

242 lines

PY

build_reference_tables.py

102 lines

PY

build_ride_events.py

216 lines

PY

build_ride_state.py

143 lines

PY

build_fact_ride_event.py

94 lines

PY

build_fact_ride.py

87 lines

PY

build_fact_fhvhv_trip.py

74 lines

PY

build_dim_small_refs.py

55 lines

PY

build_dim_entities.py

118 lines

PY

build_dim_date_zone.py

78 lines

PY

build_agg_marts.py

143 lines

PY

simulator_config.yaml

211 lines

YAML

(.venv) PS C:\Users\win-11\Desktop\Data Engineering\rideops-assistant> python test_agent.py Q: How many rides are there in total? ================================ Human Message ================================= How many rides are there in total? C:\Users\win-11\Desktop\Data Engineering\rideo

PASTED
