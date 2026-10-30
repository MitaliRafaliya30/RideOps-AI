# Power BI Dashboard (M13)

`RideOps Dashboard/RideOps Dashboard v2.pbip`: a PBIP project (TMDL semantic model + PBIR report, plain
text in git) on the Databricks SQL warehouse.

## Pages

| Page | Question it answers | Main tables |
|---|---|---|
| Executive Overview, Ride Lifecycle, Driver Performance, Two-World Comparison | Operations (v1) | `fact_ride`, `fact_ride_event`, `agg_*`, `fact_fhvhv_trip` |
| Data Quality & Pipeline Health | Did the ride-event stream's defects get caught? Bronze contract violations vs Silver reference checks | `dq_defect_scorecard`, `dq_all_quarantine` |
| **Rider Safety Overview** | How many trips are alerted, of which type, where, and how fast? | `fact_ride_safety`, `fact_safety_alert` |
| **Alert Queue** | What should the safety team act on now, and why? (reasons + recommended action) | `fact_safety_alert`, `dim_incident_type` |
| **Risk by Zone & Hour** | When and where do alerts concentrate? | `agg_safety_hourly`, `dim_zone` |
| **Model Performance** | Is the detector good? Recall, precision, false alarms, delay, per day and incident type | `fact_ride_safety`, `ml_model_performance` |
| **Safety Data Quality** | Did the last pipeline run pass its checks? History of every check | `dq_check_results` |
| **Ride Timeline** | What did one ride look like minute by minute, and why did it alert? | `safety_window_scores` |

## Model design

- **Composite model.** The v1 operational facts stay **DirectQuery** (always live). The seven safety tables
  are **Import**: small, refreshed after each pipeline run, and fast. Each safety page sends 10+ queries at once,
  which timed out against the small serverless SQL warehouse in DirectQuery. The shared `dim_date` and
  `dim_zone` are **Dual**, so they serve both.
- **Relationships:** `dim_date` and `dim_zone` -> `fact_ride_safety` -> `fact_safety_alert` and
  `safety_window_scores` (one row per ride on the one side); `dim_incident_type` -> `fact_safety_alert`;
  `dim_date` / `dim_zone` -> `agg_safety_hourly`; `dim_date` -> `ml_model_performance`.
- **Measures** (display folders Safety, Model, Data Quality) use the same definitions as `ml/metrics.py`:
  recall = caught / incidents, false alarms per 1k = rides with an alert outside an incident per 1,000 trips.
  Recall is only computed where ground truth exists; live rides never get one.
- **Honesty in the report:** the training week is labelled `training data (optimistic)`; Model Performance
  points to the holdout-week numbers in `docs/MODEL_CARD.md`.

## Refresh

After the Databricks chain (pipeline -> `score_safety` -> `build_safety_gold` -> `run_dq_checks`):
Power BI **Home > Refresh** (Import tables). DirectQuery pages need no refresh. Credentials: Databricks
personal access token.
