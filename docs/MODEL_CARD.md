# Model Card: Rider-Safety Detector (M11)

| | |
|---|---|
| **Name** | `rideops_ai.ml.rideops_safety_detector@champion` (Unity Catalog); locally `rideops_safety_detector@champion` |
| **Type** | Hybrid: deterministic rules + LightGBM binary classifier |
| **Task** | Flag a trip in progress that needs a safety check, every 60 seconds, from GPS only |
| **Code** | `ml/detector.py` (detector), `ml/train.py` (training), `notebooks/11_safety_models.ipynb` (analysis) |
| **Status** | Packaged with `python -m ml.serving`; scored on Databricks by `databricks/jobs/score_safety.py` (M12) |

---

## What it does

Every minute of every trip in progress, the detector reads that minute's GPS features (from the Silver table
`sv_ride_live_features`) plus the trip's history, and returns:

| Output | Meaning |
|---|---|
| `risk_score` | LightGBM probability that an incident is under way (0–1) |
| `alert` | true when a deterministic rule fires **or** the risk is above the threshold (0.986) |
| `alert_type` | `unexpected_stop`, `route_deviation`, `gps_spoofing`, `wrong_dropoff` |
| `reasons` | Top contributing signals in plain English (from SHAP), e.g. *"stopped for 290 s; standing still 100 % of the minute"* |

| Part | Handles | Why this part |
|---|---|---|
| Rules | GPS spoofing (implied speed > 250 km/h), wrong drop-off (trip ended > 500 m short) | Physically unambiguous, caught in under a minute; too rare for a model to learn well |
| LightGBM | Unexpected stops (long stop, possible crash), route deviations | Ambiguous cases where combining signals (duration, distance, direction, history) beats any single threshold |

**Intended use:** prioritise trips for a human safety agent to check (call the rider, offer help).
**Not for:** automatic action against a driver, or ranking drivers. An alert is a reason to look, not a verdict.

## Data

| | |
|---|---|
| Source | Simulator v2, calibrated on 15.35 M real NYC FHVHV trips (roads, demand, speeds, fares) |
| Training | Week 1 (`run_2026-05-04`, seed 42), days 1–5: 27,391 trips, 618,689 windows (4,323 incident windows) |
| Validation | Week 1 day 6: early stopping, model selection, threshold |
| Test | Week 1 day 7: development check |
| **Holdout** | Week 2 (`run_2026-05-11`, seed 7): 41,578 trips, 845 incidents, never used in development |
| Label | A 60 s window overlapping an injected incident = positive (0.7 % of windows); windows after an incident ended are left out |
| Features | 27: 18 from the current minute + 9 ride-history features (previous minute, worst so far, trend), all from `databricks/utilities/safety_features.py`, the same code the Silver pipeline runs |

## Training

- LightGBM, 9-point grid (`scale_pos_weight` ∈ {1, 5, 20} × `num_leaves` ∈ {15, 31, 63}), early stopping on
  validation PR-AUC. Chosen: `scale_pos_weight = 20`, `num_leaves = 15`, 507 trees, validation PR-AUC 0.820.
- **Threshold:** on the validation day, the highest recall within the rule baseline's own false-alarm budget
  (1.15 per 1,000 trips), so the comparison is at equal operational cost.
- Tracked in MLflow (`python -m ml.train`; browse with `mlflow ui --backend-store-uri sqlite:///mlflow.db`).

## Results (holdout week)

95 % bootstrap intervals, resampling whole trips:

| Detector | Recall | False alarms / 1k trips | Median delay |
|---|---|---|---|
| Rules (M10 baseline) | 93.3 % [91.6–94.8] | 1.54 [1.18–1.90] | 326 s [321–331] |
| Isolation Forest (unsupervised) | 30.8 % [27.8–33.8] | 1.66 [1.25–2.04] | 383 s |
| LightGBM alone | 88.2 % [86.1–90.3] | 0.99 [0.70–1.30] | 288 s |
| **Hybrid** | **94.0 % [92.3–95.4]** | **0.99 [0.70–1.30]** | **281 s [271–289]** |

**Paired test, hybrid minus rules** (same resampled trips):

| Metric | Difference | 95 % CI | Significant |
|---|---|---|---|
| Recall | +0.7 pts | −1.0 to +2.3 | no (same) |
| False alarms / 1k trips | −0.55 (−36 %) | −0.96 to −0.12 | **yes** |
| Median detection delay | −45 s | −53 to −40 | **yes** |

By incident type (hybrid): long stop 94.6 %, possible crash 100 %, route deviation 84.4 %, GPS spoofing 99.4 %,
wrong drop-off 100 %.

## Explainability

SHAP (LightGBM `pred_contrib`, no extra library needed at scoring time). Top signals: length of the current stop,
distance from the planned route, distance gone away from the destination, growth of the off-route distance, time vs
plan. Ride-history features carry about 24 % of the total contribution.

## Known limits

| Limit | Effect |
|---|---|
| **Simulated GPS and incidents** | No rain, tunnels, phone-battery or app behaviour. Real-world precision would be lower; the threshold must be re-tuned on real outcomes |
| GPS-only blind spot | The 51 missed incidents (6 %) look exactly like harmless behaviour: deviations staying ~900 m from the route (innocent detours reach that too) and stops of 4–4.5 min (innocent stops reach 320 s). Closing this needs other signals (in-app SOS, rider check-in) |
| Remaining false alarms | All from innocent detours / re-routes (27) and innocent long stops (14) on the holdout week |
| Small event counts | 845 holdout incidents; per-type numbers (e.g. 101 crashes) carry wide intervals |
| Possible crash vs long stop | Indistinguishable while happening; both raise `unexpected_stop`, the type becomes clear only later |

## Fairness and responsible use

- The model sees trip geometry and timing only: no rider or driver identity, demographics or zone, so it cannot score
  people directly. Zone-level false-alarm rates should still be monitored in Gold (M8), because a borough with more
  re-routing (road works, bridges) could get more alerts.
- Alerts go to a human; reasons are shown with every alert so the agent can judge it.

## Monitoring (planned, M8/M9)

Daily in Gold `ml_model_performance`: alert rate, false alarms per 1,000 trips (from agent outcomes), detection delay,
feature drift (PSI of the top features against the training week). Retrain or re-tune the threshold when PSI > 0.25
or the false-alarm rate leaves its band.
