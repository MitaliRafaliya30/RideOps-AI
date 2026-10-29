"""
Event-level evaluation of a safety detector, in the units a safety
operations team cares about (not window accuracy):

  recall                share of incidents with at least one alert during the incident
  detection delay       seconds from incident start to the first alert on it
                        (an alert fires when its 60 s window closes)
  false alarms / 1k     rides with an alert on a non-incident window, per 1,000 trips:
                        every one costs an agent a call to a rider who is fine
                        (windows after a ride's incident ended are not counted:
                        the same case is already open for that ride)
  precision             share of alerted rides that had a real incident

Input: labelled windows (ml.labels.label_windows) with a boolean `alert` column.
"""

import numpy as np
import pandas as pd

from ml.labels import INCIDENT_TYPES


def detections(windows: pd.DataFrame, labels: pd.DataFrame) -> pd.DataFrame:
    """One row per incident: detected or not, and the detection delay."""
    hits = windows[windows["is_incident"] & windows["alert"]]
    first_alert = hits.groupby("incident_id")["window_end"].min().rename("first_alert")
    out = (labels[labels["ride_id"].isin(windows["ride_id"])][["incident_id", "ride_id", "incident_type", "start_ts"]]
           .join(first_alert, on="incident_id"))
    out["detected"] = out["first_alert"].notna()
    out["delay_s"] = ((out["first_alert"] - out["start_ts"]).dt.total_seconds()).clip(lower=0)
    return out


def evaluate(windows: pd.DataFrame, labels: pd.DataFrame) -> dict:
    """Headline metrics plus a per-incident-type table."""
    det = detections(windows, labels)
    n_trips = windows["ride_id"].nunique()

    false_alarm = windows["alert"] & ~windows["is_incident"] & ~windows["after_incident"]
    false_alarm_rides = windows.loc[false_alarm, "ride_id"].nunique()
    alerted_rides = windows.loc[windows["alert"], "ride_id"].unique()
    detected_rides = det.loc[det["detected"], "ride_id"].unique()

    by_type = (
        det.groupby("incident_type")
        .agg(incidents=("incident_id", "size"), detected=("detected", "sum"),
             median_delay_s=("delay_s", "median"), p90_delay_s=("delay_s", lambda s: s.quantile(0.9)))
        .reindex(INCIDENT_TYPES)
    )
    by_type["recall"] = by_type["detected"] / by_type["incidents"]

    return {
        "trips": n_trips,
        "incidents": len(det),
        "recall": det["detected"].mean() if len(det) else np.nan,
        "median_delay_s": det["delay_s"].median(),
        "false_alarm_rides": false_alarm_rides,
        "false_alarms_per_1k_trips": 1000 * false_alarm_rides / max(n_trips, 1),
        "precision": len(set(detected_rides) & set(alerted_rides)) / max(len(alerted_rides), 1),
        "by_type": by_type,
    }


def summary_row(name: str, result: dict) -> dict:
    """Flat row for a comparison table (rules vs models)."""
    row = {"detector": name}
    row.update({k: result[k] for k in ("recall", "precision", "false_alarms_per_1k_trips", "median_delay_s")})
    row.update({f"recall_{t}": result["by_type"].loc[t, "recall"] for t in INCIDENT_TYPES})
    return row
