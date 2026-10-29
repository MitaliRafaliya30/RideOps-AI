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


def threshold_curve(windows: pd.DataFrame, score: pd.Series, labels: pd.DataFrame,
                    thresholds: np.ndarray, forced: pd.Series = None) -> pd.DataFrame:
    """
    Recall and false alarms / 1k trips for many alert thresholds in one pass.

    An incident is caught at threshold t when its highest-scoring window is above t;
    a ride has a false alarm when its highest-scoring clean window is above t.
    `forced` marks windows that always alert (deterministic rules in a hybrid).
    Same definitions as evaluate(), without re-running it per threshold.
    """
    s = score.astype(float)
    if forced is not None:
        s = s.where(~forced, np.inf)
    incidents = labels.loc[labels["ride_id"].isin(windows["ride_id"]), "incident_id"]
    inc_max = s[windows["is_incident"]].groupby(windows.loc[windows["is_incident"], "incident_id"]).max()
    inc_max = inc_max.reindex(incidents, fill_value=-np.inf).to_numpy()   # incident without windows: never caught
    clean = ~windows["is_incident"] & ~windows["after_incident"]
    fa_max = s[clean].groupby(windows.loc[clean, "ride_id"]).max().to_numpy()
    n_trips = windows["ride_id"].nunique()

    inc_sorted, fa_sorted = np.sort(inc_max), np.sort(fa_max)
    caught = len(inc_sorted) - np.searchsorted(inc_sorted, thresholds, side="right")
    false_rides = len(fa_sorted) - np.searchsorted(fa_sorted, thresholds, side="right")
    return pd.DataFrame({
        "threshold": thresholds,
        "recall": caught / max(len(inc_sorted), 1),
        "false_alarms_per_1k_trips": 1000 * false_rides / max(n_trips, 1),
    })


def pick_threshold(curve: pd.DataFrame, max_false_alarms_per_1k: float) -> float:
    """Highest recall within the false-alarm budget; among ties, the lowest threshold (earliest alerts)."""
    ok = curve[curve["false_alarms_per_1k_trips"] <= max_false_alarms_per_1k]
    if ok.empty:
        return float(curve["threshold"].max())
    best = ok[ok["recall"] == ok["recall"].max()]
    return float(best["threshold"].min())


def bootstrap_ci(windows: pd.DataFrame, labels: pd.DataFrame, n_boot: int = 1000, seed: int = 0) -> pd.DataFrame:
    """
    95 % confidence intervals for recall, false alarms / 1k and median delay,
    resampling whole RIDES (windows of one ride are not independent).
    With ~100 incidents per test set, a 2-point recall gap is often noise.
    """
    rides = ride_outcomes(windows, labels)
    rng = np.random.default_rng(seed)
    stats = pd.DataFrame([_ride_metrics(rides, rng.integers(0, len(rides), len(rides))) for _ in range(n_boot)])
    point = _ride_metrics(rides, np.arange(len(rides)))
    return pd.DataFrame({"value": point, "ci_low": stats.quantile(0.025), "ci_high": stats.quantile(0.975)})


def paired_bootstrap(windows: pd.DataFrame, alert_a: pd.Series, alert_b: pd.Series, labels: pd.DataFrame,
                     n_boot: int = 1000, seed: int = 0) -> pd.DataFrame:
    """
    Difference B - A of every metric, with a 95 % CI, both detectors scored on
    the SAME resampled rides. Far tighter than comparing two separate CIs:
    a CI of the difference that excludes 0 means B really differs from A.
    """
    a = ride_outcomes(windows.assign(alert=alert_a), labels)
    b = ride_outcomes(windows.assign(alert=alert_b), labels).reindex(a.index)
    rng = np.random.default_rng(seed)
    diffs = []
    for _ in range(n_boot):
        i = rng.integers(0, len(a), len(a))
        diffs.append(_ride_metrics(b, i) - _ride_metrics(a, i))
    diffs = pd.DataFrame(diffs)
    point = _ride_metrics(b, np.arange(len(b))) - _ride_metrics(a, np.arange(len(a)))
    out = pd.DataFrame({"difference": point, "ci_low": diffs.quantile(0.025), "ci_high": diffs.quantile(0.975)})
    out["significant"] = (out["ci_low"] > 0) | (out["ci_high"] < 0)
    return out


def ride_outcomes(windows: pd.DataFrame, labels: pd.DataFrame) -> pd.DataFrame:
    """One row per ride: had an incident, caught it, delay, false alarm (the unit for bootstrapping)."""
    det = detections(windows, labels).set_index("ride_id")
    clean = windows["alert"] & ~windows["is_incident"] & ~windows["after_incident"]
    rides = pd.DataFrame(index=pd.Index(windows["ride_id"].unique(), name="ride_id"))
    rides["incident"] = rides.index.isin(det.index)
    rides["caught"] = rides.index.map(det["detected"]).fillna(False).astype(bool)
    rides["delay_s"] = rides.index.map(det["delay_s"])
    rides["false_alarm"] = rides.index.isin(windows.loc[clean, "ride_id"])
    return rides


def _ride_metrics(rides: pd.DataFrame, i: np.ndarray) -> pd.Series:
    inc, caught = rides["incident"].to_numpy()[i], rides["caught"].to_numpy()[i]
    delay = rides["delay_s"].to_numpy()[i][caught]
    return pd.Series({
        "recall": caught.sum() / max(inc.sum(), 1),
        "false_alarms_per_1k_trips": 1000 * rides["false_alarm"].to_numpy()[i].mean(),
        "median_delay_s": np.median(delay) if len(delay) else np.nan,
    })


def summary_row(name: str, result: dict) -> dict:
    """Flat row for a comparison table (rules vs models)."""
    row = {"detector": name}
    row.update({k: result[k] for k in ("recall", "precision", "false_alarms_per_1k_trips", "median_delay_s")})
    row.update({f"recall_{t}": result["by_type"].loc[t, "recall"] for t in INCIDENT_TYPES})
    return row
