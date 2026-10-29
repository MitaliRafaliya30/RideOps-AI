"""
Rule-based safety baseline: what a safety team would ship on day one,
before any model. One readable rule per signal on the window features.
Every ML model must beat this on the same metrics (ml/metrics.py), or it
is not worth its complexity.

Four rules, not five: while it is happening, a possible crash looks exactly
like a long stop (a car standing still, mid-trip). Both raise an
"unexpected stop" alert; only later does the ride show which one it was.

Thresholds are tuned on the TRAIN days only (notebooks/10_safety_eda.ipynb).
Conditions that did not change recall or false alarms on train were removed
(time off route, "not at the destination"): a rule should be as simple as
the data allows.
"""

import pandas as pd

DEFAULT_THRESHOLDS = {
    "off_route_m": 800.0,          # route deviation: this far from the planned route
    "stop_s": 300.0,               # unexpected stop: standing still this long, mid-trip
    "jump_kmh": 250.0,             # GPS spoofing: physically impossible implied speed
    "dropoff_miss_m": 500.0,       # wrong drop-off: trip ended this far from the destination
}

# When several rules fire on one window, report the most severe
RULE_ORDER = ["unexpected_stop", "gps_spoofing", "route_deviation", "wrong_dropoff"]


def rule_flags(w: pd.DataFrame, t: dict = None) -> pd.DataFrame:
    """One boolean column per rule."""
    t = {**DEFAULT_THRESHOLDS, **(t or {})}
    return pd.DataFrame({
        "unexpected_stop": w["stop_run_max_s"] > t["stop_s"],
        "gps_spoofing":    w["implied_speed_max_kmh"].fillna(0) > t["jump_kmh"],
        "route_deviation": w["dist_off_route_max_m"].fillna(0) > t["off_route_m"],
        "wrong_dropoff":   w["is_final_window"].astype(bool) & (w["dist_to_dropoff_last_m"].fillna(0) > t["dropoff_miss_m"]),
    }, index=w.index)


def apply_rules(w: pd.DataFrame, t: dict = None) -> pd.DataFrame:
    """Windows plus `alert` (any rule fired) and `alert_type` (most severe rule)."""
    flags = rule_flags(w, t)[RULE_ORDER]
    out = w.copy()
    out["alert"] = flags.any(axis=1)
    out["alert_type"] = flags.idxmax(axis=1).where(out["alert"])
    return out
