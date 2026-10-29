"""
Model-ready windows: features + ride history + labels + split, for one
simulator run. Training, evaluation and the notebooks all load data here,
so every experiment sees exactly the same rows.

Splits (never train on the future):
  development run (week 1)  train = days 1-5, valid = day 6, test = day 7
  any other run (week 2)    holdout: used once, for the final verdict
"""

import pandas as pd

from ml.config import DEFAULT_RUN, FEATURES_DIR
from ml.labels import label_windows, load_behaviors, load_labels, ride_table
from utilities.safety_features import HISTORY_COLUMNS, history_features

CURRENT_WINDOW_FEATURES = [
    "dist_off_route_max_m", "dist_off_route_mean_m", "off_route_run_max_s", "away_from_dropoff_max_m",
    "stop_run_max_s", "stationary_share", "speed_mean_kmh", "speed_max_kmh", "decel_max_kmh_per_s",
    "implied_speed_max_kmh", "n_jumps", "gap_max_s", "progress_pct_last", "dist_to_dropoff_last_m",
    "time_vs_plan_last", "speed_before_stop_kmh", "is_final_window", "n_pings",
]
# Left out on purpose (M10 EDA): hour_of_day / is_night (no effect on the incident rate),
# outside_nyc_share (always 0), has_route_plan (always true in training data)
MODEL_FEATURES = CURRENT_WINDOW_FEATURES + HISTORY_COLUMNS


def load_windows(run: str = DEFAULT_RUN):
    """(windows, labels, rides) for one run, windows ready for training or scoring."""
    windows = pd.read_parquet(FEATURES_DIR / run / "window_features.parquet")
    labels, behaviors = load_labels(run), load_behaviors(run)
    windows = label_windows(history_features(windows).reset_index(drop=True), labels, behaviors)
    rides = ride_table(labels, windows, behaviors)
    if run != DEFAULT_RUN:
        rides["split"] = "holdout"
    windows["split"] = windows["ride_id"].map(rides.set_index("ride_id")["split"])
    return windows, labels, rides


def matrix(windows: pd.DataFrame, features=None) -> pd.DataFrame:
    """Feature matrix as floats (booleans become 0/1, missing stays NaN for LightGBM)."""
    return windows[features or MODEL_FEATURES].astype(float)
