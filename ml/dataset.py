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
from ml.detector import CURRENT_WINDOW_FEATURES, MODEL_FEATURES, matrix  # noqa: F401  (re-exported)
from ml.labels import label_windows, load_behaviors, load_labels, ride_table
from utilities.safety_features import history_features


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
