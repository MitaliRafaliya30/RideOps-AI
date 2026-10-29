"""
The deployable safety detector: a hybrid of rules and a model.

  - Deterministic rules (GPS spoofing, wrong drop-off) always alert: they are
    physically unambiguous, caught in under a minute, and easy to defend.
  - A LightGBM model scores every window for the ambiguous cases (unexpected
    stops, route deviations) and alerts above a threshold that was chosen on
    the validation day to stay inside the false-alarm budget.

Every alert carries an incident type and human-readable reason codes, taken
from the model's own SHAP contributions (LightGBM pred_contrib: no extra
library needed when scoring).

Saved as LightGBM's own text format plus a small JSON file, not a Python
pickle: it loads with any LightGBM 4.x and any pandas/scikit-learn version,
which matters when the scoring cluster's libraries differ from the laptop's.
This module imports only pandas, numpy, lightgbm, ml.rules and the shared
feature module, so it ships inside the MLflow model (ml/serving.py).
"""

import json
from dataclasses import dataclass, field
from pathlib import Path

import lightgbm as lgb
import numpy as np
import pandas as pd

from ml.rules import rule_flags
from utilities.safety_features import HISTORY_COLUMNS

CURRENT_WINDOW_FEATURES = [
    "dist_off_route_max_m", "dist_off_route_mean_m", "off_route_run_max_s", "away_from_dropoff_max_m",
    "stop_run_max_s", "stationary_share", "speed_mean_kmh", "speed_max_kmh", "decel_max_kmh_per_s",
    "implied_speed_max_kmh", "n_jumps", "gap_max_s", "progress_pct_last", "dist_to_dropoff_last_m",
    "time_vs_plan_last", "speed_before_stop_kmh", "is_final_window", "n_pings",
]
# Left out on purpose (M10 EDA): hour_of_day / is_night (no effect on the incident rate),
# outside_nyc_share (always 0), has_route_plan (always true in training data)
MODEL_FEATURES = CURRENT_WINDOW_FEATURES + HISTORY_COLUMNS


def matrix(windows: pd.DataFrame, features=None) -> pd.DataFrame:
    """Feature matrix as floats (booleans become 0/1, missing stays NaN for LightGBM)."""
    return windows[features or MODEL_FEATURES].astype(float)

DETERMINISTIC_RULES = ["gps_spoofing", "wrong_dropoff"]

# Which incident a feature points to, for the alert type of a model alert
FEATURE_FAMILY = {
    "stop_run_max_s": "unexpected_stop", "stationary_share": "unexpected_stop", "stop_run_max_s_prev": "unexpected_stop",
    "stationary_share_3w": "unexpected_stop", "speed_mean_kmh": "unexpected_stop", "speed_before_stop_kmh": "unexpected_stop",
    "dist_off_route_max_m": "route_deviation", "dist_off_route_mean_m": "route_deviation",
    "off_route_run_max_s": "route_deviation", "away_from_dropoff_max_m": "route_deviation",
    "dist_off_route_max_m_prev": "route_deviation", "away_from_dropoff_max_m_prev": "route_deviation",
    "off_route_max_so_far_m": "route_deviation", "away_max_so_far_m": "route_deviation",
    "off_route_growth_m": "route_deviation",
    "implied_speed_max_kmh": "gps_spoofing", "n_jumps": "gps_spoofing",
    "dist_to_dropoff_last_m": "wrong_dropoff", "is_final_window": "wrong_dropoff",
}

REASON_TEXT = {
    "stop_run_max_s": "stopped for {:.0f} s", "stationary_share": "standing still {:.0%} of the minute",
    "stop_run_max_s_prev": "already stopped {:.0f} s a minute ago", "stationary_share_3w": "mostly still for 3 minutes ({:.0%})",
    "dist_off_route_max_m": "{:.0f} m from the planned route", "dist_off_route_mean_m": "on average {:.0f} m off route",
    "off_route_run_max_s": "off route for {:.0f} s", "away_from_dropoff_max_m": "{:.0f} m further from the destination",
    "off_route_max_so_far_m": "up to {:.0f} m off route this trip", "away_max_so_far_m": "up to {:.0f} m away from the destination this trip",
    "off_route_growth_m": "off-route distance grew {:.0f} m in a minute",
    "implied_speed_max_kmh": "impossible jump ({:.0f} km/h)", "dist_to_dropoff_last_m": "{:.0f} m from the destination",
    "time_vs_plan_last": "{:.1f}x the planned trip time", "progress_pct_last": "{:.0%} of the route done",
}


@dataclass
class SafetyDetector:
    booster: lgb.Booster                  # trained LightGBM model (best iteration)
    threshold: float
    features: list = field(default_factory=lambda: list(MODEL_FEATURES))
    version: str = "v1"
    metadata: dict = field(default_factory=dict)

    def risk(self, windows: pd.DataFrame) -> np.ndarray:
        """Model risk score (0-1) per window."""
        return self.booster.predict(matrix(windows, self.features))

    def alerts(self, windows: pd.DataFrame, n_reasons: int = 3) -> pd.DataFrame:
        """risk_score, alert, alert_source, alert_type and reasons for every window."""
        out = pd.DataFrame(index=windows.index)
        out["risk_score"] = self.risk(windows)
        rules = rule_flags(windows)[DETERMINISTIC_RULES]
        by_rule = rules.any(axis=1)
        by_model = out["risk_score"] > self.threshold
        out["alert"] = by_rule | by_model
        out["alert_source"] = np.select([by_rule, by_model], ["rule", "model"], None)

        out["alert_type"] = rules.idxmax(axis=1).where(by_rule)
        out["reasons"] = None
        model_only = by_model & ~by_rule
        if model_only.any():
            top = self.top_contributions(windows.loc[model_only], n_reasons)
            out.loc[model_only, "alert_type"] = [FEATURE_FAMILY.get(t[0][0], "unusual_trip") for t in top]
            out.loc[model_only, "reasons"] = ["; ".join(_reason(f, v) for f, v in t) for t in top]
        out.loc[by_rule, "reasons"] = np.where(rules.loc[by_rule, "gps_spoofing"],
                                               "rule: physically impossible GPS jump",
                                               "rule: trip ended far from the destination")
        return out

    def top_contributions(self, windows: pd.DataFrame, n: int = 3):
        """Per window, the n features pushing the risk up the most: [(feature, value), ...]."""
        x = matrix(windows, self.features)
        contrib = self.booster.predict(x, pred_contrib=True)[:, :-1]     # last column = base value
        order = np.argsort(-contrib, axis=1)[:, :n]
        return [[(self.features[j], x.iat[i, j]) for j in row if contrib[i, j] > 0] or [(self.features[row[0]], x.iat[i, row[0]])]
                for i, row in enumerate(order)]

    def save(self, directory: Path):
        """model.txt (LightGBM) + detector.json (threshold, features, version, metadata)."""
        directory = Path(directory)
        directory.mkdir(parents=True, exist_ok=True)
        self.booster.save_model(str(directory / "model.txt"))
        (directory / "detector.json").write_text(json.dumps(
            {"threshold": self.threshold, "features": self.features, "version": self.version,
             "metadata": self.metadata}, indent=2, default=str))

    @staticmethod
    def load(directory: Path) -> "SafetyDetector":
        directory = Path(directory)
        spec = json.loads((directory / "detector.json").read_text())
        return SafetyDetector(booster=lgb.Booster(model_file=str(directory / "model.txt")),
                              threshold=spec["threshold"], features=spec["features"],
                              version=spec["version"], metadata=spec["metadata"])


def _reason(feature: str, value: float) -> str:
    template = REASON_TEXT.get(feature)
    if template is None or pd.isna(value):
        return feature.replace("_", " ")
    return template.format(value)
