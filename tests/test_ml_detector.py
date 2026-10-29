"""
Tests for ml/detector.py: the hybrid detector's alerts, types, reasons and
persistence, on synthetic windows where only a long stop means danger.
"""

import lightgbm as lgb
import numpy as np
import pandas as pd
import pytest

from ml.dataset import MODEL_FEATURES, matrix
from ml.detector import SafetyDetector


@pytest.fixture(scope="module")
def data():
    rng = np.random.default_rng(0)
    n = 4000
    w = pd.DataFrame({f: rng.random(n) * 10 for f in MODEL_FEATURES})
    w["is_final_window"] = False
    w["implied_speed_max_kmh"] = rng.random(n) * 60
    w["dist_off_route_max_m"] = rng.random(n) * 15
    w["dist_to_dropoff_last_m"] = 1000.0
    w["stop_run_max_s"] = rng.exponential(40, n)
    w["is_incident"] = w["stop_run_max_s"] > 200
    return w


@pytest.fixture(scope="module")
def detector(data):
    model = lgb.LGBMClassifier(n_estimators=60, num_leaves=7, verbose=-1, random_state=0)
    model.fit(matrix(data), data["is_incident"].astype(int))
    return SafetyDetector(booster=model.booster_, threshold=0.5)


def test_model_alerts_on_the_dangerous_signal_with_reasons(detector, data):
    out = detector.alerts(data)
    assert (out["alert"] == data["is_incident"]).mean() > 0.98
    model_alerts = out[out["alert_source"] == "model"]
    assert (model_alerts["alert_type"] == "unexpected_stop").all()
    assert model_alerts["reasons"].str.startswith("stopped for").all()
    assert out.loc[~out["alert"], "alert_type"].isna().all()


def test_deterministic_rules_alert_even_when_the_model_is_calm(detector, data):
    calm = data[~data["is_incident"]].head(2).copy()
    calm.iloc[0, calm.columns.get_loc("implied_speed_max_kmh")] = 900.0          # GPS jump
    calm.iloc[1, calm.columns.get_loc("is_final_window")] = True                  # ended 1 km short
    out = detector.alerts(calm)
    assert out["alert"].all() and (out["alert_source"] == "rule").all()
    assert out["alert_type"].tolist() == ["gps_spoofing", "wrong_dropoff"]


def test_save_and_load_give_identical_scores(detector, data, tmp_path):
    path = tmp_path / "detector"
    detector.save(path)
    loaded = SafetyDetector.load(path)
    assert np.allclose(loaded.risk(data), detector.risk(data))
    assert loaded.threshold == detector.threshold
