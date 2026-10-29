"""
Tests for ml/eda.py and ml/rules.py on synthetic data with known answers.
"""

import numpy as np
import pandas as pd
import pytest

from ml.eda import psi, rate_ci, separation
from ml.rules import apply_rules

RNG = np.random.default_rng(0)


def test_separation_perfect_none_and_reversed():
    low, high = pd.Series(RNG.normal(0, 1, 500)), pd.Series(RNG.normal(10, 1, 500))
    assert separation(high, low)["auc"] == pytest.approx(1.0)
    assert separation(low, high)["cliffs_delta"] == pytest.approx(-1.0)
    same = separation(pd.Series(RNG.normal(0, 1, 2000)), pd.Series(RNG.normal(0, 1, 2000)))
    assert same["auc"] == pytest.approx(0.5, abs=0.03) and same["ks"] < 0.06


def test_separation_ignores_missing_values():
    result = separation(pd.Series([5.0, np.nan, 6.0]), pd.Series([1.0, 2.0, np.nan]))
    assert result["auc"] == 1.0 and result["n_pos"] == 2


def test_psi_stable_vs_shifted():
    base = pd.Series(RNG.normal(0, 1, 20_000))
    assert psi(base, pd.Series(RNG.normal(0, 1, 20_000))) < 0.01
    assert psi(base, pd.Series(RNG.normal(1, 1, 20_000))) > 0.25
    assert psi(pd.Series([0.0] * 100), pd.Series([0.0] * 50)) == 0.0     # constant feature


def test_rate_ci_contains_rate_and_stays_in_bounds():
    p, low, high = rate_ci(20, 1000)
    assert p == 0.02 and 0 < low < p < high < 0.04
    assert rate_ci(0, 50)[1] == pytest.approx(0, abs=1e-12)


def test_rules_fire_on_their_signal_and_pick_most_severe():
    base = dict(dist_off_route_max_m=10.0, off_route_run_max_s=0.0, stop_run_max_s=0.0, speed_before_stop_kmh=np.nan,
                implied_speed_max_kmh=30.0, dist_to_dropoff_last_m=2000.0, is_final_window=False)
    w = pd.DataFrame([
        base,
        {**base, "dist_off_route_max_m": 1500.0, "off_route_run_max_s": 400.0},
        {**base, "stop_run_max_s": 600.0},
        {**base, "implied_speed_max_kmh": 900.0, "stop_run_max_s": 600.0},
        {**base, "is_final_window": True, "dist_to_dropoff_last_m": 3000.0},
        {**base, "is_final_window": True, "dist_to_dropoff_last_m": 20.0},
    ])
    out = apply_rules(w)
    assert out["alert"].tolist() == [False, True, True, True, True, False]
    # stop + jump on one window: the stop is the more severe alert
    assert out["alert_type"].tolist()[1:5] == ["route_deviation", "unexpected_stop", "unexpected_stop", "wrong_dropoff"]


def test_innocent_detour_and_short_stop_are_not_alerts():
    w = pd.DataFrame([
        dict(dist_off_route_max_m=550.0, stop_run_max_s=0.0, implied_speed_max_kmh=30.0,
             dist_to_dropoff_last_m=2000.0, is_final_window=False),          # 200-600 m innocent detour
        dict(dist_off_route_max_m=5.0, stop_run_max_s=230.0, implied_speed_max_kmh=0.0,
             dist_to_dropoff_last_m=900.0, is_final_window=False),           # 2-4 min innocent stop
        dict(dist_off_route_max_m=5.0, stop_run_max_s=0.0, implied_speed_max_kmh=np.nan,
             dist_to_dropoff_last_m=12.0, is_final_window=True),             # normal drop-off
    ])
    assert not apply_rules(w)["alert"].any()
