"""
Tests for ml/labels.py (window targets) and ml/metrics.py (event-level metrics).

Small hand-made rides with known incident times, so every expected label
and every metric can be checked by hand.
"""

import pandas as pd
import pytest

from ml.labels import label_windows, split_of
import numpy as np

from ml.metrics import evaluate, pick_threshold, threshold_curve

T0 = pd.Timestamp("2026-05-06 04:00")


def windows_of(ride_id, n, start=T0):
    """n consecutive 60 s windows of one ride; the last is final."""
    return pd.DataFrame({
        "ride_id": ride_id, "window_index": range(n),
        "window_start": [start + pd.Timedelta(minutes=i) for i in range(n)],
        "window_end": [start + pd.Timedelta(minutes=i + 1) for i in range(n)],
        "is_final_window": [i == n - 1 for i in range(n)],
    })


def at(minutes):
    return T0 + pd.Timedelta(minutes=minutes)


@pytest.fixture
def labels():
    return pd.DataFrame([
        {"incident_id": "i-stop", "ride_id": "stop", "incident_type": "long_stop", "start_ts": at(2.5), "end_ts": at(5.2)},
        {"incident_id": "i-crash", "ride_id": "crash", "incident_type": "possible_crash", "start_ts": at(3.5), "end_ts": pd.NaT},
        {"incident_id": "i-drop", "ride_id": "drop", "incident_type": "wrong_dropoff", "start_ts": at(4), "end_ts": at(4)},
    ])


@pytest.fixture
def windows(labels):
    behaviors = pd.DataFrame([
        {"ride_id": "ok", "kind": "innocent_long_stop", "start_ts": at(1.5), "end_ts": at(3.5)},
        {"ride_id": "ok", "kind": "traffic_stop", "start_ts": at(5), "end_ts": at(5.5)},
    ])
    w = pd.concat([windows_of("stop", 8), windows_of("crash", 6), windows_of("drop", 4), windows_of("ok", 8)],
                  ignore_index=True)
    return label_windows(w, labels, behaviors)


def types(windows, ride):
    return windows.loc[windows["ride_id"] == ride, "incident_type"].tolist()


def test_span_incident_marks_overlapping_windows(windows):
    assert types(windows, "stop") == ["normal"] * 2 + ["long_stop"] * 4 + ["normal"] * 2   # minutes 2..5


def test_windows_after_the_incident_are_flagged(windows):
    stop = windows[windows["ride_id"] == "stop"]
    assert stop["after_incident"].tolist() == [False] * 6 + [True] * 2
    assert not windows.loc[windows["ride_id"] != "stop", "after_incident"].any()


def test_alert_after_the_incident_is_not_a_false_alarm(windows, labels):
    w = windows.assign(alert=(windows["ride_id"] == "stop") & (windows["window_index"] == 7))
    result = evaluate(w, labels)
    assert result["false_alarm_rides"] == 0 and result["by_type"].loc["long_stop", "detected"] == 0


def test_crash_stays_open_until_the_ride_ends(windows):
    assert types(windows, "crash") == ["normal"] * 3 + ["possible_crash"] * 3


def test_wrong_dropoff_only_on_the_final_window(windows):
    assert types(windows, "drop") == ["normal"] * 3 + ["wrong_dropoff"]


def test_hard_negative_is_context_not_a_label(windows):
    ok = windows[windows["ride_id"] == "ok"]
    assert not ok["is_incident"].any()
    assert ok["context"].tolist() == ["none", "innocent_long_stop", "innocent_long_stop", "innocent_long_stop"] + ["none"] * 4


def test_split_is_by_day():
    ts = pd.Series(pd.to_datetime(["2026-05-04 10:00", "2026-05-08 23:59", "2026-05-09 00:00", "2026-05-10 05:00"]))
    assert split_of(ts).tolist() == ["train", "train", "valid", "test"]


def test_metrics_recall_delay_and_false_alarms(windows, labels):
    w = windows.copy()
    w["alert"] = False
    w.loc[(w["ride_id"] == "stop") & (w["window_index"] == 4), "alert"] = True    # long stop caught at minute 5
    w.loc[(w["ride_id"] == "ok") & (w["window_index"] == 2), "alert"] = True      # innocent stop: false alarm
    w.loc[(w["ride_id"] == "ok") & (w["window_index"] == 3), "alert"] = True      # same ride: still one false alarm
    result = evaluate(w, labels)

    assert result["recall"] == pytest.approx(1 / 3)
    assert result["by_type"].loc["long_stop", "median_delay_s"] == pytest.approx(150)   # 2.5 min -> 5 min
    assert result["false_alarm_rides"] == 1
    assert result["false_alarms_per_1k_trips"] == pytest.approx(250)                    # 1 of 4 trips
    assert result["precision"] == pytest.approx(0.5)
    assert pd.isna(result["by_type"].loc["gps_spoofing", "recall"])                     # no such incident


def test_threshold_curve_matches_evaluate_at_every_threshold(windows, labels):
    rng = np.random.default_rng(3)
    score = pd.Series(rng.random(len(windows)), index=windows.index)
    forced = pd.Series(False, index=windows.index)
    forced.iloc[[2, 20]] = True                                    # deterministic rule alerts
    thresholds = np.linspace(0, 1, 21)
    curve = threshold_curve(windows, score, labels, thresholds, forced=forced)
    for t, row in zip(thresholds, curve.itertuples()):
        direct = evaluate(windows.assign(alert=(score > t) | forced), labels)
        assert row.recall == pytest.approx(direct["recall"])
        assert row.false_alarms_per_1k_trips == pytest.approx(direct["false_alarms_per_1k_trips"])


def test_pick_threshold_respects_budget_and_prefers_earlier_alerts():
    curve = pd.DataFrame({"threshold": [0.1, 0.2, 0.3, 0.4], "recall": [1.0, 0.9, 0.9, 0.5],
                          "false_alarms_per_1k_trips": [5.0, 1.0, 0.5, 0.0]})
    assert pick_threshold(curve, 1.0) == 0.2
    assert pick_threshold(curve, 0.0) == 0.4


def test_bootstrap_point_estimate_matches_evaluate_and_brackets_it(windows, labels):
    from ml.metrics import bootstrap_ci
    w = windows.assign(alert=False)
    w.loc[(w["ride_id"] == "stop") & (w["window_index"] == 4), "alert"] = True
    w.loc[(w["ride_id"] == "crash") & (w["window_index"] == 5), "alert"] = True
    w.loc[(w["ride_id"] == "ok") & (w["window_index"] == 2), "alert"] = True
    ci = bootstrap_ci(w, labels, n_boot=300)
    direct = evaluate(w, labels)
    for metric in ("recall", "false_alarms_per_1k_trips", "median_delay_s"):
        assert ci.loc[metric, "value"] == pytest.approx(direct[metric])
        assert ci.loc[metric, "ci_low"] <= ci.loc[metric, "value"] <= ci.loc[metric, "ci_high"]


def test_paired_bootstrap_sees_a_real_difference_and_none_against_itself(windows, labels):
    from ml.metrics import paired_bootstrap
    nothing = pd.Series(False, index=windows.index)
    everything_in_incidents = windows["is_incident"]
    diff = paired_bootstrap(windows, nothing, everything_in_incidents, labels, n_boot=300)
    assert diff.loc["recall", "difference"] == pytest.approx(1.0)
    same = paired_bootstrap(windows, everything_in_incidents, everything_in_incidents, labels, n_boot=100)
    assert (same["difference"].fillna(0) == 0).all() and not same["significant"].any()
