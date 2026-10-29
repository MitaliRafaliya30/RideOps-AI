"""
Tests for the safety Gold job (databricks/jobs/build_safety_gold.py) on a
local Spark session.

The key check is parity: recall, false alarms per 1k trips, precision and
detection delay computed in Spark for the dashboards must equal what
ml/metrics.py reports for the model card, on real simulator windows. If the
two ever disagree, the dashboard and the model card tell different stories.
"""

import importlib.util
import os
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
JOB_FILE = ROOT / "databricks" / "jobs" / "build_safety_gold.py"


@pytest.fixture(scope="module")
def spark():
    os.environ.pop("SPARK_HOME", None)
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    try:
        from pyspark.sql import SparkSession
        session = (SparkSession.builder.master("local[2]").appName("safety-gold-tests")
                   .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "4")
                   .config("spark.sql.session.timeZone", "UTC")
                   .config("spark.sql.ansi.enabled", "true")      # like Databricks serverless
                   .getOrCreate())
        session.range(1).count()
    except Exception as exc:
        pytest.skip(f"local Spark unavailable: {exc}")
    yield session
    session.stop()


@pytest.fixture(scope="module")
def job():
    spec = importlib.util.spec_from_file_location("build_safety_gold", JOB_FILE)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _scores(windows, alert):
    out = windows[["ride_id", "source", "window_index", "window_start", "window_end", "is_final_window"]].copy()
    out["alert"] = alert.to_numpy()
    out["risk_score"] = np.where(out["alert"], 0.99, 0.01)
    out["model_version"] = "test"
    return out


def _alerts(job, spark, scores_pd, alert_type):
    scores = spark.createDataFrame(scores_pd.assign(alert_type=np.where(scores_pd["alert"], alert_type, None),
                                                    alert_source="model", reasons="test",
                                                    scored_at=pd.Timestamp("2026-05-11")))
    from importlib import util
    spec = util.spec_from_file_location("score_safety", ROOT / "databricks" / "jobs" / "score_safety.py")
    score_job = util.module_from_spec(spec)
    spec.loader.exec_module(score_job)
    return scores, score_job.alerts_from_scores(scores)


def test_dim_incident_type_covers_every_alert_type(spark, job):
    from ml.rules import RULE_ORDER
    dim = job.dim_incident_type(spark).toPandas()
    assert set(dim["alert_type"]) == set(RULE_ORDER)
    assert dim["incident_type_key"].is_unique and dim.iloc[0]["severity"] == "high"


def test_nearest_zone_picks_the_closest_centroid(spark, job):
    plans = spark.createDataFrame(pd.DataFrame({"ride_id": ["a", "b"], "pickup_lat": [40.758, 40.641],
                                                "pickup_lon": [-73.985, -73.778]}))
    geo = spark.createDataFrame(pd.DataFrame({"LocationID": [230, 132, 1], "lat": [40.759, 40.646, 40.69],
                                              "lon": [-73.984, -73.786, -74.17]}))
    dim = spark.createDataFrame(pd.DataFrame({"zone_id": [1, 132, 230], "zone_key": [11, 22, 33]}))
    got = job.nearest_zone(plans, geo, dim).toPandas().set_index("ride_id")["pickup_zone_key"].to_dict()
    assert got == {"a": 33, "b": 22}                       # Times Square, JFK


def test_gold_metrics_equal_model_card_metrics(spark, job):
    """Real windows of the dev test day, scored by the M10 rules: Spark Gold vs ml/metrics.py."""
    from ml.config import DEFAULT_RUN, FEATURES_DIR
    if not (FEATURES_DIR / DEFAULT_RUN).exists():
        pytest.skip("local feature cache missing (python -m ml.build_features)")
    from ml.dataset import load_windows
    from ml.metrics import evaluate
    from ml.rules import apply_rules

    windows, labels, _ = load_windows()
    test = windows[windows["split"] == "test"].reset_index(drop=True)
    test_labels = labels[labels["ride_id"].isin(test["ride_id"])]
    alert = apply_rules(test)["alert"]
    expected = evaluate(test.assign(alert=alert), labels)

    scores, alerts = _alerts(job, spark, _scores(test, alert), "unexpected_stop")
    spark_labels = spark.createDataFrame(test_labels.assign(source=DEFAULT_RUN)[
        ["incident_id", "ride_id", "incident_type", "start_ts", "end_ts", "source"]])
    no_zones = spark.createDataFrame(pd.DataFrame({"ride_id": ["-"], "pickup_zone_key": [0]}))
    rides = job.fact_ride_safety(scores, alerts, spark_labels, no_zones)
    perf = job.ml_model_performance(rides).toPandas()

    total = perf[["rides", "incidents", "caught", "false_alarm_rides", "alerted_rides"]].sum()
    assert total["rides"] == expected["trips"] and total["incidents"] == expected["incidents"]
    assert total["caught"] / total["incidents"] == pytest.approx(expected["recall"])
    assert 1000 * total["false_alarm_rides"] / total["rides"] == pytest.approx(expected["false_alarms_per_1k_trips"])
    assert total["caught"] / total["alerted_rides"] == pytest.approx(expected["precision"])

    ride_pd = rides.toPandas()
    caught_delays = ride_pd.loc[ride_pd["outcome"] == "caught", "detection_delay_s"]
    assert caught_delays.median() == pytest.approx(expected["median_delay_s"], abs=1)
    assert set(ride_pd["outcome"]) <= {"caught", "missed", "false_alarm", "clean"}
    assert (ride_pd["data_role"] == "training data (optimistic)").all()


def test_live_rides_get_rates_but_never_recall(spark, job):
    t0 = pd.Timestamp("2026-10-29 10:00")
    w = pd.DataFrame({"ride_id": ["live-1"] * 3, "source": "live", "window_index": [0, 1, 2],
                      "window_start": [t0 + pd.Timedelta(minutes=i) for i in range(3)],
                      "is_final_window": [False, False, True]})
    w["window_end"] = w["window_start"] + pd.Timedelta(minutes=1)
    scores, alerts = _alerts(job, spark, _scores(w, pd.Series([False, True, True])), "unexpected_stop")
    empty_labels = spark.createDataFrame(pd.DataFrame({
        "incident_id": ["x"], "ride_id": ["sim-only"], "incident_type": ["long_stop"],
        "start_ts": [t0], "end_ts": [t0], "source": ["run_x"]}))
    no_zones = spark.createDataFrame(pd.DataFrame({"ride_id": ["-"], "pickup_zone_key": [0]}))
    rides = job.fact_ride_safety(scores, alerts, empty_labels, no_zones)
    perf = job.ml_model_performance(rides).toPandas().iloc[0]
    assert perf["alerted_rides_per_1k"] == 1000 and not perf["has_ground_truth"]
    assert pd.isna(perf["recall"]) and pd.isna(perf["false_alarms_per_1k"])
    ride = rides.toPandas().iloc[0]
    assert ride["first_alert_type"] == "unexpected_stop" and pd.isna(ride["outcome"]) and ride["data_role"] == "live"

    hourly = job.agg_safety_hourly(rides, alerts).toPandas().iloc[0]
    assert hourly["rides_tracked"] == 1 and hourly["alerts_unexpected_stop"] == 1 and hourly["alerts_gps_spoofing"] == 0


def test_zone_hourly_cancellation_rate_counts_cancelled_rides(spark):
    """M8 fix: the rate was computed after filtering to completed rides, so it was always 0."""
    import types
    sys.modules["pyspark.pipelines"] = types.SimpleNamespace(table=lambda **kw: (lambda fn: fn))
    import pyspark
    pyspark.pipelines = sys.modules["pyspark.pipelines"]
    spec = importlib.util.spec_from_file_location("build_agg_marts", ROOT / "databricks" / "pipelines" / "gold" / "build_agg_marts.py")
    marts = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(marts)

    t = pd.Timestamp("2026-10-29 08:15")
    rides = spark.createDataFrame(pd.DataFrame({
        "pickup_zone_key": [5, 5, 5, 5], "date_key": [20261029] * 4, "requested_at": [t] * 4,
        "final_status": ["completed", "completed", "completed", "cancelled"],
        "wait_time_sec": [100.0, 200.0, 300.0, None], "total_fare": [10.0, 20.0, 30.0, None]}))
    fhvhv = spark.createDataFrame(pd.DataFrame({
        "pickup_zone_key": [5], "date_key": [20261029], "pickup_hour": [8], "wait_time_sec": [60.0], "total_fare": [15.0]}))
    tables = {"rideops_ai.gold.fact_ride": rides, "rideops_ai.gold.fact_fhvhv_trip": fhvhv}
    marts.spark = types.SimpleNamespace(read=types.SimpleNamespace(table=tables.__getitem__))

    row = marts.agg_zone_hourly().toPandas().set_index("source").loc["operational"]
    assert row["cancellation_rate"] == pytest.approx(0.25)
    assert row["trip_count"] == 3 and row["avg_wait_time_sec"] == 200 and row["total_revenue"] == 60
