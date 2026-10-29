"""
Tests for the data quality job (databricks/jobs/run_dq_checks.py) on a local
Spark session in ANSI mode (like Databricks serverless): thresholds, the
Spark PSI against ml/eda.py, and that reconciliation catches lost rows.
"""

import importlib.util
import os
import sys
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
JOB_FILE = ROOT / "databricks" / "jobs" / "run_dq_checks.py"
NOW = datetime(2026, 5, 11, 12, 0)


@pytest.fixture(scope="module")
def spark():
    os.environ.pop("SPARK_HOME", None)
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    try:
        from pyspark.sql import SparkSession
        session = (SparkSession.builder.master("local[2]").appName("dq-tests")
                   .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "2")
                   .config("spark.sql.session.timeZone", "UTC").config("spark.sql.ansi.enabled", "true")
                   .getOrCreate())
        session.range(1).count()
    except Exception as exc:
        pytest.skip(f"local Spark unavailable: {exc}")
    yield session
    session.stop()


@pytest.fixture(scope="module")
def dq():
    spec = importlib.util.spec_from_file_location("run_dq_checks", JOB_FILE)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_status_from_thresholds(dq):
    assert dq.check("c", "x", "t", 0.0, warn=0.0, fail=0.1)["status"] == "pass"
    assert dq.check("c", "x", "t", 0.05, warn=0.0, fail=0.1)["status"] == "warn"
    assert dq.check("c", "x", "t", 0.5, warn=0.0, fail=0.1)["status"] == "fail"
    assert dq.check("c", "x", "t", 99, warn=60)["status"] == "warn"                 # warn-only check
    assert dq.check("c", "x", "t", None, skip_reason="too few")["status"] == "skipped"


def test_spark_psi_agrees_with_eda_psi(spark, dq):
    from ml.eda import psi as pandas_psi
    rng = np.random.default_rng(0)
    ref, same, shifted = rng.normal(0, 1, 20_000), rng.normal(0, 1, 20_000), rng.normal(1, 1, 20_000)
    df = lambda v: spark.createDataFrame(pd.DataFrame({"x": v}))
    assert dq.psi(df(ref), df(same), "x") == pytest.approx(pandas_psi(pd.Series(ref), pd.Series(same)), abs=0.01)
    assert dq.psi(df(ref), df(shifted), "x") == pytest.approx(pandas_psi(pd.Series(ref), pd.Series(shifted)), rel=0.05)


@pytest.fixture
def tables(spark):
    t0 = pd.Timestamp("2026-05-04 08:00")
    sim = pd.DataFrame({"message_id": [f"s{i}" for i in range(100)],
                        "ping_ts": [(t0 + pd.Timedelta(seconds=10 * i)).isoformat() for i in range(100)]})
    sim = pd.concat([sim, sim.head(5)])                                   # 5 duplicates
    live = pd.DataFrame({"message_id": ["l1", "l2", "p1"], "message_type": ["gps_ping", "gps_ping", "route_plan"],
                         "ping_ts": ["2026-05-11T11:50:00Z", "2026-05-11T11:50:10Z", None]})
    silver = pd.DataFrame({"message_id": [f"s{i}" for i in range(100)] + ["l1", "l2"],
                           "source": ["run_a"] * 100 + ["live"] * 2,
                           "ping_ts": [t0 + pd.Timedelta(seconds=10 * i) for i in range(100)]
                           + [pd.Timestamp("2026-05-11 11:50:00"), pd.Timestamp("2026-05-11 11:50:10")],
                           "is_jump": [False] * 101 + [True], "is_outside_nyc": False, "is_gap": False,
                           "has_route_plan": True})
    rng = np.random.default_rng(1)
    features = pd.DataFrame({"ride_id": [f"r{i % 300}" for i in range(3000)],
                             "source": ["run_2026-05-04_7d_50000r_seed42"] * 3000,
                             **{c: rng.random(3000) for c in ["dist_off_route_max_m", "stop_run_max_s", "speed_mean_kmh",
                                                              "time_vs_plan_last", "progress_pct_last"]}})
    live_features = features.head(40).assign(source="live", ride_id="live-1")
    features = pd.concat([features, live_features])
    scores = features[["ride_id"]].copy()
    rides = pd.DataFrame({"ride_id": [f"r{i}" for i in range(300)] + ["live-1"],
                          "source": ["run_2026-05-04_7d_50000r_seed42"] * 300 + ["live"],
                          "has_alert": [i < 6 for i in range(300)] + [False]})
    labels = pd.DataFrame({"ride_id": ["r1", "r2"]})
    quarantine = pd.DataFrame({"message_id": ["bad"]})
    sdf = spark.createDataFrame
    return {"live_messages": sdf(live), "sim_pings": sdf(sim), "silver_pings": sdf(silver), "features": sdf(features),
            "scores": sdf(scores), "ride_safety": sdf(rides), "labels": sdf(labels), "quarantine": sdf(quarantine)}


def by_name(rows):
    return {(r["check_name"], r["source"]): r for r in rows}


def test_all_checks_on_a_healthy_dataset(spark, dq, tables):
    rows = by_name(dq.run_all(tables, NOW))
    assert rows[("gps_pings_bronze_to_silver", None)]["status"] == "pass"
    assert "5 duplicates" in rows[("gps_pings_bronze_to_silver", None)]["details"]
    assert rows[("every_window_scored", None)]["status"] == "pass"
    assert rows[("gold_rides_equal_scored_rides", None)]["status"] == "pass"
    assert rows[("labelled_rides_without_gps", None)]["status"] == "pass"
    assert rows[("live_gps_freshness_min", "live")]["value"] == pytest.approx(9 + 50 / 60)
    assert rows[("alerted_rides_per_1k", "run_2026-05-04_7d_50000r_seed42")]["status"] == "pass"   # 20 per 1k
    assert rows[("alerted_rides_per_1k", "live")]["status"] == "skipped"                           # 1 ride
    assert rows[("drift_psi_stop_run_max_s", "live")]["status"] == "skipped"                        # 40 windows
    assert rows[("gps_jump_share", None)]["status"] == "warn"                                       # 1 of 102 = 0.98 %
    # every row fits the results table
    assert len(dq.to_dataframe(spark, list(rows.values()), "run-1", NOW).collect()) == len(rows)


def test_reconciliation_catches_a_lost_ping_and_unscored_windows(spark, dq, tables):
    tables["silver_pings"] = tables["silver_pings"].where("message_id != 's7'")
    tables["scores"] = tables["scores"].limit(100)
    rows = by_name(dq.run_all(tables, NOW))
    lost = rows[("gps_pings_bronze_to_silver", None)]
    assert lost["status"] == "fail" and "lost 1" in lost["details"]
    assert rows[("every_window_scored", None)]["status"] == "fail"


def test_drift_is_measured_when_there_is_enough_data(spark, dq, tables):
    features = tables["features"].toPandas()
    shifted = features[features["source"] != "live"].head(2000).assign(source="run_week2")
    shifted["stop_run_max_s"] = shifted["stop_run_max_s"] + 1.0                       # whole distribution moved
    tables["features"] = spark.createDataFrame(pd.concat([features, shifted]))
    rows = by_name(dq.drift_checks(tables["features"]))
    assert rows[("drift_psi_stop_run_max_s", "run_week2")]["status"] == "fail"
    assert rows[("drift_psi_speed_mean_kmh", "run_week2")]["status"] == "pass"
