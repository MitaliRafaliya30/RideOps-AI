"""
Tests for the GPS Silver transformations (databricks/pipelines/silver/build_gps_silver.py)
on a local Spark session.

Bronze-shaped inputs are built from real simulator rides on the grid city:
the same rides go in as "live" stream messages and as backfill rows, with
duplicates mixed in, so the tests check dedup, the live/backfill union, the
per-ride feature join, and when a live ride counts as ended.
Skipped if a local Spark session cannot start.
"""

import importlib.util
import os
import sys
import types
from datetime import datetime, timedelta
from pathlib import Path
from random import Random

import pandas as pd
import pytest

from simulation.behaviors import BehaviorConfig, BehaviorInjector
from simulation.gps import GpsGenerator
from simulation.trip_planner import TripPlanner

ROOT = Path(__file__).resolve().parents[1]
SILVER_FILE = ROOT / "databricks" / "pipelines" / "silver" / "build_gps_silver.py"
QUIET = dict(traffic_stops_per_km=0, dropout_prob=0, innocent_long_stop_prob=0, innocent_detour_prob=0)
T0 = datetime(2026, 5, 6, 4, 0)


@pytest.fixture(scope="module")
def spark():
    os.environ.pop("SPARK_HOME", None)
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    try:
        from pyspark.sql import SparkSession
        session = (SparkSession.builder.master("local[2]").appName("silver-gps-tests")
                   .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "2")
                   .config("spark.sql.session.timeZone", "UTC")
                   .config("spark.sql.execution.arrow.pyspark.enabled", "true").getOrCreate())
        session.range(1).count()
    except Exception as exc:
        pytest.skip(f"local Spark unavailable: {exc}")
    yield session
    session.stop()


@pytest.fixture(scope="module")
def silver():
    sys.modules["pyspark.pipelines"] = types.SimpleNamespace(
        table=lambda **kw: (lambda fn: fn), view=lambda **kw: (lambda fn: fn), read_stream=lambda name: None)
    import pyspark
    pyspark.pipelines = sys.modules["pyspark.pipelines"]
    sys.path.insert(0, str(ROOT / "databricks"))          # pipeline root: utilities/ is importable
    spec = importlib.util.spec_from_file_location("build_gps_silver", SILVER_FILE)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def messages(grid_router, calibration, ride_id, start, incident=None, seed=1):
    planner = TripPlanner(grid_router, calibration, traffic_noise=0,
                          injector=BehaviorInjector(grid_router, BehaviorConfig(**QUIET)))
    trip = planner.build_trip(10, 20, start, Random(seed), incident=incident)
    gps = GpsGenerator(Random(seed), noise_m=0, speed_noise=0)
    return gps.route_plan_message(trip, ride_id, 7), gps.pings(trip, ride_id, 7)


@pytest.fixture
def bronze(spark, grid_router, calibration):
    """Bronze-shaped DataFrames: live stream (2 rides) and backfill (1 ride), with duplicates."""
    old_plan, old_pings = messages(grid_router, calibration, "live-old", T0, "long_stop")
    new_plan, new_pings = messages(grid_router, calibration, "live-new", T0 + timedelta(hours=2), seed=2)
    sim_plan, sim_pings = messages(grid_router, calibration, "sim-1", T0, "gps_spoofing", seed=3)

    live_rows = [old_plan, new_plan, new_plan] + old_pings + new_pings + new_pings[:5]   # re-delivered copies
    live = spark.createDataFrame(pd.DataFrame(live_rows).assign(_ingested_at=pd.Timestamp("2026-05-06")))
    sim_plans = spark.createDataFrame(pd.DataFrame([sim_plan]).assign(_run_id="run_test"))
    sim = spark.createDataFrame(pd.DataFrame(sim_pings).assign(_run_id="run_test"))
    return {"live": live, "sim_plans": sim_plans, "sim_pings": sim,
            "counts": {"live-old": len(old_pings), "live-new": len(new_pings), "sim-1": len(sim_pings)}}


def test_route_plans_one_per_ride_with_source(silver, bronze):
    plans = silver.build_route_plans(bronze["live"], bronze["sim_plans"]).toPandas()
    assert sorted(plans["ride_id"]) == ["live-new", "live-old", "sim-1"]      # duplicate plan removed
    assert dict(zip(plans["ride_id"], plans["source"])) == {"live-old": "live", "live-new": "live", "sim-1": "run_test"}
    assert plans["planned_at"].notna().all()


def test_pings_deduplicated_and_typed(silver, bronze):
    pings = silver.build_pings(bronze["live"], bronze["sim_pings"]).toPandas()
    assert pings.groupby("ride_id").size().to_dict() == bronze["counts"]     # 5 re-sent copies dropped
    assert str(pings["ping_ts"].dtype).startswith("datetime64")


def test_ping_features_join_each_ride_to_its_plan(silver, bronze):
    plans = silver.build_route_plans(bronze["live"], bronze["sim_plans"])
    pings = silver.build_pings(bronze["live"], bronze["sim_pings"])
    pf = silver.add_ping_features(pings, plans).toPandas()
    assert len(pf) == sum(bronze["counts"].values())
    assert pf["has_route_plan"].all()
    by_ride = pf.groupby("ride_id")
    assert by_ride["stop_run_s"].max()["live-old"] >= 230               # the long stop is visible
    assert by_ride["is_jump"].sum()["sim-1"] >= 2                        # the spoofing is visible
    assert by_ride["dist_off_route_m"].max()["live-new"] < 5             # the normal ride is clean


def test_windows_and_when_a_live_ride_has_ended(silver, bronze):
    plans = silver.build_route_plans(bronze["live"], bronze["sim_plans"])
    pings = silver.build_pings(bronze["live"], bronze["sim_pings"])
    wf = silver.add_window_features(silver.add_ping_features(pings, plans)).toPandas()
    final = wf[wf["is_final_window"]].groupby("ride_id").size().to_dict()
    # live-old finished hours before the newest live ping -> ended; live-new IS the newest -> still open;
    # backfill rides are always complete
    assert final == {"live-old": 1, "sim-1": 1}
    assert set(wf["ride_id"]) == {"live-old", "live-new", "sim-1"}


def test_labels_clean_and_deduplicated(spark, silver):
    raw = spark.createDataFrame(pd.DataFrame([
        {"incident_id": "i1", "ride_id": "r1", "incident_type": "long_stop", "severity": "high",
         "params": "{}", "start_ts": "2026-05-06T04:10:00.000Z", "end_ts": "2026-05-06T04:20:00.000Z", "_run_id": "run_x"},
    ] * 2))
    labels = silver.build_labels(raw).toPandas()
    assert len(labels) == 1 and labels.loc[0, "source"] == "run_x"
    assert labels.loc[0, "start_ts"] == pd.Timestamp("2026-05-06 04:10:00")
