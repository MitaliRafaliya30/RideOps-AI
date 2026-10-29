"""
Tests for databricks/utilities/safety_features.py

The feature module is imported the same way the Databricks pipeline imports it
(`utilities.safety_features`, from the pipeline root). Trips come from the real
simulator on the 20 x 20 grid city in conftest.py, so each incident type is
checked against the signal it should leave in the features.
"""

import sys
from datetime import datetime
from pathlib import Path
from random import Random

import numpy as np
import pandas as pd
import polyline
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "databricks"))

from utilities.safety_features import (  # noqa: E402
    PING_COLUMNS, WINDOW_COLUMNS, Route, decode_polyline, ping_features, window_features,
)
from simulation.behaviors import BehaviorConfig, BehaviorInjector  # noqa: E402
from simulation.gps import GpsGenerator  # noqa: E402
from simulation.trip_planner import TripPlanner  # noqa: E402

NIGHT = datetime(2026, 5, 6, 4, 0)
QUIET = dict(traffic_stops_per_km=0, dropout_prob=0, innocent_long_stop_prob=0, innocent_detour_prob=0)


def ride(grid_router, calibration, incident=None, noise_m=0.0, seed=3):
    """Simulate one ride and return (pings DataFrame, plan dict, trip)."""
    planner = TripPlanner(grid_router, calibration, traffic_noise=0,
                          injector=BehaviorInjector(grid_router, BehaviorConfig(**QUIET)))
    trip = planner.build_trip(10, 20, NIGHT, Random(seed), incident=incident)
    gps = GpsGenerator(Random(seed), noise_m=noise_m, speed_noise=0)
    plan = gps.route_plan_message(trip, "ride-1", 7)
    pings = pd.DataFrame(gps.pings(trip, "ride-1", 7)).assign(source="test")
    return pings, plan, trip


def features(grid_router, calibration, incident=None, **kw):
    pings, plan, trip = ride(grid_router, calibration, incident, **kw)
    pf = ping_features(pings, plan)
    return pf, window_features(pf, ride_ended=True), trip


# ---------------- geometry ----------------

def test_polyline_decoder_matches_reference_library():
    coords = [(40.7128, -74.006), (40.7138, -74.001), (40.72, -73.99)]
    assert decode_polyline(polyline.encode(coords, 5)) == pytest.approx(coords, abs=1e-5)


def test_distance_to_route_and_progress():
    # A straight east-west street, ~843 m long
    route = Route([(40.75, -73.99), (40.75, -73.98)])
    off, along = route.project([40.75, 40.75 + 100 / 111_320], [-73.985, -73.985])
    assert off[0] == pytest.approx(0, abs=0.5)
    assert off[1] == pytest.approx(100, rel=0.01)                 # 100 m north of the street
    assert along[0] == pytest.approx(route.length / 2, rel=0.01)  # halfway along


# ---------------- normal trip ----------------

def test_normal_trip_stays_on_route_and_finishes(grid_router, calibration):
    pf, wf, _ = features(grid_router, calibration)
    assert list(pf.columns) == PING_COLUMNS and list(wf.columns) == WINDOW_COLUMNS
    assert pf["dist_off_route_m"].max() < 5 and pf["off_route_run_s"].max() == 0
    assert pf["away_from_dropoff_m"].max() < 300                 # closes in on the destination (grid detours aside)
    assert not pf["is_jump"].any() and pf["stop_run_s"].max() == 0
    assert pf["progress_pct"].iloc[-1] == pytest.approx(1.0, abs=0.01)
    assert pf["dist_to_dropoff_m"].iloc[-1] < 5
    assert pf["time_vs_plan"].iloc[-1] == pytest.approx(1.0, abs=0.02)


def test_gps_noise_keeps_normal_trip_close_to_route(grid_router, calibration):
    pf, _, _ = features(grid_router, calibration, noise_m=4.0)
    assert np.percentile(pf["dist_off_route_m"], 95) < 15


def test_windows_cover_the_trip_and_only_the_last_is_final(grid_router, calibration):
    pf, wf, trip = features(grid_router, calibration)
    assert len(wf) == int(trip.duration_s // 60) + 1
    assert wf["is_final_window"].tolist() == [False] * (len(wf) - 1) + [True]
    assert wf["n_pings"].sum() == len(pf)


def test_live_ride_not_ended_has_no_final_window(grid_router, calibration):
    pings, plan, _ = ride(grid_router, calibration)
    wf = window_features(ping_features(pings, plan), ride_ended=False)
    assert not wf["is_final_window"].any()


# ---------------- incidents leave their signal ----------------

def test_route_deviation_moves_far_from_route(grid_router, calibration):
    pf, wf, _ = features(grid_router, calibration, "route_deviation")
    assert pf["dist_off_route_m"].max() > 400
    assert wf["dist_off_route_max_m"].max() > 400
    assert wf["off_route_run_max_s"].max() >= 60                 # stays off route, not a single noisy ping
    assert wf["away_from_dropoff_max_m"].max() > 200             # drives away from the destination


def test_pings_milliseconds_apart_give_no_impossible_speed(grid_router, calibration):
    # Found in EDA: the last ping of 1 % of rides came 0.001-0.3 s after the
    # previous one; 4 m of GPS noise over 0.05 s read as 600+ km/h "spoofing"
    pings, plan, _ = ride(grid_router, calibration, noise_m=4.0)
    extra = pings.iloc[[-1]].assign(
        message_id="late-copy", seq_no=pings["seq_no"].max() + 1, lat=pings["lat"].iloc[-1] + 8 / 111_320,
        ping_ts=(pd.to_datetime(pings["ping_ts"].iloc[-1]) + pd.Timedelta(milliseconds=50)).isoformat())
    pf = ping_features(pd.concat([pings, extra], ignore_index=True), plan)
    assert not pf["is_jump"].any()
    assert np.isnan(pf["implied_speed_kmh"].iloc[-1]) and np.isnan(pf["decel_kmh_per_s"].iloc[-1])


def test_long_stop_builds_a_long_stop_run(grid_router, calibration):
    pf, wf, trip = features(grid_router, calibration, "long_stop")
    stop_s = trip.incidents[0].end_s - trip.incidents[0].start_s
    assert pf["stop_run_s"].max() == pytest.approx(stop_s, abs=20)
    assert wf["stop_run_max_s"].max() >= 230


def test_spoofing_shows_impossible_speed(grid_router, calibration):
    pf, wf, _ = features(grid_router, calibration, "gps_spoofing")
    assert pf["is_jump"].sum() >= 2                       # jump away, and jump back
    assert wf["implied_speed_max_kmh"].max() > 250


def test_wrong_dropoff_ends_far_from_destination(grid_router, calibration):
    _, wf, _ = features(grid_router, calibration, "wrong_dropoff")
    final = wf[wf["is_final_window"]].iloc[0]
    assert final["dist_to_dropoff_last_m"] >= 800
    assert final["progress_pct_last"] < 0.9


def test_crash_is_a_stop_from_speed_that_never_ends(grid_router, calibration):
    _, wf, _ = features(grid_router, calibration, "possible_crash")
    final = wf[wf["is_final_window"]].iloc[0]
    assert final["stop_run_max_s"] >= 400
    assert final["speed_before_stop_kmh"] > 25


# ---------------- robustness ----------------

def test_duplicates_and_arrival_order_do_not_change_features(grid_router, calibration):
    pings, plan, _ = ride(grid_router, calibration, "long_stop")
    messy = pd.concat([pings, pings.sample(10, random_state=1)]).sample(frac=1, random_state=2)
    clean = ping_features(pings, plan).reset_index(drop=True)
    pd.testing.assert_frame_equal(ping_features(messy, plan).reset_index(drop=True), clean)


def test_missing_route_plan_keeps_pings_without_route_features(grid_router, calibration):
    pings, _, _ = ride(grid_router, calibration)
    pf = ping_features(pings, None)
    assert len(pf) == len(pings) and not pf["has_route_plan"].any()
    assert pf["dist_off_route_m"].isna().all() and pf["implied_speed_kmh"].notna().sum() == len(pf) - 1


def test_spark_style_naive_timestamps_give_same_result(grid_router, calibration):
    pings, plan, _ = ride(grid_router, calibration)
    naive = pings.assign(ping_ts=pd.to_datetime(pings["ping_ts"], utc=True).dt.tz_localize(None))
    naive_plan = {**plan, "planned_at": pd.Timestamp(plan["planned_at"]).tz_localize(None)}
    a = ping_features(pings, plan)["time_vs_plan"]
    b = ping_features(naive, naive_plan)["time_vs_plan"]
    assert np.allclose(a, b)
