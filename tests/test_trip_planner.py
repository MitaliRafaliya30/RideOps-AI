"""
Tests for simulation/trip_planner.py
Uses the same tiny road graph as test_router.py and hand-made calibration tables.
"""

from collections import Counter
from datetime import datetime
from random import Random

import pytest

from simulation.trip_planner import TripPlanner

RUSH = datetime(2026, 5, 6, 16, 0)     # Wednesday 4 pm
NIGHT = datetime(2026, 5, 6, 4, 0)     # Wednesday 4 am
SATURDAY = datetime(2026, 5, 9, 16, 0)


def test_od_sampling_follows_real_shares(calibration):
    rng = Random(3)
    counts = Counter(calibration.sample_od(rng) for _ in range(10_000))
    assert counts[(10, 20)] / 10_000 == pytest.approx(0.8, abs=0.02)


def test_traffic_factor_by_hour_and_day_type(calibration):
    assert calibration.traffic_factor(RUSH) == 2.8
    assert calibration.traffic_factor(NIGHT) == 1.3
    assert calibration.traffic_factor(SATURDAY) == 2.0


def test_trip_duration_is_free_flow_times_traffic(router, calibration):
    planner = TripPlanner(router, calibration, traffic_noise=0)
    trip = planner.build_trip(10, 20, RUSH, Random(1))
    assert trip.duration_s == pytest.approx(50.0 * 2.8)    # free-flow 50 s on the test graph
    assert trip.end_ts > trip.start_ts


def test_rush_hour_slower_than_night(router, calibration):
    planner = TripPlanner(router, calibration, traffic_noise=0)
    rush = planner.build_trip(10, 20, RUSH, Random(1))
    night = planner.build_trip(10, 20, NIGHT, Random(1))
    assert rush.duration_s > night.duration_s
    assert rush.distance_m == night.distance_m              # same road, different traffic


def test_noise_varies_duration_but_keeps_median(router, calibration):
    planner = TripPlanner(router, calibration, traffic_noise=0.15)
    durations = sorted(planner.build_trip(10, 20, NIGHT, Random(i)).duration_s for i in range(2001))
    assert len(set(durations)) > 100
    assert durations[1000] == pytest.approx(50.0 * 1.3, rel=0.05)
