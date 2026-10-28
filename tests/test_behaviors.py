"""
Tests for simulation/timeline.py and simulation/behaviors.py
Uses the 20 x 20 grid city from conftest.py, at night (traffic factor 1.3).
"""

import json
from collections import Counter
from datetime import datetime, timedelta
from random import Random

import pytest

from geodata.router import haversine_m
from simulation.behaviors import INCIDENT_TYPES, BehaviorConfig, BehaviorInjector
from simulation.gps import GpsGenerator, TripTrack
from simulation.timeline import Behavior, Timeline
from simulation.trip_planner import TripPlanner

NIGHT = datetime(2026, 5, 6, 4, 0)
QUIET = dict(traffic_stops_per_km=0, dropout_prob=0, innocent_long_stop_prob=0, innocent_detour_prob=0)


def planner(grid_router, calibration, **config):
    injector = BehaviorInjector(grid_router, BehaviorConfig(**config))
    return TripPlanner(grid_router, calibration, traffic_noise=0, injector=injector)


def trip_with(grid_router, calibration, incident, seed=3, **config):
    cfg = {**QUIET, **config}
    return planner(grid_router, calibration, **cfg).build_trip(10, 20, NIGHT, Random(seed), incident=incident)


def pings_of(trip, seed=1):
    return GpsGenerator(Random(seed), noise_m=0, speed_noise=0).pings(trip, "ride", 7)


def ping_seconds(trip, ping):
    return (datetime.fromisoformat(ping["ping_ts"][:-1]) - trip.start_ts).total_seconds()


# ---------------- timeline edits ----------------

def test_pause_shifts_later_times_and_labels():
    tl = Timeline([(0.0, 0.0), (0.0, 0.01)], [0.0, 100.0])
    later = Behavior("x", False, 60.0, 80.0)
    tl.behaviors.append(later)
    tl.insert_pause(50.0, 30.0)
    assert tl.duration_s == 130.0
    assert (later.start_s, later.end_s) == (90.0, 110.0)
    assert tl.position_at(65.0) == tl.position_at(50.0)          # standing still


def test_truncate_drops_behaviours_after_the_end():
    tl = Timeline([(0.0, 0.0), (0.0, 0.01)], [0.0, 100.0])
    tl.behaviors += [Behavior("before", False, 10.0, 70.0), Behavior("after", False, 60.0, 90.0)]
    tl.truncate(50.0)
    assert tl.duration_s == 50.0
    assert [(b.kind, b.end_s) for b in tl.behaviors] == [("before", 50.0)]


# ---------------- normal behaviour ----------------

def test_plain_trip_follows_the_plan(grid_router, calibration):
    trip = trip_with(grid_router, calibration, None)
    assert trip.incidents == [] and trip.track_coords == trip.route.coords


def test_traffic_stops_keep_calibrated_duration(grid_router, calibration):
    plain = trip_with(grid_router, calibration, None)
    stops = trip_with(grid_router, calibration, None, traffic_stops_per_km=2.0)
    assert stops.duration_s == pytest.approx(plain.duration_s)
    assert any(b.kind == "traffic_stop" for b in stops.behaviors)
    assert stops.incidents == []


def test_dropout_leaves_a_gap_in_pings(grid_router, calibration):
    trip = trip_with(grid_router, calibration, None, dropout_prob=1.0)
    seconds = [ping_seconds(trip, p) for p in pings_of(trip)]
    assert max(b - a for a, b in zip(seconds, seconds[1:])) >= 20
    assert trip.incidents == []                                  # a dropout is not an incident


def test_hard_negatives_are_not_incidents(grid_router, calibration):
    trip = trip_with(grid_router, calibration, None, innocent_long_stop_prob=1.0, innocent_detour_prob=1.0)
    kinds = {b.kind for b in trip.behaviors}
    assert {"innocent_long_stop", "innocent_detour"} <= kinds
    assert trip.incidents == []


# ---------------- incidents ----------------

def test_route_deviation_leaves_the_plan(grid_router, calibration):
    trip = trip_with(grid_router, calibration, "route_deviation")
    [incident] = trip.incidents
    assert incident.kind == "route_deviation" and incident.params["detour_m"] >= 1000
    farthest = max(min(haversine_m(p, q) for q in trip.route.coords) for p in trip.track_coords)
    assert farthest >= 500                                       # really off the planned route
    assert trip.track_coords[-1] == trip.route.coords[-1]        # still reaches the dropoff


def test_long_stop_shows_minutes_without_movement(grid_router, calibration):
    trip = trip_with(grid_router, calibration, "long_stop")
    [incident] = trip.incidents
    track = TripTrack(trip)
    assert incident.end_s - incident.start_s >= 240
    assert track.at(incident.start_s + 60).speed_kmh == 0.0


def test_crash_stops_from_speed_and_never_moves_again(grid_router, calibration):
    trip = trip_with(grid_router, calibration, "possible_crash")
    [incident] = trip.incidents
    track = TripTrack(trip)
    assert incident.params["speed_before_kmh"] > 25
    assert track.at(incident.start_s - 5).speed_kmh > 25
    assert all(track.at(t).speed_kmh == 0.0 for t in range(int(incident.start_s) + 1, int(trip.duration_s), 30))


def test_wrong_dropoff_ends_short_of_destination(grid_router, calibration):
    trip = trip_with(grid_router, calibration, "wrong_dropoff")
    [incident] = trip.incidents
    assert haversine_m(trip.track_coords[-1], trip.route.coords[-1]) >= 800
    assert incident.start_s == pytest.approx(trip.duration_s)


def test_spoofing_makes_reported_position_jump(grid_router, calibration):
    trip = trip_with(grid_router, calibration, "gps_spoofing")
    [incident] = trip.incidents
    track = TripTrack(trip)
    jumps = [
        haversine_m((p["lat"], p["lon"]), (track.at(ping_seconds(trip, p)).lat, track.at(ping_seconds(trip, p)).lon))
        for p in pings_of(trip)
        if incident.start_s <= ping_seconds(trip, p) < incident.end_s
    ]
    assert jumps and min(jumps) >= 1900


def test_incident_labels_match_contract(grid_router, calibration):
    trip = trip_with(grid_router, calibration, "long_stop")
    [label] = GpsGenerator(Random(1)).incident_labels(trip, "ride-9")
    assert set(label) == {"incident_id", "ride_id", "incident_type", "start_ts", "end_ts", "severity", "params"}
    assert label["incident_type"] == "long_stop" and label["start_ts"].endswith("Z")
    assert json.loads(label["params"])["stop_s"] >= 240
    expected_start = trip.start_ts + timedelta(seconds=trip.incidents[0].start_s)
    assert label["start_ts"].startswith(expected_start.isoformat()[:19])


@pytest.mark.parametrize("incident", INCIDENT_TYPES)
def test_every_incident_type_can_be_injected(grid_router, calibration, incident):
    trip = trip_with(grid_router, calibration, incident, **{**QUIET, "traffic_stops_per_km": 0.6})
    assert [b.kind for b in trip.incidents] == [incident]


def test_incident_rate_is_about_two_percent(grid_router):
    injector = BehaviorInjector(grid_router)
    rng = Random(0)
    counts = Counter(injector.choose_incident(rng) for _ in range(50_000))
    incidents = sum(v for k, v in counts.items() if k is not None)
    assert incidents / 50_000 == pytest.approx(BehaviorConfig().total_incident_rate, abs=0.003)
