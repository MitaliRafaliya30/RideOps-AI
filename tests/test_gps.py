"""
Tests for simulation/gps.py
Uses the tiny road graph and calibration from conftest.py (rush hour, factor 2.8).
"""

import statistics
from datetime import datetime, timedelta, timezone
from random import Random

import polyline
import pytest

from geodata.router import haversine_m
from simulation.gps import GpsGenerator, TripTrack, utc_iso
from simulation.trip_planner import TripPlanner

RUSH = datetime(2026, 5, 6, 16, 0)
RIDE_ID, DRIVER_ID = "ride-1", 42


@pytest.fixture
def trip(router, calibration):
    # Free-flow 50 s x rush factor 2.8 = 140 s; A->B ends at 30 x 2.8 = 84 s
    return TripPlanner(router, calibration, traffic_noise=0).build_trip(10, 20, RUSH, Random(1))


def exact_gps(seed=1):
    return GpsGenerator(Random(seed), noise_m=0, speed_noise=0)


def test_pings_every_10s_plus_final_at_dropoff(trip):
    pings = exact_gps().pings(trip, RIDE_ID, DRIVER_ID)
    assert [p["seq_no"] for p in pings] == list(range(1, 16))     # 0, 10, ..., 130, 140 s
    assert pings[0]["ping_ts"] == utc_iso(RUSH)
    assert pings[-1]["ping_ts"] == utc_iso(RUSH + timedelta(seconds=140))


def test_first_and_last_ping_at_pickup_and_dropoff(trip):
    pings = exact_gps().pings(trip, RIDE_ID, DRIVER_ID)
    coords = trip.route.coords
    assert (pings[0]["lat"], pings[0]["lon"]) == coords[0]
    assert (pings[-1]["lat"], pings[-1]["lon"]) == coords[-1]


def test_position_follows_route_timing(trip):
    fix = TripTrack(trip).at(84.0)                                  # end of the A->B road
    assert (round(fix.lat, 6), round(fix.lon, 6)) == trip.route.coords[1]


def test_speed_and_heading_on_first_road(trip):
    a, b = trip.route.coords[0], trip.route.coords[1]
    fix = TripTrack(trip).at(40.0)
    assert fix.speed_kmh == pytest.approx(haversine_m(a, b) / 84.0 * 3.6)
    assert fix.heading_deg == pytest.approx(90.0, abs=0.5)          # A->B runs due east


def test_ping_matches_contract(trip):
    ping = exact_gps().pings(trip, RIDE_ID, DRIVER_ID)[3]
    assert set(ping) == {
        "message_type", "schema_version", "message_id", "ride_id", "driver_id", "seq_no",
        "ping_ts", "lat", "lon", "speed_kmh", "heading_deg", "accuracy_m",
    }
    assert ping["message_type"] == "gps_ping" and ping["ping_ts"].endswith("Z")
    assert ping["speed_kmh"] >= 0 and 0 <= ping["heading_deg"] < 360


def test_route_plan_message(router, calibration):
    trip = TripPlanner(router, calibration, traffic_noise=0.3).build_trip(10, 20, RUSH, Random(5))
    plan = exact_gps().route_plan_message(trip, RIDE_ID, DRIVER_ID)
    assert plan["message_type"] == "route_plan"
    assert polyline.decode(plan["route_polyline"], 5) == trip.route.coords
    assert plan["planned_duration_s"] == 140                         # typical traffic, not this trip's luck
    assert round(trip.duration_s) != 140                             # the actual trip differs


def test_gps_error_is_a_few_meters(trip):
    gps, track = GpsGenerator(Random(3), noise_m=4.0), TripTrack(trip)
    truth = track.at(60.0)
    errors = [
        haversine_m((truth.lat, truth.lon), (p["lat"], p["lon"]))
        for p in (gps.ping_message(trip, RIDE_ID, DRIVER_ID, 1, truth) for _ in range(2000))
    ]
    assert 3.0 < statistics.median(errors) < 7.0                     # Rayleigh median ~ 1.18 x 4 m


def test_same_seed_same_messages(trip):
    assert GpsGenerator(Random(9)).pings(trip, RIDE_ID, DRIVER_ID) == GpsGenerator(Random(9)).pings(trip, RIDE_ID, DRIVER_ID)


def test_utc_iso_converts_aware_timestamps():
    ny = timezone(timedelta(hours=-4))
    assert utc_iso(datetime(2026, 5, 6, 12, 0, tzinfo=ny)) == "2026-05-06T16:00:00.000Z"
