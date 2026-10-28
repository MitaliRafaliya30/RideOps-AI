"""
Tests for the v2 path of RideEventGenerator (trip_planner attached).
No PostgreSQL needed: a fake entity pool stands in for master data.
"""

from datetime import datetime, timedelta
from random import Random

import pytest

from database.generators.ride_event_generator import RideEventGenerator
from simulation.trip_planner import TripPlanner

START = datetime(2026, 5, 6, 16, 0)    # weekday rush hour


class FakePool:
    """Minimal stand-in for EntityPool."""

    def __init__(self, rng: Random):
        self.rng = rng

    def pick_passenger(self):
        return {"id": self.rng.randint(1, 1000)}

    def pick_driver(self):
        return {"id": self.rng.randint(1, 100)}

    def vehicle_for_driver(self, driver_id):
        return {"id": driver_id}

    def pick_zone(self):
        return 999       # would be wrong in v2: zones must come from the planner

    def pick_payment_method(self):
        return 1


def make_generator(router, calibration, seed=1, completion_rate=1.0):
    rng = Random(seed)
    planner = TripPlanner(router, calibration, traffic_noise=0)
    config = {"generation": {"completion_rate": completion_rate}}
    return RideEventGenerator(FakePool(rng), config, rng, trip_planner=planner)


def test_completed_ride_uses_real_route(router, calibration):
    events, trip = make_generator(router, calibration).generate_ride_with_trip(START)

    assert trip is not None
    assert [e["event_type"] for e in events] == ["requested", "assigned", "accepted", "arrived", "started", "completed"]
    completed = events[-1]
    assert completed["trip_miles"] == pytest.approx(trip.distance_miles, abs=0.01)
    assert (completed["pickup_location_id"], completed["dropoff_location_id"]) in {(10, 20), (30, 20)}


def test_trip_starts_and_ends_with_the_ride(router, calibration):
    events, trip = make_generator(router, calibration).generate_ride_with_trip(START)
    started = datetime.fromisoformat(events[-2]["event_timestamp"])
    completed = datetime.fromisoformat(events[-1]["event_timestamp"])

    assert trip.start_ts == started
    assert completed - started == timedelta(seconds=round(trip.duration_s))


def test_cancelled_ride_has_no_trip(router, calibration):
    events, trip = make_generator(router, calibration, completion_rate=0.0).generate_ride_with_trip(START)
    assert events[-1]["event_type"] == "cancelled"
    assert trip is None


def test_generate_ride_still_returns_only_events(router, calibration):
    events = make_generator(router, calibration).generate_ride(START)
    assert isinstance(events, list) and events[0]["event_type"] == "requested"
