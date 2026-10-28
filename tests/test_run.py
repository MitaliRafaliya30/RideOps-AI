"""
Tests for simulation/rides.py and simulation/run.py (backfill and live modes).
Uses the grid city and a fake entity pool; live mode runs on a fake clock.
"""

import json
from collections import defaultdict
from datetime import datetime, timedelta
from random import Random

import pandas as pd
import pytest

from database.generators.ride_event_generator import RideEventGenerator
from simulation.behaviors import BehaviorConfig, BehaviorInjector
from simulation.gps import GpsGenerator
from simulation.rides import RIDE_EVENTS_HUB, RIDE_GPS_HUB, RideFactory, parse_ts
from simulation.run import backfill, live, request_time_sampler
from simulation.trip_planner import TripPlanner
from tests.conftest import FakePool

NIGHT = datetime(2026, 5, 6, 3, 0)


def make_factory(grid_router, calibration, seed=1, incident_rates=None, completion_rate=0.85):
    rng = Random(seed)
    config = BehaviorConfig()
    if incident_rates is not None:
        config.incident_rates = incident_rates
    planner = TripPlanner(grid_router, calibration, injector=BehaviorInjector(grid_router, config))
    generator = RideEventGenerator(FakePool(rng), {"generation": {"completion_rate": completion_rate}}, rng,
                                   trip_planner=planner)
    return RideFactory(generator, None, GpsGenerator(rng))


def flat_demand():
    return pd.DataFrame([{"day_of_week": d, "hour": h, "share": 1 / 168} for d in range(1, 8) for h in range(24)])


# ---------------- ride factory ----------------

def test_completed_ride_produces_both_streams(grid_router, calibration):
    ride = make_factory(grid_router, calibration, completion_rate=1.0).make(NIGHT)
    events = ride.bodies(RIDE_EVENTS_HUB)
    [plan] = ride.bodies(RIDE_GPS_HUB, "route_plan")
    pings = ride.bodies(RIDE_GPS_HUB, "gps_ping")

    assert [e["event_type"] for e in events][-1] == "completed"
    started = next(e for e in events if e["event_type"] == "started")
    assert parse_ts(plan["planned_at"]) == parse_ts(started["event_timestamp"])
    assert pings[0]["driver_id"] == started["driver_id"] == plan["driver_id"]
    assert {m.ride_id for m in ride.stream} == {ride.ride_id}


def test_every_message_is_scheduled_at_its_own_time(grid_router, calibration):
    ride = make_factory(grid_router, calibration, completion_rate=1.0).make(NIGHT)
    for m in ride.stream:
        stamp = m.body.get("ping_ts") or m.body.get("planned_at") or m.body["event_timestamp"]
        assert m.emit_at == parse_ts(stamp)


def test_late_defect_delays_sending_but_not_the_timestamp(grid_router, calibration):
    factory = make_factory(grid_router, calibration, completion_rate=1.0)

    class HoldBackLast:
        def inject(self, events):
            events = [dict(e) for e in events]
            events[-1]["_emit_delay_s"] = 120
            return events

    factory.defect_injector = HoldBackLast()
    ride = factory.make(NIGHT)
    last = [m for m in ride.stream if m.hub == RIDE_EVENTS_HUB][-1]
    assert last.emit_at - parse_ts(last.body["event_timestamp"]) == timedelta(seconds=120)
    assert "_emit_delay_s" not in last.body                     # internal field is never sent


def test_cancelled_ride_sends_no_gps(grid_router, calibration):
    ride = make_factory(grid_router, calibration, completion_rate=0.0).make(NIGHT)
    assert ride.bodies(RIDE_GPS_HUB) == [] and ride.incident_labels == []


# ---------------- backfill ----------------

def test_request_times_stay_in_window():
    sample = request_time_sampler(flat_demand(), datetime(2026, 5, 4), 3, Random(1))
    times = [sample() for _ in range(500)]
    assert min(times) >= datetime(2026, 5, 4) and max(times) < datetime(2026, 5, 7)


def test_backfill_writes_tables_and_manifest(grid_router, calibration, tmp_path):
    factory = make_factory(grid_router, calibration, incident_rates={"long_stop": 0.5})
    manifest = backfill(factory, 40, datetime(2026, 5, 4), 2, tmp_path, seed=3, flush_every=15,
                        demand=flat_demand())

    for table in ("ride_events", "route_plans", "gps_pings", "incident_labels", "behaviors"):
        assert list((tmp_path / table).glob("part-*.parquet")), table
    pings = pd.read_parquet(tmp_path / "gps_pings")
    labels = pd.read_parquet(tmp_path / "incident_labels")
    assert len(pings) == manifest["rows"]["gps_pings"]
    assert set(labels["incident_type"]) == {"long_stop"}
    assert manifest["measured_incident_rate"] == pytest.approx(len(labels) / manifest["completed_trips_with_gps"], abs=1e-4)
    assert json.loads((tmp_path / "manifest.json").read_text())["rides"] == 40


def test_labels_are_not_in_the_stream_tables(grid_router, calibration, tmp_path):
    factory = make_factory(grid_router, calibration, incident_rates={"long_stop": 1.0})
    backfill(factory, 10, datetime(2026, 5, 4), 1, tmp_path, seed=3, demand=flat_demand())
    pings = pd.read_parquet(tmp_path / "gps_pings")
    events = pd.read_parquet(tmp_path / "ride_events")
    assert not {"incident_type", "severity"} & set(pings.columns)
    assert not events["raw_json"].str.contains("incident").any()


# ---------------- live ----------------

class FakeClock:
    def __init__(self):
        self.t = 0.0

    def __call__(self):
        return self.t

    def sleep(self, seconds):
        self.t += max(seconds, 0.001)


class RecordingSink:
    def __init__(self, clock, start):
        self.clock, self.start = clock, start
        self.records, self.sent, self.failed, self.closed = [], defaultdict(int), defaultdict(int), False

    def send(self, hub, messages, partition_key):
        now = self.start + timedelta(seconds=self.clock())
        self.records += [(hub, partition_key, now, m) for m in messages]
        self.sent[hub] += len(messages)
        return True

    def close(self):
        self.closed = True


def test_live_sends_each_message_when_it_happens(grid_router, calibration):
    clock = FakeClock()
    sink = RecordingSink(clock, NIGHT)
    summary = live(make_factory(grid_router, calibration, completion_rate=1.0), sink, rate=0.02,
                   duration_s=300, seed=5, clock=clock, sleep=clock.sleep, now=NIGHT)

    assert summary["rides"] >= 1 and sink.closed
    for hub, ride_id, sent_at, body in sink.records:
        stamp = body.get("ping_ts") or body.get("planned_at") or body["event_timestamp"]
        assert sent_at >= parse_ts(stamp)                                  # never sent before it happened
        assert (sent_at - parse_ts(stamp)).total_seconds() < 2              # and not much later

    by_ride = defaultdict(list)
    for hub, ride_id, sent_at, body in sink.records:
        if body.get("message_type") == "gps_ping":
            by_ride[ride_id].append(body["seq_no"])
    assert all(seqs == sorted(seqs) for seqs in by_ride.values())         # pings stay in order per ride


def test_internal_fields_never_leave_the_simulator(grid_router, calibration):
    factory = make_factory(grid_router, calibration, completion_rate=1.0)

    class MarkIncomplete:
        def inject(self, events):
            events = [dict(e) for e in events[:-1]]
            events[-1]["_incomplete_ride"] = True                # v1 defect 8 marker
            return events

    factory.defect_injector = MarkIncomplete()
    ride = factory.make(NIGHT)
    assert not any(k.startswith("_") for m in ride.stream for k in m.body)
