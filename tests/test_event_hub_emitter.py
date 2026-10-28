"""
Tests for database/generators/event_hub_emitter.py (v1 emitter)
Validates serialization, batching per ride, and metrics.

The Azure producer is replaced by a fake, so no connection is needed.
(These replace earlier tests written for a simulated emitter API that no
longer exists.)
"""

import json
from decimal import Decimal

import pytest

import database.generators.event_hub_emitter as emitter_module
from database.generators.event_hub_emitter import EventHubEmitter


class FakeBatch:
    def __init__(self):
        self.items = []

    def add(self, data):
        self.items.append(json.loads(b"".join(data.body)))


class FakeProducer:
    def __init__(self, fail=False):
        self.fail, self.batches, self.closed = fail, [], False

    def create_batch(self, **kwargs):
        return FakeBatch()

    def send_batch(self, batch):
        if self.fail:
            raise ConnectionError("unreachable")
        self.batches.append(batch)

    def close(self):
        self.closed = True


@pytest.fixture
def make_emitter(monkeypatch):
    def _make(fail=False):
        producer = FakeProducer(fail)
        monkeypatch.setattr(emitter_module.EventHubProducerClient, "from_connection_string",
                            lambda conn, eventhub_name: producer)
        config = {"event_hubs": {"connection_string": "Endpoint=sb://test/", "hub_name": "ride-events"}}
        return EventHubEmitter(config), producer
    return _make


RIDE = [
    {"event_id": "e1", "ride_id": "r1", "event_type": "requested", "base_passenger_fare": None},
    {"event_id": "e2", "ride_id": "r1", "event_type": "completed", "base_passenger_fare": 21.5},
]


def test_hub_name_comes_from_config(make_emitter):
    emitter, _ = make_emitter()
    assert emitter.hub_name == "ride-events"


def test_one_batch_per_ride_in_order(make_emitter):
    emitter, producer = make_emitter()
    assert emitter.send(RIDE)
    [batch] = producer.batches
    assert [e["event_id"] for e in batch.items] == ["e1", "e2"]


def test_metrics_after_send(make_emitter):
    emitter, _ = make_emitter()
    emitter.send(RIDE)
    metrics = emitter.get_metrics()
    assert metrics["events_sent"] == 2 and metrics["batches_sent"] == 1 and metrics["events_failed"] == 0
    assert metrics["bytes_sent"] > 0


def test_empty_ride_sends_nothing(make_emitter):
    emitter, producer = make_emitter()
    assert emitter.send([])
    assert producer.batches == []


def test_failed_send_is_counted_not_raised(make_emitter):
    emitter, _ = make_emitter(fail=True)
    assert emitter.send(RIDE) is False
    assert emitter.get_metrics()["events_failed"] == 2


def test_decimals_are_serialized(make_emitter):
    emitter, _ = make_emitter()
    payload = json.loads(emitter._serialize_event({"fare": Decimal("12.50")}))
    assert payload == {"fare": "12.50"}


def test_close_closes_producer(make_emitter):
    emitter, producer = make_emitter()
    emitter.close()
    assert producer.closed
