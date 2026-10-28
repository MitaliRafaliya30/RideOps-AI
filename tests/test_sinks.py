"""
Tests for simulation/sinks.py
Event Hubs is replaced by a fake producer; nothing touches Azure.
"""

import json

import pytest

from simulation.sinks import EventHubSink, JsonlSink


class FakeBatch:
    def __init__(self, partition_key, capacity):
        self.partition_key, self.capacity, self.items = partition_key, capacity, []

    def add(self, data):
        if len(self.items) >= self.capacity:
            raise ValueError("batch full")
        self.items.append(json.loads(b"".join(data.body)))


class FakeProducer:
    def __init__(self, capacity=100, fail_times=0):
        self.capacity, self.fail_times = capacity, fail_times
        self.sent_batches, self.closed = [], False

    def create_batch(self, partition_key=None):
        return FakeBatch(partition_key, self.capacity)

    def send_batch(self, batch):
        if self.fail_times:
            self.fail_times -= 1
            raise ConnectionError("throttled")
        self.sent_batches.append(batch)

    def close(self):
        self.closed = True


def make_sink(producer, sleeps):
    return EventHubSink({"ride-gps": "conn"}, producer_factory=lambda conn, hub: producer, sleep=sleeps.append)


def test_messages_keep_their_partition_key():
    producer, sleeps = FakeProducer(), []
    assert make_sink(producer, sleeps).send("ride-gps", [{"seq_no": 1}, {"seq_no": 2}], "ride-1")
    [batch] = producer.sent_batches
    assert batch.partition_key == "ride-1"
    assert [m["seq_no"] for m in batch.items] == [1, 2]


def test_full_batch_is_split_without_losing_order():
    producer, sleeps = FakeProducer(capacity=2), []
    make_sink(producer, sleeps).send("ride-gps", [{"seq_no": i} for i in range(5)], "ride-1")
    assert [[m["seq_no"] for m in b.items] for b in producer.sent_batches] == [[0, 1], [2, 3], [4]]


def test_retries_with_exponential_backoff():
    producer, sleeps = FakeProducer(fail_times=2), []
    sink = make_sink(producer, sleeps)
    assert sink.send("ride-gps", [{"seq_no": 1}], "ride-1")
    assert sleeps == [1.0, 2.0]
    assert sink.sent["ride-gps"] == 1 and sink.failed["ride-gps"] == 0


def test_gives_up_after_three_attempts_and_counts_failures():
    producer, sleeps = FakeProducer(fail_times=5), []
    sink = make_sink(producer, sleeps)
    assert not sink.send("ride-gps", [{"a": 1}, {"a": 2}], "ride-1")
    assert sink.failed["ride-gps"] == 2 and sleeps == [1.0, 2.0]


def test_close_closes_every_producer():
    producer, sleeps = FakeProducer(), []
    sink = make_sink(producer, sleeps)
    sink.close()
    assert producer.closed


def test_jsonl_sink_writes_hub_and_body(tmp_path):
    sink = JsonlSink(tmp_path / "out.jsonl")
    sink.send("ride-events", [{"event_type": "requested"}], "ride-1")
    sink.close()
    [line] = (tmp_path / "out.jsonl").read_text().splitlines()
    record = json.loads(line)
    assert record["hub"] == "ride-events" and record["body"] == {"event_type": "requested"}
    assert record["partition_key"] == "ride-1"
