"""
Tests for the Bronze GPS contract rules (databricks/pipelines/bronze/read_eventhub_gps.py).

Runs parse_and_validate() on a local Spark session with hand-made Kafka rows.
The pipeline module imports `pyspark.pipelines`, which only exists on
Databricks, so it is replaced by a no-op stub before the file is loaded.
Tests are skipped if a local Spark session cannot start.
"""

import importlib.util
import json
import os
import sys
import types
from pathlib import Path

import pytest

PIPELINE_FILE = Path(__file__).resolve().parents[1] / "databricks" / "pipelines" / "bronze" / "read_eventhub_gps.py"


def _stub_pipelines_module():
    dp = types.SimpleNamespace(
        table=lambda **kwargs: (lambda fn: fn),
        view=lambda **kwargs: (lambda fn: fn),
        read_stream=lambda name: None,
    )
    sys.modules["pyspark.pipelines"] = types.SimpleNamespace(**vars(dp))
    import pyspark
    pyspark.pipelines = sys.modules["pyspark.pipelines"]


@pytest.fixture(scope="module")
def spark():
    # A separate Spark 4 install on this machine must not leak its jars into PySpark 3.5
    os.environ.pop("SPARK_HOME", None)
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    try:
        from pyspark.sql import SparkSession
        session = (SparkSession.builder.master("local[1]").appName("bronze-gps-tests")
                   .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "1")
                   .config("spark.sql.session.timeZone", "UTC").getOrCreate())
        session.range(1).count()
    except Exception as exc:                                    # no Java / Spark on this machine
        pytest.skip(f"local Spark unavailable: {exc}")
    yield session
    session.stop()


@pytest.fixture(scope="module")
def bronze():
    _stub_pipelines_module()
    spec = importlib.util.spec_from_file_location("read_eventhub_gps", PIPELINE_FILE)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


PING = {
    "message_type": "gps_ping", "schema_version": 1, "message_id": "m1", "ride_id": "r1", "driver_id": 7,
    "seq_no": 3, "ping_ts": "2026-05-06T17:30:20.000Z", "lat": 40.7, "lon": -73.9,
    "speed_kmh": 25.5, "heading_deg": 90.0, "accuracy_m": 5.0,
}
PLAN = {
    "message_type": "route_plan", "schema_version": 1, "message_id": "m2", "ride_id": "r1", "driver_id": 7,
    "planned_at": "2026-05-06T17:30:00.000Z", "pickup_lat": 40.7, "pickup_lon": -73.9,
    "dropoff_lat": 40.8, "dropoff_lon": -73.95, "route_polyline": "abc", "planned_distance_m": 5000.0,
    "planned_duration_s": 900,
}


def run(spark, bronze, messages):
    """messages: list of dicts (sent as JSON) or raw strings. Returns {message_id or text: reason}."""
    rows = [(i, (json.dumps(m) if isinstance(m, dict) else m).encode("utf8"), "ride-gps", 0, i, None)
            for i, m in enumerate(messages)]
    raw = spark.createDataFrame(rows, "i int, value binary, topic string, partition int, offset long, timestamp timestamp")
    out = bronze.parse_and_validate(raw.drop("i")).collect()
    return {(r["message_id"] or r["raw_json"]): (r["_is_valid"], r["_quarantine_reason"]) for r in out}


def reason_of(spark, bronze, message):
    [(valid, reason)] = run(spark, bronze, [message]).values()
    return None if valid else reason


def with_(base, **changes):
    return {**base, **changes}


def without(base, *keys):
    return {k: v for k, v in base.items() if k not in keys}


def test_valid_messages_pass(spark, bronze):
    assert reason_of(spark, bronze, PING) is None
    assert reason_of(spark, bronze, PLAN) is None


def test_heading_is_nullable(spark, bronze):
    assert reason_of(spark, bronze, with_(PING, heading_deg=None)) is None


def test_malformed_json(spark, bronze):
    assert reason_of(spark, bronze, "{this is not json") == "malformed_json"


def test_type_mismatch_is_malformed_not_lost(spark, bronze):
    # the str() bug from v1: a number sent as text
    assert reason_of(spark, bronze, with_(PING, speed_kmh="fast")) == "malformed_json"


def test_unknown_message_type(spark, bronze):
    assert reason_of(spark, bronze, with_(PING, message_type="gps_blob")) == "unknown_message_type"


@pytest.mark.parametrize("field", ["message_id", "ride_id", "driver_id", "ping_ts", "lat", "lon", "speed_kmh", "seq_no", "accuracy_m"])
def test_missing_ping_field(spark, bronze, field):
    assert reason_of(spark, bronze, without(PING, field)) == "null_required_field"


@pytest.mark.parametrize("field", ["planned_at", "pickup_lat", "dropoff_lon", "route_polyline", "planned_distance_m", "planned_duration_s"])
def test_missing_plan_field(spark, bronze, field):
    assert reason_of(spark, bronze, without(PLAN, field)) == "null_required_field"


def test_missing_message_type_is_caught(spark, bronze):
    assert reason_of(spark, bronze, without(PING, "message_type")) == "null_required_field"


@pytest.mark.parametrize("changes", [
    {"lat": 91.0}, {"lat": -90.5}, {"lon": 181.0}, {"lon": -200.0},
    {"speed_kmh": -1.0}, {"accuracy_m": -3.0}, {"heading_deg": 400.0}, {"heading_deg": -5.0}, {"seq_no": 0},
])
def test_out_of_range_ping(spark, bronze, changes):
    assert reason_of(spark, bronze, with_(PING, **changes)) == "out_of_range_value"


@pytest.mark.parametrize("changes", [
    {"pickup_lat": 95.0}, {"dropoff_lon": 190.0}, {"planned_distance_m": 0.0}, {"planned_duration_s": -5},
])
def test_out_of_range_plan(spark, bronze, changes):
    assert reason_of(spark, bronze, with_(PLAN, **changes)) == "out_of_range_value"


def test_invalid_timestamp(spark, bronze):
    assert reason_of(spark, bronze, with_(PING, ping_ts="yesterday")) == "invalid_timestamp"
    assert reason_of(spark, bronze, with_(PLAN, planned_at="2026-13-45")) == "invalid_timestamp"


def test_gps_jump_is_not_quarantined(spark, bronze):
    # A teleport is a spoofing signal: it must reach Silver, not be thrown away here (ADR-011)
    far_away = with_(PING, lat=41.9, lon=-72.0)           # ~200 km from NYC, still a valid coordinate
    assert reason_of(spark, bronze, far_away) is None


def test_every_row_lands_in_exactly_one_table(spark, bronze):
    batch = [PING, PLAN, with_(PING, message_id="m3", lat=999.0), "garbage", without(with_(PING, message_id="m4"), "ride_id")]
    results = run(spark, bronze, batch)
    assert len(results) == 5
    assert sum(valid for valid, _ in results.values()) == 2          # valid rows
    assert all(reason is not None for valid, reason in results.values() if not valid)


def test_lineage_columns_are_kept(spark, bronze):
    raw = spark.createDataFrame([(json.dumps(PING).encode(), "ride-gps", 4, 99, None)],
                                "value binary, topic string, partition int, offset long, timestamp timestamp")
    [row] = bronze.parse_and_validate(raw).collect()
    assert (row["_kafka_topic"], row["_kafka_partition"], row["_kafka_offset"]) == ("ride-gps", 4, 99)
    assert row["raw_json"] == json.dumps(PING)
