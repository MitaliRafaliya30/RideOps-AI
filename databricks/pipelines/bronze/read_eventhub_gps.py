# ============================================================================
# BRONZE LAYER: GPS stream from Azure Event Hubs (hub: ride-gps)
# ----------------------------------------------------------------------------
# Two message types arrive on the same hub (contract: docs/DATA_DESIGN.md 2.2, 2.3):
#   route_plan   once per trip, the expected route
#   gps_ping     every 10 s while the trip is in progress
#
# Bronze responsibilities, same as read_eventhub.py:
#   1. Read from source        2. Parse JSON with an explicit schema
#   3. Validate the CONTRACT   4. Store AS-IS (no business logic)
#   5. Add metadata + Kafka lineage   6. Quarantine contract violations
#
# Quarantine = single-row contract violations only (ADR-001):
#   malformed_json, unknown_message_type, null_required_field,
#   invalid_timestamp, out_of_range_value
#
# NOT quarantined here, on purpose:
#   duplicates, out-of-order pings and gaps   -> need other rows       -> Silver
#   GPS jumps (impossible speed)               -> a spoofing signal    -> Silver flags it (ADR-011)
#
# The validation lives in parse_and_validate(), a plain function with no
# pipeline calls, so tests/test_bronze_gps.py can run it on a local Spark.
# ============================================================================

from pyspark import pipelines as dp
from pyspark.sql.functions import (
    coalesce, col, current_timestamp, expr, from_json, lit, when,
)
from pyspark.sql.types import (
    DoubleType, IntegerType, LongType, StringType, StructField, StructType,
)

# ----------------------------------------------------------------------------
# EXPECTED SCHEMA: one struct for both message types (message_type tells them apart)
# ----------------------------------------------------------------------------
GPS_SCHEMA = StructType([
    # common
    StructField("message_type",        StringType()),
    StructField("schema_version",      IntegerType()),
    StructField("message_id",          StringType()),
    StructField("ride_id",             StringType()),
    StructField("driver_id",           LongType()),
    # route_plan
    StructField("planned_at",          StringType()),
    StructField("pickup_lat",          DoubleType()),
    StructField("pickup_lon",          DoubleType()),
    StructField("dropoff_lat",         DoubleType()),
    StructField("dropoff_lon",         DoubleType()),
    StructField("route_polyline",      StringType()),
    StructField("planned_distance_m",  DoubleType()),
    StructField("planned_duration_s",  LongType()),
    # gps_ping
    StructField("seq_no",              IntegerType()),
    StructField("ping_ts",             StringType()),
    StructField("lat",                 DoubleType()),
    StructField("lon",                 DoubleType()),
    StructField("speed_kmh",           DoubleType()),
    StructField("heading_deg",         DoubleType()),
    StructField("accuracy_m",          DoubleType()),
    # filled by from_json when the text is not valid JSON
    StructField("_corrupt_record",     StringType()),
])

VALID_MESSAGE_TYPES = ["route_plan", "gps_ping"]

COMMON_REQUIRED = ["message_type", "message_id", "ride_id", "driver_id"]
REQUIRED_BY_TYPE = {
    "route_plan": ["planned_at", "pickup_lat", "pickup_lon", "dropoff_lat", "dropoff_lon",
                   "route_polyline", "planned_distance_m", "planned_duration_s"],
    "gps_ping":   ["seq_no", "ping_ts", "lat", "lon", "speed_kmh", "accuracy_m"],   # heading is nullable
}


def _flag(condition):
    """A rule must be True or False, never NULL: a NULL would make the row vanish from both tables."""
    return coalesce(condition, lit(False))


def _out_of_range():
    in_world = lambda lat, lon: (lat < -90) | (lat > 90) | (lon < -180) | (lon > 180)
    plan = (col("message_type") == "route_plan") & (
        in_world(col("pickup_lat"), col("pickup_lon"))
        | in_world(col("dropoff_lat"), col("dropoff_lon"))
        | (col("planned_distance_m") <= 0)
        | (col("planned_duration_s") <= 0)
    )
    ping = (col("message_type") == "gps_ping") & (
        in_world(col("lat"), col("lon"))
        | (col("speed_kmh") < 0)
        | (col("accuracy_m") < 0)
        | (col("heading_deg") < 0) | (col("heading_deg") > 360)
        | (col("seq_no") < 1)
    )
    return _flag(plan | ping)


def parse_and_validate(raw):
    """
    raw: a DataFrame with the Kafka columns (value, topic, partition, offset, timestamp).
    Returns every message with `_is_valid` and `_quarantine_reason` (first matching rule wins).
    """
    parsed = (
        raw
        .withColumn("raw_json", col("value").cast("string"))
        .withColumn("data", from_json(col("raw_json"), GPS_SCHEMA, {"mode": "PERMISSIVE"}))
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source", lit("event_hubs_gps"))
        .withColumn("_kafka_topic", col("topic"))
        .withColumn("_kafka_partition", col("partition"))
        .withColumn("_kafka_offset", col("offset"))
        .withColumn("_kafka_timestamp", col("timestamp"))
    )
    msg = parsed.select(
        "data.*", "raw_json",
        "_ingested_at", "_source",
        "_kafka_topic", "_kafka_partition", "_kafka_offset", "_kafka_timestamp",
    )

    rule_malformed = _flag(col("_corrupt_record").isNotNull())
    rule_unknown_type = _flag(col("message_type").isNotNull() & ~col("message_type").isin(*VALID_MESSAGE_TYPES))

    rule_missing = lit(False)
    for field in COMMON_REQUIRED:
        rule_missing = rule_missing | col(field).isNull()
    for message_type, fields in REQUIRED_BY_TYPE.items():
        for field in fields:
            rule_missing = rule_missing | ((col("message_type") == message_type) & col(field).isNull())
    rule_missing = _flag(rule_missing)

    # try_cast gives NULL for a bad string; Databricks serverless runs in ANSI
    # mode, where to_timestamp() would fail the whole stream instead.
    rule_bad_timestamp = _flag(
        ((col("message_type") == "gps_ping") & col("ping_ts").isNotNull() & expr("try_cast(ping_ts AS TIMESTAMP)").isNull())
        | ((col("message_type") == "route_plan") & col("planned_at").isNotNull() & expr("try_cast(planned_at AS TIMESTAMP)").isNull())
    )
    rule_out_of_range = _out_of_range()

    reason = (
        when(rule_malformed, "malformed_json")
        .when(rule_unknown_type, "unknown_message_type")
        .when(rule_missing, "null_required_field")
        .when(rule_bad_timestamp, "invalid_timestamp")
        .when(rule_out_of_range, "out_of_range_value")
    )
    return msg.withColumn("_quarantine_reason", reason).withColumn("_is_valid", reason.isNull())


# ----------------------------------------------------------------------------
# STEP 1: internal view that reads the hub and tags validity
# ----------------------------------------------------------------------------
@dp.view(
    name="gps_messages_staged",
    comment="Internal: parses raw ride-gps messages and flags contract validity"
)
def gps_messages_staged():
    # Pipeline configuration, read at run time (see databricks/pipeline_settings.json)
    namespace = spark.conf.get("RideOps-Eventhub-Namespace")
    hub = spark.conf.get("RideOps-Eventhub-GPS", "ride-gps")
    connection_string = spark.conf.get("connection_string")

    kafka_options = {
        "kafka.bootstrap.servers": f"{namespace}.servicebus.windows.net:9093",
        "subscribe": hub,
        "kafka.sasl.mechanism": "PLAIN",
        "kafka.security.protocol": "SASL_SSL",
        "kafka.sasl.jaas.config": (
            "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule "
            f'required username="$ConnectionString" password="{connection_string}";'
        ),
        "kafka.request.timeout.ms": 10000,
        "kafka.session.timeout.ms": 10000,
        "maxOffsetsPerTrigger": 50000,     # ~120 pings per ride: 50k messages is about 400 rides per batch
        "startingOffsets": "earliest",
        # The GPS hub keeps only 1 day. A pipeline that was off for longer would
        # hit offsets that no longer exist; with "true" that fails every run
        # until a full refresh. GPS is high-volume and short-lived, so skip the
        # gap (Spark logs it) rather than stop the stream.
        "failOnDataLoss": "false",
    }
    raw = spark.readStream.format("kafka").options(**kafka_options).load()
    return parse_and_validate(raw)


# ----------------------------------------------------------------------------
# STEP 2: Bronze table, contract-valid messages only
# ----------------------------------------------------------------------------
@dp.table(
    name="gps_messages_raw",
    comment="Bronze: contract-valid route plans and GPS pings from Event Hubs, stored as sent"
)
def gps_messages_raw():
    return (
        dp.read_stream("gps_messages_staged")
        .where(col("_is_valid"))
        .drop("_is_valid", "_corrupt_record", "_quarantine_reason")
    )


# ----------------------------------------------------------------------------
# STEP 3: quarantine, contract violations with the reason
# ----------------------------------------------------------------------------
@dp.table(
    name="gps_messages_quarantine",
    comment="Bronze: GPS messages that broke the contract, kept with the raw text and the reason"
)
def gps_messages_quarantine():
    return (
        dp.read_stream("gps_messages_staged")
        .where(~col("_is_valid"))
        .withColumn("_quarantined_at", current_timestamp())
        .drop("_is_valid")
    )
