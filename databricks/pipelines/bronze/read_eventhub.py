# ============================================================================
# BRONZE LAYER: Ride Events Ingestion from Azure Event Hubs
# ----------------------------------------------------------------------------
# Bronze responsibilities (and ONLY these):
#   1. Read from source (Event Hubs)
#   2. Parse into structured format (JSON)
#   3. Validate SCHEMA (structure + contract, no business logic)
#   4. Store AS-IS (raw + parsed columns, nothing transformed)
#   5. Add METADATA (_ingested_at, _source, etc.)
#   6. Handle ERRORS (quarantine records that fail schema validation)
#   7. Track LINEAGE (kafka topic/partition/offset)
#
# Quarantine scope (per the locked v1 defect contract):
#   Only defects 5, 6, 7 land here, because they can be detected by looking
#   at a single row in isolation:
#     5. Null required fields (always-required + stage-required)
#     6. Invalid event_type (not one of the 7 legal values)
#     7. Out-of-range values on completed events (negative fare/miles/etc.)
#   Defects 1, 2, 3, 4, 8 (ordering, lateness, duplicates, incomplete rides)
#   need cross-row comparison or the passage of time to detect, so they
#   correctly stay in ride_events_raw for Silver to handle.
# ============================================================================

from pyspark import pipelines as dp
from pyspark.sql.types import *
from pyspark.sql.functions import *

# ----------------------------------------------------------------------------
# CONFIGURATION
# Values come from Pipeline Settings > Configuration
# NOTE: Configuration is read inside the function, not at module import time,
# to ensure pipeline config values are available when the streaming query starts.
# ----------------------------------------------------------------------------

# ----------------------------------------------------------------------------
# EXPECTED SCHEMA
# "_corrupt_record" catches malformed JSON that can't be parsed at all.
# ----------------------------------------------------------------------------
event_schema = StructType([
    StructField("event_id",               StringType()),
    StructField("ride_id",                StringType()),
    StructField("event_type",             StringType()),
    StructField("event_timestamp",        StringType()),
    StructField("passenger_id",           LongType()),
    StructField("driver_id",              LongType()),
    StructField("vehicle_id",             LongType()),
    StructField("pickup_location_id",     IntegerType()),
    StructField("dropoff_location_id",    IntegerType()),
    StructField("payment_method_id",      IntegerType()),
    StructField("cancellation_reason_id", IntegerType()),
    StructField("trip_miles",             DoubleType()),
    StructField("base_passenger_fare",    DoubleType()),
    StructField("tolls",                  DoubleType()),
    StructField("bcf",                    DoubleType()),
    StructField("sales_tax",              DoubleType()),
    StructField("congestion_surcharge",   DoubleType()),
    StructField("airport_fee",            DoubleType()),
    StructField("cbd_congestion_fee",     DoubleType()),
    StructField("tips",                   DoubleType()),
    StructField("driver_pay",             DoubleType()),
    StructField("_corrupt_record",        StringType()),
])

# ----------------------------------------------------------------------------
# CONTRACT: fields that must never be null, on every event regardless of stage
# ----------------------------------------------------------------------------
ALWAYS_REQUIRED_FIELDS = ["event_id", "ride_id", "event_type", "event_timestamp"]

# ----------------------------------------------------------------------------
# CONTRACT: fields required from a given event_type onward.
# This mirrors the "required vs optional by stage" table from the event
# contract design (driver_id/vehicle_id are null before assignment, then
# required; fare/trip fields only appear on completed; cancellation_reason_id
# only appears on cancelled).
#
# Note: driver_id/vehicle_id on "cancelled" are deliberately NOT checked here,
# because they're only required if the ride had already been assigned before
# it cancelled. That's a stateful nuance, out of scope for a single-row check.
# ----------------------------------------------------------------------------
STAGE_REQUIRED_FIELDS = {
    "assigned":  ["driver_id", "vehicle_id"],
    "accepted":  ["driver_id", "vehicle_id"],
    "arrived":   ["driver_id", "vehicle_id"],
    "started":   ["driver_id", "vehicle_id"],
    "completed": ["driver_id", "vehicle_id", "trip_miles", "base_passenger_fare"],
    "cancelled": ["cancellation_reason_id"],
}

# ----------------------------------------------------------------------------
# CONTRACT: the only 7 legal event_type values
# ----------------------------------------------------------------------------
VALID_EVENT_TYPES = [
    "requested", "assigned", "accepted", "arrived",
    "started", "completed", "cancelled",
]


# ----------------------------------------------------------------------------
# STEP 1: Internal view that reads, parses, and tags validity
# ----------------------------------------------------------------------------
@dp.view(
    name="ride_events_staged",
    comment="Internal: parses raw Event Hubs messages and flags schema validity"
)
def ride_events_staged():
    # Read configuration values at function execution time
    EH_NAMESPACE = spark.conf.get("RideOps-Eventhub-Namespace")
    EH_NAME      = spark.conf.get("RideOps-Eventhub")
    EH_CONN_STR  = spark.conf.get("connection_string")

    KAFKA_OPTIONS = {
        "kafka.bootstrap.servers" : f"{EH_NAMESPACE}.servicebus.windows.net:9093",
        "subscribe"               : EH_NAME,
        "kafka.sasl.mechanism"    : "PLAIN",
        "kafka.security.protocol" : "SASL_SSL",
        "kafka.sasl.jaas.config"  : (
            "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule "
            f'required username="$ConnectionString" password="{EH_CONN_STR}";'
        ),
        "kafka.request.timeout.ms" : 10000,
        "kafka.session.timeout.ms" : 10000,
        "maxOffsetsPerTrigger"     : 10000,
        "failOnDataLoss"           : "true",
        "startingOffsets"          : "earliest",
    }

    raw = (
        spark.readStream
        .format("kafka")
        .options(**KAFKA_OPTIONS)
        .load()
    )

    parsed = (
        raw
        .withColumn("raw_json", col("value").cast("string"))
        .withColumn("data", from_json(col("raw_json"), event_schema, {"mode": "PERMISSIVE"}))
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source", lit("event_hubs"))
        .withColumn("_kafka_topic", col("topic"))
        .withColumn("_kafka_partition", col("partition"))
        .withColumn("_kafka_offset", col("offset"))
        .withColumn("_kafka_timestamp", col("timestamp"))
    )

    flattened = parsed.select(
        "data.*",
        "raw_json",
        "_ingested_at", "_source",
        "_kafka_topic", "_kafka_partition", "_kafka_offset", "_kafka_timestamp",
    )

    # ---- Rule: malformed JSON, couldn't even be parsed ----
    rule_malformed = col("_corrupt_record").isNotNull()

    # ---- Rule: an always-required header field is null (defect 5a) ----
    rule_missing_header = lit(False)
    for field in ALWAYS_REQUIRED_FIELDS:
        rule_missing_header = rule_missing_header | col(field).isNull()

    # ---- Rule: event_type is set but not one of the 7 legal values (defect 6) ----
    # (event_type being null is already covered by rule_missing_header)
    rule_invalid_type = col("event_type").isNotNull() & (~col("event_type").isin(VALID_EVENT_TYPES))

    # ---- Rule: a stage-required field is null for this event's stage (defect 5b) ----
    rule_missing_stage_field = lit(False)
    for stage, fields in STAGE_REQUIRED_FIELDS.items():
        for field in fields:
            rule_missing_stage_field = rule_missing_stage_field | (
                (col("event_type") == stage) & col(field).isNull()
            )

    # ---- Rule: negative values where only non-negative makes sense (defect 7) ----
    # Only checked on completed events, since that's the only stage these
    # fields are populated. Upper-bound "absurd value" checks (e.g. trip_miles
    # > 100) are intentionally left out here, that's a softer/MEDIUM-severity
    # signal better suited to Silver, not a hard Bronze rejection.
    rule_out_of_range = (
    (col("event_type") == "completed") & (
        (col("trip_miles") < 0) | (col("trip_miles") > 500) |
        (col("base_passenger_fare") < 0) | (col("base_passenger_fare") > 500) |
        (col("tolls") < 0) |
        (col("tips") < 0) | (col("tips") > 500) |
        (col("driver_pay") < 0) | (col("driver_pay") > 500)
    )
)

    is_invalid = (
        rule_malformed
        | rule_missing_header
        | rule_invalid_type
        | rule_missing_stage_field
        | rule_out_of_range
    )

    # First matching rule wins, gives quarantine rows a readable reason
    # instead of just a boolean, so you can query quarantine by defect type.
    quarantine_reason = (
        when(rule_malformed, "malformed_json")
        .when(rule_missing_header, "null_required_field")
        .when(rule_invalid_type, "invalid_event_type")
        .when(rule_missing_stage_field, "null_required_field")
        .when(rule_out_of_range, "out_of_range_value")
        .otherwise(None)
    )

    return (
        flattened
        .withColumn("_is_valid", ~is_invalid)
        .withColumn("_quarantine_reason", quarantine_reason)
    )


# ----------------------------------------------------------------------------
# STEP 2: Bronze table, only schema-valid records
# ----------------------------------------------------------------------------
@dp.table(
    name="ride_events_raw",
    comment="Bronze: schema-valid ride events, stored as-is from Event Hubs"
)
def ride_events_raw():
    return (
        dp.read_stream("ride_events_staged")
        .where(col("_is_valid"))
        .drop("_is_valid", "_corrupt_record", "_quarantine_reason")
    )


# ----------------------------------------------------------------------------
# STEP 3: Quarantine table, only genuine contract violations (defects 5/6/7)
# ----------------------------------------------------------------------------
@dp.table(
    name="ride_events_quarantine",
    comment="Bronze: events that failed schema validation (defects 5, 6, 7), kept for investigation"
)
def ride_events_quarantine():
    return (
        dp.read_stream("ride_events_staged")
        .where(~col("_is_valid"))
        .withColumn("_quarantined_at", current_timestamp())
        .drop("_is_valid")
    )