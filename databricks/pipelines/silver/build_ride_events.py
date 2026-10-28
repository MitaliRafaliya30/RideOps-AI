# ============================================================================
# SILVER LAYER: Ride Events
# ----------------------------------------------------------------------------
# Implements Silver Step 6 from the design doc: dedup, watermark, referential
# validation, and enrichment for the streaming ride event source.
#
# Handles defects 3, 4 (dedup) and 1, 2 (ordering/lateness, via watermark).
# Defects 5, 6, 7 were already caught in Bronze (schema-level), so events
# reaching ride_events_raw are already structurally valid, nothing here
# re-checks null event_id or invalid event_type.
#
# Watermark = 45 minutes: derived from the defect injector's own numbers,
# defect 2 holds back terminal events by up to 15-30 minutes at its highest
# severity setting, so 45 minutes gives headroom before anything is dropped
# instead of demonstrating late-handling.
#
# dropDuplicatesWithinWatermark, not dropDuplicates+watermark: the latter
# keeps unbounded global state that never expires. The former bounds state
# to the watermark window, the correct choice for a Continuous pipeline.
# ============================================================================

from pyspark import pipelines as dp
from pyspark.sql.functions import *

CATALOG        = "rideops_ai"
BRONZE_SCHEMA  = "bronze"
SILVER_SCHEMA  = "silver"
WATERMARK      = "45 minutes"


# ----------------------------------------------------------------------------
# STAGE 1: read, watermark, dedup, zone check
# One streaming view, so sv_ride_events and sv_ride_events_quarantine below
# branch off identical deduped state instead of each running their own
# independent (and inconsistent) dedup pass.
# ----------------------------------------------------------------------------
@dp.view(name="ride_events_processed")
def ride_events_processed():

    events = (
        spark.readStream.table(f"{CATALOG}.{BRONZE_SCHEMA}.ride_events_raw")
        .withColumn("event_timestamp", col("event_timestamp").cast("timestamp"))
        .withWatermark("event_timestamp", WATERMARK)
        # defect 3: exact duplicate event_id
        .dropDuplicatesWithinWatermark(["event_id"])
        # defect 4: near-duplicate, same ride_id + event_type, new event_id
        .dropDuplicatesWithinWatermark(["ride_id", "event_type"])
    )

    zone_pu = spark.read.table(f"{CATALOG}.{SILVER_SCHEMA}.sv_zone").select(
        col("zone_id").alias("__pu_id")
    )
    zone_do = spark.read.table(f"{CATALOG}.{SILVER_SCHEMA}.sv_zone").select(
        col("zone_id").alias("__do_id")
    )

    checked = (
        events
        .join(broadcast(zone_pu), events["pickup_location_id"] == col("__pu_id"), "left")
        .join(broadcast(zone_do), events["dropoff_location_id"] == col("__do_id"), "left")
    )

    is_valid = col("__pu_id").isNotNull() & col("__do_id").isNotNull()
    reason = (
        when(col("__pu_id").isNull(), "unresolvable_pickup_zone")
        .when(col("__do_id").isNull(), "unresolvable_dropoff_zone")
        .otherwise(None)
    )

    return (
        checked
        .withColumn("_is_valid", is_valid)
        .withColumn("_fail_reason", reason)
        .drop("__pu_id", "__do_id")
    )


# ----------------------------------------------------------------------------
# STAGE 2a: quarantine, the one thing that can't merge into sv_quarantine
# (that table is a batch materialized view from M2, this source is a real
# stream, Lakeflow can't target both write patterns at one physical table)
# ----------------------------------------------------------------------------
@dp.table(
    name=f"{CATALOG}.{SILVER_SCHEMA}.sv_ride_events_quarantine",
    comment="Silver: ride events with an unresolvable zone reference"
)
def sv_ride_events_quarantine():
    df = spark.readStream.table("ride_events_processed").filter(~col("_is_valid"))
    return (
        df.select(
            lit("ride_events").alias("source_table"),
            to_json(struct(*[c for c in df.columns if not c.startswith("_")])).alias("record_json"),
            col("_fail_reason").alias("failed_rule"),
            current_timestamp().alias("quarantined_at"),
        )
    )


# ----------------------------------------------------------------------------
# STAGE 2b: sv_ride_events, valid rows, enriched
# ----------------------------------------------------------------------------
@dp.table(
    name=f"{CATALOG}.{SILVER_SCHEMA}.sv_ride_events",
    comment="Silver: deduplicated, enriched ride lifecycle events, one row per event"
)
def sv_ride_events():

    valid = spark.readStream.table("ride_events_processed").filter(col("_is_valid"))

    # ---- SCD2 dimensions, current row only ----
    # __END_AT IS NULL is what makes this "current". Skipping this filter
    # would join against every historical version and silently fan out
    # duplicate rows into the stream.
    passengers = (
        spark.read.table(f"{CATALOG}.{SILVER_SCHEMA}.sv_passengers")
        .filter(col("__END_AT").isNull())
        .select(
            col("passenger_id").alias("__p_id"),
            col("membership_tier_id"),
        )
    )
    drivers = (
        spark.read.table(f"{CATALOG}.{SILVER_SCHEMA}.sv_drivers")
        .filter(col("__END_AT").isNull())
        .select(
            col("driver_id").alias("__d_id"),
            col("driver_name"),
            col("rating").alias("driver_rating"),
        )
    )
    vehicles = (
        spark.read.table(f"{CATALOG}.{SILVER_SCHEMA}.sv_vehicles")
        .filter(col("__END_AT").isNull())
        .select(
            col("vehicle_id").alias("__v_id"),
            col("vehicle_type_id"),
        )
    )

    # ---- small reference dims ----
    tiers = spark.read.table(f"{CATALOG}.{SILVER_SCHEMA}.sv_membership_tiers").select(
        col("membership_tier_id").alias("__tier_id"), col("membership_name")
    )
    vtypes = spark.read.table(f"{CATALOG}.{SILVER_SCHEMA}.sv_vehicle_types").select(
        col("vehicle_type_id").alias("__vtype_id"), col("vehicle_type_name")
    )
    payments = spark.read.table(f"{CATALOG}.{SILVER_SCHEMA}.sv_payment_methods").select(
        col("payment_method_id").alias("__pm_id"), col("payment_method_name")
    )
    statuses = spark.read.table(f"{CATALOG}.{SILVER_SCHEMA}.sv_ride_status").select(
        col("ride_status_id").alias("__status_id"), col("ride_status_name")
    )
    reasons = spark.read.table(f"{CATALOG}.{SILVER_SCHEMA}.sv_cancellation_reasons").select(
        col("cancellation_reason_id").alias("__reason_id"), col("reason_category")
    )
    zones = spark.read.table(f"{CATALOG}.{SILVER_SCHEMA}.sv_zone").select(
        col("zone_id").alias("__zone_id"), col("zone_name"), col("borough")
    )

    enriched = (
        valid
        # entity dims: left join, unresolved becomes "unknown member" below,
        # not a dropped or quarantined row
        .join(broadcast(passengers), valid["passenger_id"] == col("__p_id"), "left")
        .join(broadcast(drivers), valid["driver_id"] == col("__d_id"), "left")
        .join(broadcast(vehicles), valid["vehicle_id"] == col("__v_id"), "left")
        # descriptive lookups, chained off what was just resolved
        .join(broadcast(tiers), col("membership_tier_id") == col("__tier_id"), "left")
        .join(broadcast(vtypes), col("vehicle_type_id") == col("__vtype_id"), "left")
        .join(broadcast(payments), valid["payment_method_id"] == col("__pm_id"), "left")
        .join(broadcast(statuses), lower(valid["event_type"]) == lower(col("ride_status_name")), "left")
        .join(broadcast(reasons), valid["cancellation_reason_id"] == col("__reason_id"), "left")
        .join(
            broadcast(zones.withColumnRenamed("zone_name", "pickup_zone_name")
                            .withColumnRenamed("borough", "pickup_borough")),
            valid["pickup_location_id"] == col("__zone_id"), "left"
        )
        .join(
            broadcast(zones.withColumnRenamed("zone_name", "dropoff_zone_name")
                            .withColumnRenamed("borough", "dropoff_borough")
                            .withColumnRenamed("__zone_id", "__zone_id2")),
            valid["dropoff_location_id"] == col("__zone_id2"), "left"
        )
    )

    # ---- unknown-member pattern for late-arriving dimensions ----
    # A driver_id that exists in Postgres but hasn't synced into sv_drivers
    # yet is expected timing skew, not bad data. Keep the event, mark the
    # attribute unknown instead of quarantining a valid record.
    with_fallbacks = (
        enriched
        .withColumn("driver_name", coalesce(col("driver_name"), lit("Unknown")))
        .withColumn("vehicle_type_name", coalesce(col("vehicle_type_name"), lit("Unknown")))
        .withColumn("membership_name", coalesce(col("membership_name"), lit("Unknown")))
    )

    return (
        with_fallbacks
        .withColumn("_silver_processed_at", current_timestamp())
        .select(
            "event_id", "ride_id", "event_type", "event_timestamp",
            "passenger_id", "membership_name",
            "driver_id", "driver_name", "driver_rating",
            "vehicle_id", "vehicle_type_name",
            "pickup_location_id", "pickup_zone_name", "pickup_borough",
            "dropoff_location_id", "dropoff_zone_name", "dropoff_borough",
            "payment_method_name",
            "ride_status_name",
            "cancellation_reason_id", "reason_category",
            "trip_miles", "base_passenger_fare", "tolls", "bcf", "sales_tax",
            "congestion_surcharge", "airport_fee", "cbd_congestion_fee",
            "tips", "driver_pay",
            "_kafka_topic", "_kafka_partition", "_kafka_offset",
            "_silver_processed_at",
        )
    )