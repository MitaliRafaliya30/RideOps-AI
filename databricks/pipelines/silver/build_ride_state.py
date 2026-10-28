# ============================================================================
# SILVER LAYER: Ride State (collapsed event stream, one row per ride_id)
# ----------------------------------------------------------------------------
# Two layers:
#   sv_ride_state_raw  -> create_auto_cdc_flow target. Accumulates milestone
#                         timestamps as events arrive, keyed by ride_id,
#                         sequenced by event_timestamp. ignore_null_updates
#                         means a later event's nulls never erase columns an
#                         earlier event already set.
#   sv_ride_state      -> derives final_status, cancellation_stage,
#                         durations, and staleness from the accumulated
#                         columns. A batch read, not a stream, since these
#                         are properties of the combined row, not a single
#                         incoming change.
#
# Defect 8 (incomplete rides): NOT an error. A ride stuck at "arrived" with
# no further events is a legitimate open row here, status stays "in_flight"
# until either a terminal event arrives or the 2-hour staleness threshold
# (locked earlier) flips it to "stale".
# ============================================================================

from pyspark import pipelines as dp
from pyspark.sql.functions import *

CATALOG          = "rideops_ai"
SILVER_SCHEMA    = "silver"
STALE_THRESHOLD  = 2 * 60 * 60  # 2 hours, in seconds


# ----------------------------------------------------------------------------
# LAYER 1: pivot each event into its milestone column, feed Auto CDC
# ----------------------------------------------------------------------------
@dp.view(name="ride_state_pivoted")
def ride_state_pivoted():
    events = spark.readStream.table(f"{CATALOG}.{SILVER_SCHEMA}.sv_ride_events")

    return (
        events
        .withColumn("requested_at", when(col("event_type") == "requested", col("event_timestamp")))
        .withColumn("assigned_at", when(col("event_type") == "assigned", col("event_timestamp")))
        .withColumn("accepted_at", when(col("event_type") == "accepted", col("event_timestamp")))
        .withColumn("arrived_at", when(col("event_type") == "arrived", col("event_timestamp")))
        .withColumn("started_at", when(col("event_type") == "started", col("event_timestamp")))
        .withColumn("completed_at", when(col("event_type") == "completed", col("event_timestamp")))
        .withColumn("cancelled_at", when(col("event_type") == "cancelled", col("event_timestamp")))
        # every event updates these, used for staleness in Layer 2
        .withColumn("last_event_type", col("event_type"))
        .withColumn("last_event_at", col("event_timestamp"))
        # everything else (passenger_id, driver_id, driver_name, vehicle_id,
        # vehicle_type_name, zones, payment_method_name, cancellation_reason_id,
        # reason_category, trip_miles, fare fields) already passes through
        # correctly null-or-populated per stage, straight from sv_ride_events,
        # no extra logic needed for those columns
    )


dp.create_streaming_table(
    name=f"{CATALOG}.{SILVER_SCHEMA}.sv_ride_state_raw",
    comment="Silver: accumulated ride milestones, upserted per event via Auto CDC (internal, use sv_ride_state instead)"
)

dp.create_auto_cdc_flow(
    target=f"{CATALOG}.{SILVER_SCHEMA}.sv_ride_state_raw",
    source="ride_state_pivoted",
    keys=["ride_id"],
    sequence_by=col("event_timestamp"),
    ignore_null_updates=True,
    stored_as_scd_type=1,
)


# ----------------------------------------------------------------------------
# LAYER 2: derive status, cancellation stage, durations, staleness
# ----------------------------------------------------------------------------
@dp.table(
    name=f"{CATALOG}.{SILVER_SCHEMA}.sv_ride_state",
    comment="Silver: one row per ride, final status, timing, and staleness derived from accumulated milestones"
)
def sv_ride_state():
    df = spark.read.table(f"{CATALOG}.{SILVER_SCHEMA}.sv_ride_state_raw")

    with_status = (
        df
        .withColumn(
            "final_status",
            when(col("completed_at").isNotNull(), "completed")
            .when(col("cancelled_at").isNotNull(), "cancelled")
            .otherwise("in_flight")
        )
        .withColumn(
            # only meaningful when final_status = "cancelled". Checked from
            # latest stage backward, since a ride reaching "started" implies
            # every earlier stage already happened too.
            "cancellation_stage",
            when(col("cancelled_at").isNotNull(),
                when(col("started_at").isNotNull(), "started")
                .when(col("arrived_at").isNotNull(), "arrived")
                .when(col("accepted_at").isNotNull(), "accepted")
                .when(col("assigned_at").isNotNull(), "assigned")
                .otherwise("requested")
            )
        )
    )

    with_durations = (
        with_status
        .withColumn(
            "time_to_assign_sec",
            unix_timestamp(col("assigned_at")) - unix_timestamp(col("requested_at"))
        )
        .withColumn(
            "wait_time_sec",
            unix_timestamp(col("arrived_at")) - unix_timestamp(col("requested_at"))
        )
        .withColumn(
            "time_to_pickup_sec",
            unix_timestamp(col("started_at")) - unix_timestamp(col("arrived_at"))
        )
        .withColumn(
            "ride_duration_sec",
            unix_timestamp(col("completed_at")) - unix_timestamp(col("started_at"))
        )
        .withColumn(
            "total_fare",
            col("base_passenger_fare") + col("tolls") + col("bcf")
            + col("sales_tax") + col("congestion_surcharge")
            + col("airport_fee") + col("cbd_congestion_fee") + col("tips")
        )
    )

    with_staleness = (
        with_durations
        .withColumn(
            "age_seconds",
            unix_timestamp(current_timestamp()) - unix_timestamp(col("last_event_at"))
        )
        .withColumn(
            "is_stale",
            (col("final_status") == "in_flight") & (col("age_seconds") > STALE_THRESHOLD)
        )
    )

    return with_staleness.withColumn("_silver_processed_at", current_timestamp())