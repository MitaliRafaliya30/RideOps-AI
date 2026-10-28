# ============================================================================
# SILVER LAYER: Data Quality Observability
# ----------------------------------------------------------------------------
# Two tables, not a monitoring framework:
#   dq_all_quarantine    -> unions sv_quarantine (M2, batch) and
#                            sv_ride_events_quarantine (M4, streaming) into
#                            one queryable interface, since Lakeflow can't
#                            physically merge a batch materialized view and
#                            a streaming table into one target.
#   dq_defect_scorecard  -> one row per locked v1 defect, with a real count,
#                            so every defect this project deliberately
#                            injects is provable, not just claimed.
#
# Row counts per Silver table are deliberately NOT a table here, that's a
# two-line ad hoc query, building pipeline infrastructure for it would be
# exactly the overengineering this project has been avoiding throughout.
# ============================================================================

from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.window import Window

CATALOG       = "rideops_ai"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"


# ----------------------------------------------------------------------------
# dq_all_quarantine: single interface over both quarantine tables
# ----------------------------------------------------------------------------
@dp.table(
    name=f"{CATALOG}.{SILVER_SCHEMA}.dq_all_quarantine",
    comment="Silver: unified view of every quarantined record across all sources"
)
def dq_all_quarantine():
    master_data_failures = spark.read.table(f"{CATALOG}.{SILVER_SCHEMA}.sv_quarantine")
    ride_event_failures = spark.read.table(f"{CATALOG}.{SILVER_SCHEMA}.sv_ride_events_quarantine")

    return master_data_failures.unionByName(ride_event_failures)


# ----------------------------------------------------------------------------
# dq_defect_scorecard: proof, not a claim, for each of the 8 locked defects
# ----------------------------------------------------------------------------
@dp.table(
    name=f"{CATALOG}.{SILVER_SCHEMA}.dq_defect_scorecard",
    comment="Silver: measured count of every v1 defect, proving injection and handling both actually happened"
)
def dq_defect_scorecard():

    bronze_events = spark.read.table(f"{CATALOG}.{BRONZE_SCHEMA}.ride_events_raw")
    bronze_quarantine = spark.read.table(f"{CATALOG}.{BRONZE_SCHEMA}.ride_events_quarantine")
    ride_state = spark.read.table(f"{CATALOG}.{SILVER_SCHEMA}.sv_ride_state")

    # ---- defect 1: out-of-order, via a window function ----
    # For each ride, order events by arrival (_kafka_offset), compare each
    # event's timestamp to the one before it. A backward step in event_time
    # while moving forward in arrival order is direct evidence of scrambling.
    w = Window.partitionBy("ride_id").orderBy("_kafka_offset")
    out_of_order_count = (
        bronze_events
        .withColumn("_prev_ts", lag("event_timestamp").over(w))
        .filter(col("_prev_ts").isNotNull() & (col("event_timestamp") < col("_prev_ts")))
        .count()
    )

    # ---- defect 2: late arrival, event_timestamp vs Kafka enqueue time ----
    # 60s threshold separates genuine defect-2 injection (5-30 min holds)
    # from ordinary pipeline processing latency.
    late_arrival_count = (
    bronze_events
    .filter(
        (col("_kafka_timestamp").cast("long") - col("event_timestamp").cast("timestamp").cast("long")) > 60
    )
    .count()
    )

    # ---- defect 3: exact duplicates, measured before Silver's dedup ----
    total_events = bronze_events.count()
    distinct_event_ids = bronze_events.select("event_id").distinct().count()
    exact_duplicate_count = total_events - distinct_event_ids

    # ---- defect 4: near-duplicates, same ride_id+event_type, new event_id ----
    near_duplicate_count = (
        bronze_events
        .groupBy("ride_id", "event_type")
        .agg(countDistinct("event_id").alias("n"))
        .filter(col("n") > 1)
        .count()
    )

    # ---- defects 5, 6, 7: schema violations, already broken out in Bronze ----
    quarantine_by_reason = {
        row["_quarantine_reason"]: row["cnt"]
        for row in (
            bronze_quarantine
            .groupBy("_quarantine_reason")
            .count()
            .withColumnRenamed("count", "cnt")
            .collect()
        )
    }
    null_field_count = quarantine_by_reason.get("null_required_field", 0)
    invalid_type_count = quarantine_by_reason.get("invalid_event_type", 0)
    out_of_range_count = quarantine_by_reason.get("out_of_range_value", 0)

    # ---- defect 8: incomplete rides, graceful handling proven ----
    in_flight_count = ride_state.filter(col("final_status") == "in_flight").count()
    stale_count = ride_state.filter(col("is_stale") == True).count()

    rows = [
        (1, "out_of_order_events",   "Bronze/Silver", out_of_order_count,   "backward step in event_time within a ride, ordered by arrival"),
        (2, "late_arriving_events",  "Bronze",         late_arrival_count,   "enqueue time exceeds event time by >60s"),
        (3, "exact_duplicates",      "Bronze",         exact_duplicate_count,"repeated event_id, measured before Silver dedup"),
        (4, "near_duplicates",       "Bronze",         near_duplicate_count, "(ride_id, event_type) with >1 distinct event_id"),
        (5, "null_required_fields",  "Bronze",         null_field_count,     "from ride_events_quarantine"),
        (6, "invalid_event_type",    "Bronze",         invalid_type_count,   "from ride_events_quarantine"),
        (7, "out_of_range_values",   "Bronze",         out_of_range_count,   "from ride_events_quarantine"),
        (8, "incomplete_rides",      "Silver",         stale_count,      f"{in_flight_count} total rides currently in_flight (includes normal in-progress rides); {stale_count} of those are past the 2hr stale threshold and represent genuine defect-8 cases"),
    ]

    return spark.createDataFrame(
        rows,
        schema="defect_id INT, defect_name STRING, layer STRING, count LONG, notes STRING"
    ).withColumn("_measured_at", current_timestamp())