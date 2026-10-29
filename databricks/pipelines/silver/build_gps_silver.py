# ============================================================================
# SILVER LAYER: GPS, route plans, live safety features, safety labels
# ----------------------------------------------------------------------------
# Four materialized views, live stream and simulator backfill unified:
#
#   sv_route_plans          one row per ride: the planned route
#   sv_gps_pings            one row per ping: deduplicated, ordered by event
#                           time, per-ping features and quality flags
#   sv_ride_live_features   one row per ride per 60 s window, with ride history:
#                           the model input (each row scores on its own)
#   sv_safety_labels        cleaned incident ground truth (training only)
#
# Every feature comes from utilities/safety_features.py, the same file used
# for local training. One implementation for training and scoring means the
# model never sees a feature computed two different ways (training-serving
# skew). That file must sit in the pipeline root: <root>/utilities/.
#
# Quality rules handled here (Bronze only checked single rows, ADR-001):
#   duplicates      -> dropped by message_id (at-least-once delivery)
#   out of order    -> pings sorted by event time inside each ride
#   gaps            -> flagged (is_gap), not filled
#   GPS jumps       -> flagged (is_jump), kept: a spoofing signal (ADR-011)
#   outside NYC     -> flagged (is_outside_nyc)
#
# `source` tells the worlds apart: "live" for the Event Hubs stream, the
# backfill run id (run_...) for simulator training data.
# ============================================================================

from pyspark import cloudpickle
from pyspark import pipelines as dp
from pyspark.sql.functions import col, expr, lit, max as max_, row_number, when
from pyspark.sql.window import Window

import utilities.safety_features as safety_features
from utilities.safety_features import (
    ENDED_AFTER_S, HISTORY_COLUMNS, HISTORY_DDL, PING_FEATURES_DDL, WINDOW_COLUMNS, WINDOW_FEATURES_DDL,
    history_features, ping_features, window_features,
)

# The feature functions run inside Spark's Python workers (applyInPandas).
# By default Spark ships a function as "import it from utilities.safety_features",
# and the workers cannot import from the pipeline folder. Shipping the module's
# code with the function makes the workers need nothing installed.
cloudpickle.register_pickle_by_value(safety_features)

CATALOG       = "rideops_ai"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"

PLAN_COLUMNS = {
    "ride_id": "string", "message_id": "string", "driver_id": "bigint",
    "planned_at": "string", "pickup_lat": "double", "pickup_lon": "double",
    "dropoff_lat": "double", "dropoff_lon": "double", "route_polyline": "string",
    "planned_distance_m": "double", "planned_duration_s": "bigint",
}
PING_COLUMNS = {
    "message_id": "string", "ride_id": "string", "driver_id": "bigint", "seq_no": "int",
    "ping_ts": "string", "lat": "double", "lon": "double", "speed_kmh": "double",
    "heading_deg": "double", "accuracy_m": "double",
}


def _typed(df, columns, source):
    """Contract columns with fixed types (live JSON and backfill Parquet differ slightly), plus `source`."""
    return (
        df.withColumn("source", source)
        .select(*[col(name).cast(dtype).alias(name) for name, dtype in columns.items()], "source")
    )


# ----------------------------------------------------------------------------
# Pure transformations (DataFrame in, DataFrame out): tested on a local Spark
# ----------------------------------------------------------------------------
def build_route_plans(live_messages, sim_plans):
    live = _typed(live_messages.where(col("message_type") == "route_plan"), PLAN_COLUMNS, lit("live"))
    sim = _typed(sim_plans, PLAN_COLUMNS, col("_run_id"))
    plans = live.unionByName(sim)

    # One plan per ride; a re-delivered copy is identical, so keep the first by message_id
    w = Window.partitionBy("ride_id").orderBy(col("message_id"))
    return (
        plans
        .withColumn("_rn", row_number().over(w))
        .where(col("_rn") == 1)
        .drop("_rn")
        .withColumn("planned_at", expr("try_cast(planned_at AS TIMESTAMP)"))
    )


def build_pings(live_messages, sim_pings):
    live = _typed(live_messages.where(col("message_type") == "gps_ping"), PING_COLUMNS, lit("live"))
    sim = _typed(sim_pings, PING_COLUMNS, col("_run_id"))
    return (
        live.unionByName(sim)
        # at-least-once delivery re-sends byte-identical copies: one per message_id
        .dropDuplicates(["message_id"])
        .withColumn("ping_ts", expr("try_cast(ping_ts AS TIMESTAMP)"))
        .where(col("ping_ts").isNotNull())
    )


def add_ping_features(pings, plans):
    """Per-ride feature computation: each ride's pings meet its route plan in pandas."""
    def per_ride(ride_pings, ride_plan):
        if ride_pings.empty:                       # a plan whose pings have not arrived yet
            return ping_features(ride_pings, None)  # empty frame with the output columns
        plan = ride_plan.iloc[0].to_dict() if len(ride_plan) else None
        return ping_features(ride_pings, plan)

    plan_cols = plans.select("ride_id", "route_polyline", "planned_at", "planned_duration_s", "dropoff_lat", "dropoff_lon")
    return (
        pings.groupBy("ride_id")
        .cogroup(plan_cols.groupBy("ride_id"))
        .applyInPandas(per_ride, PING_FEATURES_DDL)
    )


def add_window_features(ping_feats):
    """
    A backfill ride is complete. A live ride has ended when its last ping is
    older than ENDED_AFTER_S compared with the newest live ping; until then its
    last window is not final (it may still be moving).
    """
    newest_live = ping_feats.where(col("source") == "live").agg(max_("ping_ts").alias("_newest_live"))
    ride_last = ping_feats.groupBy("ride_id").agg(max_("ping_ts").alias("_ride_last"))
    ended = (
        ride_last.crossJoin(newest_live)
        .withColumn("ride_ended", expr(f"""
            _newest_live IS NULL
            OR unix_timestamp(_newest_live) - unix_timestamp(_ride_last) > {ENDED_AFTER_S}
        """))
        .select("ride_id", "ride_ended")
    )
    with_flag = ping_feats.join(ended, "ride_id").withColumn(
        "ride_ended", when(col("source") == "live", col("ride_ended")).otherwise(lit(True))
    )

    # Ride history (previous minute, worst so far, trend) is added here, per
    # ride, with the same function training uses: every row of the table is
    # then a complete model input that can be scored on its own.
    def per_ride(pdf):
        windows = window_features(pdf, ride_ended=bool(pdf["ride_ended"].iloc[0]))
        return history_features(windows)[WINDOW_COLUMNS + HISTORY_COLUMNS]

    return with_flag.groupBy("ride_id").applyInPandas(per_ride, f"{WINDOW_FEATURES_DDL}, {HISTORY_DDL}")


def build_labels(labels_raw):
    return (
        labels_raw
        .select(
            "incident_id", "ride_id", "incident_type", "severity", "params",
            expr("try_cast(start_ts AS TIMESTAMP)").alias("start_ts"),
            expr("try_cast(end_ts AS TIMESTAMP)").alias("end_ts"),
            col("_run_id").alias("source"),
        )
        .dropDuplicates(["incident_id"])
    )


# ----------------------------------------------------------------------------
# Pipeline tables
# ----------------------------------------------------------------------------
def _bronze(name):
    return spark.read.table(f"{CATALOG}.{BRONZE_SCHEMA}.{name}")


def _silver(name):
    return spark.read.table(f"{CATALOG}.{SILVER_SCHEMA}.{name}")


@dp.table(
    name=f"{CATALOG}.{SILVER_SCHEMA}.sv_route_plans",
    comment="Silver: one planned route per ride, live stream and simulator backfill"
)
def sv_route_plans():
    return build_route_plans(_bronze("gps_messages_raw"), _bronze("sim_route_plans_raw"))


@dp.table(
    name=f"{CATALOG}.{SILVER_SCHEMA}.sv_gps_pings",
    comment="Silver: deduplicated GPS pings in event-time order, with per-ping safety features and quality flags"
)
def sv_gps_pings():
    pings = build_pings(_bronze("gps_messages_raw"), _bronze("sim_gps_pings_raw"))
    return add_ping_features(pings, _silver("sv_route_plans"))


@dp.table(
    name=f"{CATALOG}.{SILVER_SCHEMA}.sv_ride_live_features",
    comment="Silver: safety features per ride per 60 s window, the input of the safety models"
)
def sv_ride_live_features():
    return add_window_features(_silver("sv_gps_pings"))


@dp.table(
    name=f"{CATALOG}.{SILVER_SCHEMA}.sv_safety_labels",
    comment="Silver: cleaned safety incident ground truth (training and evaluation only, never used for scoring)"
)
def sv_safety_labels():
    return build_labels(_bronze("safety_labels_raw"))
