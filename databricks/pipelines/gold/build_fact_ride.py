# ============================================================================
# GOLD LAYER: Ride Fact
# ----------------------------------------------------------------------------
# Grain: one row per ride_id, from sv_ride_state.
#
# ride_id is the natural key here, not wrapped in an invented surrogate.
# date_key computed by formula (same as dim_date's own construction), not
# joined, standard practice for date dimensions.
#
# final_status / cancellation_stage stay as plain text on the fact, not FK'd
# to dim_ride_status, that dimension's 7 values describe individual events
# (fact_ride_event's job in G5), this fact's status is a coarser, already-
# computed 3-value category from Silver.
#
# payment_method joined by NAME, not id: sv_ride_events dropped
# payment_method_id from its final select in Silver M4. Fixing that properly
# requires a full-refresh cascade through the whole checkpointed event
# stream. Name-join accepted here since payment_method_name is a small,
# stable, controlled-vocabulary field, not free text.
#
# Every entity FK is a LEFT join, nullable by design: a ride cancelled at
# "requested" never gets a driver_key or vehicle_key, that's a legitimate
# state, not a data quality problem.
# ============================================================================

from pyspark import pipelines as dp
from pyspark.sql.functions import *

CATALOG       = "rideops_ai"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA   = "gold"


@dp.table(
    name=f"{CATALOG}.{GOLD_SCHEMA}.fact_ride",
    comment="Gold: one row per ride, current status, timing, and fare"
)
def fact_ride():

    rides = spark.read.table(f"{CATALOG}.{SILVER_SCHEMA}.sv_ride_state")

    passengers = spark.read.table(f"{CATALOG}.{GOLD_SCHEMA}.dim_passenger").select(
        col("passenger_id").alias("__pax_id"), col("passenger_key")
    )
    drivers = spark.read.table(f"{CATALOG}.{GOLD_SCHEMA}.dim_driver").select(
        col("driver_id").alias("__drv_id"), col("driver_key")
    )
    vehicles = spark.read.table(f"{CATALOG}.{GOLD_SCHEMA}.dim_vehicle").select(
        col("vehicle_id").alias("__veh_id"), col("vehicle_key")
    )
    pu_zones = spark.read.table(f"{CATALOG}.{GOLD_SCHEMA}.dim_zone").select(
        col("zone_id").alias("__pu_zone_id"), col("zone_key").alias("pickup_zone_key")
    )
    do_zones = spark.read.table(f"{CATALOG}.{GOLD_SCHEMA}.dim_zone").select(
        col("zone_id").alias("__do_zone_id"), col("zone_key").alias("dropoff_zone_key")
    )
    payments = spark.read.table(f"{CATALOG}.{GOLD_SCHEMA}.dim_payment_method").select(
        col("payment_method_name").alias("__pm_name"), col("payment_method_key")
    )

    resolved = (
        rides
        .join(broadcast(passengers), rides["passenger_id"] == col("__pax_id"), "left")
        .join(broadcast(drivers), rides["driver_id"] == col("__drv_id"), "left")
        .join(broadcast(vehicles), rides["vehicle_id"] == col("__veh_id"), "left")
        .join(broadcast(pu_zones), rides["pickup_location_id"] == col("__pu_zone_id"), "left")
        .join(broadcast(do_zones), rides["dropoff_location_id"] == col("__do_zone_id"), "left")
        .join(broadcast(payments), rides["payment_method_name"] == col("__pm_name"), "left")
        .withColumn(
            "date_key",
            (year("requested_at") * 10000 + month("requested_at") * 100 + dayofmonth("requested_at")).cast("int")
        )
    )

    return resolved.select(
        "ride_id", "date_key",
        "passenger_key", "driver_key", "vehicle_key",
        "pickup_zone_key", "dropoff_zone_key", "payment_method_key",
        "final_status", "cancellation_stage",
        "requested_at", "assigned_at", "accepted_at", "arrived_at",
        "started_at", "completed_at", "cancelled_at",
        "time_to_assign_sec", "wait_time_sec", "time_to_pickup_sec", "ride_duration_sec",
        "trip_miles", "base_passenger_fare", "tolls", "bcf", "sales_tax",
        "congestion_surcharge", "airport_fee", "cbd_congestion_fee",
        "tips", "driver_pay", "total_fare",
        "age_seconds", "is_stale",
    )