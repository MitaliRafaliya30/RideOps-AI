# ============================================================================
# GOLD LAYER: Ride Event Fact
# ----------------------------------------------------------------------------
# Grain: one row per ride lifecycle event, from sv_ride_events.
#
# This table's job is narrower than fact_ride's: Silver's enrichment step
# already resolved driver name, vehicle type, zone names, etc. as text, so
# here we're mainly swapping those Silver lookups for Gold surrogate keys,
# plus resolving event_type against dim_ride_status, which fact_ride
# deliberately did not do (that dimension describes individual events, not
# a whole ride's coarser 3-value status).
#
# Zone joins use pickup_location_id/dropoff_location_id (the raw ids
# sv_ride_events still carries), preferred over name-joining since the id
# is directly available here, no ambiguity risk.
#
# event_type -> dim_ride_status is a name-join, same accepted trade-off as
# fact_ride's payment_method join: small, fixed, controlled vocabulary.
# ============================================================================

from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.window import Window

CATALOG       = "rideops_ai"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA   = "gold"


@dp.table(
    name=f"{CATALOG}.{GOLD_SCHEMA}.fact_ride_event",
    comment="Gold: one row per ride lifecycle event"
)
def fact_ride_event():

    events = spark.read.table(f"{CATALOG}.{SILVER_SCHEMA}.sv_ride_events")

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
    statuses = spark.read.table(f"{CATALOG}.{GOLD_SCHEMA}.dim_ride_status").select(
        col("ride_status_name").alias("__status_name"), col("ride_status_key")
    )
    reasons = spark.read.table(f"{CATALOG}.{GOLD_SCHEMA}.dim_cancellation_reason").select(
        col("cancellation_reason_id").alias("__reason_id"), col("cancellation_reason_key")
    )

    resolved = (
        events
        .join(broadcast(passengers), events["passenger_id"] == col("__pax_id"), "left")
        .join(broadcast(drivers), events["driver_id"] == col("__drv_id"), "left")
        .join(broadcast(vehicles), events["vehicle_id"] == col("__veh_id"), "left")
        .join(broadcast(pu_zones), events["pickup_location_id"] == col("__pu_zone_id"), "left")
        .join(broadcast(do_zones), events["dropoff_location_id"] == col("__do_zone_id"), "left")
        .join(broadcast(payments), events["payment_method_name"] == col("__pm_name"), "left")
        .join(broadcast(statuses), lower(events["event_type"]) == lower(col("__status_name")), "left")
        .join(broadcast(reasons), events["cancellation_reason_id"] == col("__reason_id"), "left")
        .withColumn(
            "date_key",
            (year("event_timestamp") * 10000 + month("event_timestamp") * 100 + dayofmonth("event_timestamp")).cast("int")
        )
    )

    w = Window.orderBy("event_timestamp", "event_id")

    return (
        resolved
        .withColumn("ride_event_key", row_number().over(w))
        .select(
            "ride_event_key", "event_id", "ride_id", "date_key",
            "event_timestamp", "event_type", "ride_status_key",
            "passenger_key", "driver_key", "vehicle_key",
            "pickup_zone_key", "dropoff_zone_key",
            "payment_method_key", "cancellation_reason_key",
            "trip_miles", "base_passenger_fare", "tolls", "bcf", "sales_tax",
            "congestion_surcharge", "airport_fee", "cbd_congestion_fee",
            "tips", "driver_pay",
        )
    )