# ============================================================================
# GOLD LAYER: Passenger, Driver, and Vehicle Dimensions
# ----------------------------------------------------------------------------
# Current-only snapshots of Silver's SCD2 tables (__END_AT IS NULL), each
# with a Gold-owned surrogate key. Full history stays available in Silver
# for anyone who needs point-in-time analysis later.
#
# vehicle_type folded into dim_vehicle, membership_tier and preferred
# payment method folded into dim_passenger, both are single-parent
# descriptive attributes, not relationships facts need independently.
#
# driver_id deliberately NOT carried into dim_vehicle: vehicle-to-driver
# assignment is event-scoped (can change over time), not a stable vehicle
# attribute. That relationship is correctly captured per-ride on fact_ride,
# which carries both driver_key and vehicle_key on the same row.
# ============================================================================

from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.window import Window

CATALOG       = "rideops_ai"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA   = "gold"


@dp.table(
    name=f"{CATALOG}.{GOLD_SCHEMA}.dim_passenger",
    comment="Gold: current passenger attributes, membership tier and preferred payment denormalized in"
)
def dim_passenger():
    passengers = (
        spark.read.table(f"{CATALOG}.{SILVER_SCHEMA}.sv_passengers")
        .filter(col("__END_AT").isNull())
    )

    tiers = spark.read.table(f"{CATALOG}.{SILVER_SCHEMA}.sv_membership_tiers").select(
        col("membership_tier_id").alias("__tier_id"), col("membership_name")
    )
    payments = spark.read.table(f"{CATALOG}.{SILVER_SCHEMA}.sv_payment_methods").select(
        col("payment_method_id").alias("__pay_id"), col("payment_method_name").alias("preferred_payment_method_name")
    )
    zones = spark.read.table(f"{CATALOG}.{SILVER_SCHEMA}.sv_zone").select(
        col("zone_id").alias("__zone_id"), col("borough").alias("home_borough"), col("zone_name").alias("home_zone_name")
    )

    enriched = (
        passengers
        .join(broadcast(tiers), passengers["membership_tier_id"] == col("__tier_id"), "left")
        .join(broadcast(payments), passengers["preferred_payment_method_id"] == col("__pay_id"), "left")
        .join(broadcast(zones), passengers["home_zone_id"] == col("__zone_id"), "left")
    )

    w = Window.orderBy("passenger_id")

    return (
        enriched
        .withColumn("passenger_key", row_number().over(w))
        .select(
            "passenger_key", "passenger_id",
            "signup_date", "avg_driver_rating_given",
            "membership_name",
            "preferred_payment_method_name",
            "home_zone_id", "home_borough", "home_zone_name",
        )
    )


@dp.table(
    name=f"{CATALOG}.{GOLD_SCHEMA}.dim_driver",
    comment="Gold: current driver attributes"
)
def dim_driver():
    drivers = (
        spark.read.table(f"{CATALOG}.{SILVER_SCHEMA}.sv_drivers")
        .filter(col("__END_AT").isNull())
    )

    w = Window.orderBy("driver_id")

    return (
        drivers
        .withColumn("driver_key", row_number().over(w))
        .select(
            "driver_key", "driver_id", "driver_code", "driver_name",
            "rating", "experience_years", "join_date",
        )
    )


@dp.table(
    name=f"{CATALOG}.{GOLD_SCHEMA}.dim_vehicle",
    comment="Gold: current vehicle attributes, vehicle type denormalized in. No driver reference, that's event-scoped, captured on fact_ride instead"
)
def dim_vehicle():
    vehicles = (
        spark.read.table(f"{CATALOG}.{SILVER_SCHEMA}.sv_vehicles")
        .filter(col("__END_AT").isNull())
    )

    vtypes = spark.read.table(f"{CATALOG}.{SILVER_SCHEMA}.sv_vehicle_types").select(
        col("vehicle_type_id").alias("__vt_id"), col("vehicle_type_name"),
        col("capacity"), col("base_fare_multiplier"),
    )

    enriched = vehicles.join(broadcast(vtypes), vehicles["vehicle_type_id"] == col("__vt_id"), "left")

    w = Window.orderBy("vehicle_id")

    return (
        enriched
        .withColumn("vehicle_key", row_number().over(w))
        .select(
            "vehicle_key", "vehicle_id",
            "vehicle_type_name", "capacity", "base_fare_multiplier",
            "manufacture_year", "last_service_date",
        )
    )