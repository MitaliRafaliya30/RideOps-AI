# ============================================================================
# GOLD LAYER: FHVHV Trip Fact
# ----------------------------------------------------------------------------
# Grain: one row per historical trip, from sv_fhvhv_trips.
#
# Resolves ONLY against dim_date and dim_zone. No passenger/driver/vehicle
# dimensions, by design: FHVHV data was never assigned those ids, and
# fabricating a join to World A's entity dimensions would mean inventing
# data that doesn't exist. dim_zone and dim_date are the sole, deliberate
# bridge between the operational and historical worlds.
#
# trip_id is a stable SHA-256 hash of the same composite fingerprint Silver
# already deduped on, not row_number(). This table grows every time a new
# month of FHVHV data lands, row_number() would renumber every existing key
# on every rebuild, a hash of stable input fields never does.
# ============================================================================

from pyspark import pipelines as dp
from pyspark.sql.functions import *

CATALOG       = "rideops_ai"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA   = "gold"


@dp.table(
    name=f"{CATALOG}.{GOLD_SCHEMA}.fact_fhvhv_trip",
    comment="Gold: one row per historical Uber (HV0003) trip"
)
def fact_fhvhv_trip():

    trips = spark.read.table(f"{CATALOG}.{SILVER_SCHEMA}.sv_fhvhv_trips")

    pu_zones = spark.read.table(f"{CATALOG}.{GOLD_SCHEMA}.dim_zone").select(
        col("zone_id").alias("__pu_id"), col("zone_key").alias("pickup_zone_key")
    )
    do_zones = spark.read.table(f"{CATALOG}.{GOLD_SCHEMA}.dim_zone").select(
        col("zone_id").alias("__do_id"), col("zone_key").alias("dropoff_zone_key")
    )

    resolved = (
        trips
        .join(broadcast(pu_zones), trips["pickup_zone_id"] == col("__pu_id"), "left")
        .join(broadcast(do_zones), trips["dropoff_zone_id"] == col("__do_id"), "left")
        .withColumn(
            "date_key",
            (year("pickup_at") * 10000 + month("pickup_at") * 100 + dayofmonth("pickup_at")).cast("int")
        )
        .withColumn(
            "trip_id",
            sha2(concat_ws("||",
                col("hvfhs_license_num"),
                col("pickup_at").cast("string"),
                col("pickup_zone_id").cast("string"),
                col("dropoff_zone_id").cast("string"),
                col("base_passenger_fare").cast("string"),
            ), 256)
        )
    )

    return resolved.select(
        "trip_id", "date_key", "hvfhs_license_num",
        "pickup_zone_key", "dropoff_zone_key",
        "request_at", "on_scene_at", "pickup_at", "dropoff_at",
        "pickup_hour",
        "trip_miles", "trip_time_sec", "computed_duration_sec", "duration_variance_sec",
        "wait_time_sec",
        "base_passenger_fare", "tolls", "bcf", "sales_tax",
        "congestion_surcharge", "airport_fee", "cbd_congestion_fee",
        "tips", "driver_pay", "total_fare",
        "shared_request_flag", "shared_match_flag", "access_a_ride_flag",
        "wav_request_flag", "wav_match_flag",
        "has_negative_fare", "has_negative_driver_pay", "has_timestamp_anomaly",
    )