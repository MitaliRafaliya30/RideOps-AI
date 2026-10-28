# ============================================================================
# SILVER LAYER: NYC FHVHV Historical Trips
# ----------------------------------------------------------------------------
# Implements the locked Step 5c decisions from the profiling report review:
#   - ADR-004 scope filter: HV0003 (Uber) only, not HV0005
#   - Deduplication on a composite fingerprint (no natural key exists)
#   - Flag, don't drop, for all four confirmed data quality findings
#   - Standardize into the common column vocabulary shared with sv_ride_events
#   - Derive parallel timing measures so both worlds are comparable in Gold
#   - Zone-enrich both pickup and dropoff (pickup and dropoff are separate
#     roles against the same sv_zone dimension)
#
# This table stays single, no quarantine split, because none of the four
# flagged issues are schema violations. They're all business-rule questions
# still open per the profiling report, so Silver surfaces them as flags and
# lets Gold/analysts decide, rather than encoding an unmade decision.
# ============================================================================

from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.window import Window

CATALOG       = "rideops_ai"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"

# Composite fingerprint used for dedup, since FHVHV has no primary key.
# True duplicates match on all of these simultaneously.
DEDUP_KEYS = [
    "hvfhs_license_num", "pickup_datetime",
    "PULocationID", "DOLocationID", "base_passenger_fare",
]


@dp.table(
    name=f"{CATALOG}.{SILVER_SCHEMA}.sv_fhvhv_trips",
    comment="Silver: standardized Uber (HV0003) historical trips, flagged not filtered for the four confirmed DQ findings"
)
def sv_fhvhv_trips():

    df = spark.read.table(f"{CATALOG}.{BRONZE_SCHEMA}.fhvhv_trips_raw")

    # ---- ADR-004: scope to HV0003 only, ~15M of 22M rows ----
    scoped = df.filter(col("hvfhs_license_num") == "HV0003")

    # ---- dedupe: keep one row per composite fingerprint ----
    # row_number(), not dropDuplicates(), for the same reason as M1/M2:
    # deterministic, explicit tie-breaking rather than relying on Spark's
    # unspecified dropDuplicates ordering.
    w = Window.partitionBy(*DEDUP_KEYS).orderBy(col("dropoff_datetime").asc())

    deduped = (
        scoped
        .withColumn("_rn", row_number().over(w))
        .filter(col("_rn") == 1)
        .drop("_rn")
    )

    # ---- standardize types and names into the common vocabulary ----
    standardized = (
        deduped
        .withColumn("request_at", col("request_datetime").cast("timestamp"))
        .withColumn("on_scene_at", col("on_scene_datetime").cast("timestamp"))
        .withColumn("pickup_at", col("pickup_datetime").cast("timestamp"))
        .withColumn("dropoff_at", col("dropoff_datetime").cast("timestamp"))
        .withColumn("pickup_zone_id", col("PULocationID").cast("long"))
        .withColumn("dropoff_zone_id", col("DOLocationID").cast("long"))
        .withColumn("trip_miles", col("trip_miles").cast("double"))
        .withColumn("trip_time_sec", col("trip_time").cast("long"))
        .withColumn("base_passenger_fare", col("base_passenger_fare").cast("decimal(10,2)"))
        .withColumn("tolls", col("tolls").cast("decimal(10,2)"))
        .withColumn("bcf", col("bcf").cast("decimal(10,2)"))
        .withColumn("sales_tax", col("sales_tax").cast("decimal(10,2)"))
        .withColumn("congestion_surcharge", col("congestion_surcharge").cast("decimal(10,2)"))
        .withColumn("airport_fee", col("airport_fee").cast("decimal(10,2)"))
        .withColumn("cbd_congestion_fee", col("cbd_congestion_fee").cast("decimal(10,2)"))
        .withColumn("tips", col("tips").cast("decimal(10,2)"))
        .withColumn("driver_pay", col("driver_pay").cast("decimal(10,2)"))
        .withColumn("shared_request_flag", col("shared_request_flag") == "Y")
        .withColumn("shared_match_flag", col("shared_match_flag") == "Y")
        .withColumn("access_a_ride_flag", col("access_a_ride_flag") == "Y")
        .withColumn("wav_request_flag", col("wav_request_flag") == "Y")
        .withColumn("wav_match_flag", col("wav_match_flag") == "Y")
    )

    # ---- derived measures: parallel to the operational world's timings ----
    with_measures = (
        standardized
        .withColumn(
            "wait_time_sec",
            unix_timestamp(col("pickup_at")) - unix_timestamp(col("request_at"))
        )
        .withColumn(
            "computed_duration_sec",
            unix_timestamp(col("dropoff_at")) - unix_timestamp(col("pickup_at"))
        )
        .withColumn(
            "duration_variance_sec",
            col("trip_time_sec") - col("computed_duration_sec")
        )
        .withColumn(
            "total_fare",
            col("base_passenger_fare") + col("tolls") + col("bcf")
            + col("sales_tax") + col("congestion_surcharge")
            + col("airport_fee") + col("cbd_congestion_fee") + col("tips")
        )
        .withColumn("pickup_date", to_date(col("pickup_at")))
        .withColumn("pickup_hour", hour(col("pickup_at")))
    )

    # ---- the four confirmed DQ flags: soft signals, not rejections ----
    flagged = (
        with_measures
        .withColumn("has_negative_fare", col("base_passenger_fare") < 0)
        .withColumn("has_negative_driver_pay", col("driver_pay") < 0)
        .withColumn("has_timestamp_anomaly", col("request_at") > col("on_scene_at"))
        # duration_variance_sec already carries the trip_time mismatch signal
        # as a number, not a boolean, since the report showed most variance
        # is small (~10 sec) and a hard flag threshold would be arbitrary.
        # Downstream can decide their own tolerance by filtering on the value.
    )

    # ---- zone enrichment: pickup and dropoff are two roles, same dimension ----
    zone_pu = spark.read.table(f"{CATALOG}.{SILVER_SCHEMA}.sv_zone").select(
        col("zone_id").alias("__pu_id"),
        col("borough").alias("pickup_borough"),
        col("zone_name").alias("pickup_zone_name"),
    )
    zone_do = spark.read.table(f"{CATALOG}.{SILVER_SCHEMA}.sv_zone").select(
        col("zone_id").alias("__do_id"),
        col("borough").alias("dropoff_borough"),
        col("zone_name").alias("dropoff_zone_name"),
    )

    enriched = (
        flagged
        .join(broadcast(zone_pu), flagged["pickup_zone_id"] == col("__pu_id"), "left")
        .join(broadcast(zone_do), flagged["dropoff_zone_id"] == col("__do_id"), "left")
        .drop("__pu_id", "__do_id")
    )

    return (
        enriched
        .withColumn("source", lit("fhvhv"))
        .withColumn("_silver_processed_at", current_timestamp())
        .select(
            "source", "hvfhs_license_num",
            "request_at", "on_scene_at", "pickup_at", "dropoff_at",
            "pickup_date", "pickup_hour",
            "pickup_zone_id", "pickup_borough", "pickup_zone_name",
            "dropoff_zone_id", "dropoff_borough", "dropoff_zone_name",
            "trip_miles", "trip_time_sec", "computed_duration_sec", "duration_variance_sec",
            "wait_time_sec",
            "base_passenger_fare", "tolls", "bcf", "sales_tax",
            "congestion_surcharge", "airport_fee", "cbd_congestion_fee",
            "tips", "driver_pay", "total_fare",
            "shared_request_flag", "shared_match_flag", "access_a_ride_flag",
            "wav_request_flag", "wav_match_flag",
            "has_negative_fare", "has_negative_driver_pay", "has_timestamp_anomaly",
            "_silver_processed_at",
        )
    )