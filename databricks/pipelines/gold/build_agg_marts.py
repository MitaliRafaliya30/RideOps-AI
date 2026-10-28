# ============================================================================
# GOLD LAYER: Aggregation Marts
# ----------------------------------------------------------------------------
# agg_zone_hourly and agg_revenue_daily deliberately union World A
# (fact_ride) and World B (fact_fhvhv_trip), tagged by `source`, never
# merged into a single fabricated row. This is the actual payoff of the
# two-world design locked from the start of this project: comparing live
# operational demand/revenue against the historical NYC market benchmark,
# by the one shared bridge, dim_zone and dim_date.
#
# agg_driver_daily stays World A only. Drivers don't exist in FHVHV data.
# ============================================================================

from pyspark import pipelines as dp
from pyspark.sql.functions import *

CATALOG     = "rideops_ai"
GOLD_SCHEMA = "gold"


# ============================================================================
# agg_zone_hourly: demand pattern by zone + hour, both worlds
# ============================================================================
@dp.table(
    name=f"{CATALOG}.{GOLD_SCHEMA}.agg_zone_hourly",
    comment="Gold: trip demand by zone and hour, operational and historical, tagged by source"
)
def agg_zone_hourly():

    fact_ride = spark.read.table(f"{CATALOG}.{GOLD_SCHEMA}.fact_ride")
    fact_fhvhv = spark.read.table(f"{CATALOG}.{GOLD_SCHEMA}.fact_fhvhv_trip")

    world_a = (
        fact_ride
        .filter(col("final_status") == "completed")
        .withColumn("hour_of_day", hour("requested_at"))
        .groupBy(col("pickup_zone_key").alias("zone_key"), "date_key", "hour_of_day")
        .agg(
            count("*").alias("trip_count"),
            avg("wait_time_sec").alias("avg_wait_time_sec"),
            (sum(when(col("final_status") == "cancelled", 1).otherwise(0)) / count("*")).alias("cancellation_rate"),
            sum("total_fare").alias("total_revenue"),
        )
        .withColumn("source", lit("operational"))
    )

    # FHVHV has no cancellation concept, every row is a completed trip
    world_b = (
        fact_fhvhv
        .groupBy(col("pickup_zone_key").alias("zone_key"), "date_key", col("pickup_hour").alias("hour_of_day"))
        .agg(
            count("*").alias("trip_count"),
            avg("wait_time_sec").alias("avg_wait_time_sec"),
            sum("total_fare").alias("total_revenue"),
        )
        .withColumn("cancellation_rate", lit(None).cast("double"))
        .withColumn("source", lit("fhvhv"))
    )

    return world_a.unionByName(world_b).select(
        "zone_key", "date_key", "hour_of_day", "source",
        "trip_count", "avg_wait_time_sec", "cancellation_rate", "total_revenue",
    )


# ============================================================================
# agg_driver_daily: World A only, drivers don't exist in FHVHV data
# ============================================================================
@dp.table(
    name=f"{CATALOG}.{GOLD_SCHEMA}.agg_driver_daily",
    comment="Gold: driver performance by day, grouped by request date (rides are short enough that request and completion almost always share a calendar day)"
)
def agg_driver_daily():

    fact_ride = spark.read.table(f"{CATALOG}.{GOLD_SCHEMA}.fact_ride")

    return (
        fact_ride
        .filter(col("driver_key").isNotNull())
        .groupBy("driver_key", "date_key")
        .agg(
            sum(when(col("final_status") == "completed", 1).otherwise(0)).alias("completed_rides"),
            sum(when(col("final_status") == "cancelled", 1).otherwise(0)).alias("cancelled_rides"),
            count("*").alias("total_rides"),
            sum("driver_pay").alias("total_earnings"),
            avg(when(col("final_status") == "completed", col("ride_duration_sec"))).alias("avg_ride_duration_sec"),
            avg(when(col("final_status") == "completed", col("total_fare"))).alias("avg_fare_per_ride"),
        )
        .withColumn("completion_rate", col("completed_rides") / col("total_rides"))
    )


# ============================================================================
# agg_revenue_daily: financial breakdown by day + zone, both worlds
# ============================================================================
@dp.table(
    name=f"{CATALOG}.{GOLD_SCHEMA}.agg_revenue_daily",
    comment="Gold: full fare component breakdown by day and zone, operational and historical, tagged by source"
)
def agg_revenue_daily():

    fact_ride = spark.read.table(f"{CATALOG}.{GOLD_SCHEMA}.fact_ride")
    fact_fhvhv = spark.read.table(f"{CATALOG}.{GOLD_SCHEMA}.fact_fhvhv_trip")

    world_a = (
        fact_ride
        .filter(col("final_status") == "completed")
        .groupBy("date_key", col("pickup_zone_key").alias("zone_key"))
        .agg(
            count("*").alias("trip_count"),
            sum("base_passenger_fare").alias("base_fare_total"),
            sum("tolls").alias("tolls_total"),
            sum("bcf").alias("bcf_total"),
            sum("sales_tax").alias("sales_tax_total"),
            sum("congestion_surcharge").alias("congestion_surcharge_total"),
            sum("airport_fee").alias("airport_fee_total"),
            sum("cbd_congestion_fee").alias("cbd_congestion_fee_total"),
            sum("tips").alias("tips_total"),
            sum("driver_pay").alias("driver_pay_total"),
            sum("total_fare").alias("total_revenue"),
        )
        .withColumn("source", lit("operational"))
    )

    world_b = (
        fact_fhvhv
        .groupBy("date_key", col("pickup_zone_key").alias("zone_key"))
        .agg(
            count("*").alias("trip_count"),
            sum("base_passenger_fare").alias("base_fare_total"),
            sum("tolls").alias("tolls_total"),
            sum("bcf").alias("bcf_total"),
            sum("sales_tax").alias("sales_tax_total"),
            sum("congestion_surcharge").alias("congestion_surcharge_total"),
            sum("airport_fee").alias("airport_fee_total"),
            sum("cbd_congestion_fee").alias("cbd_congestion_fee_total"),
            sum("tips").alias("tips_total"),
            sum("driver_pay").alias("driver_pay_total"),
            sum("total_fare").alias("total_revenue"),
        )
        .withColumn("source", lit("fhvhv"))
    )

    return world_a.unionByName(world_b)