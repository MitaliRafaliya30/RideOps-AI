# ============================================================================
# GOLD LAYER: Calendar and Zone Dimensions
# ----------------------------------------------------------------------------
# dim_date:  range derived from the actual min/max across both fact sources,
#            not hardcoded, plus a 90-day forward buffer so the continuously
#            running event stream never outgrows it between rebuilds.
# dim_zone:  the shared bridge dimension, carried forward from Silver's
#            sv_zone, now with a Gold-owned surrogate key.
# ============================================================================

from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.window import Window

CATALOG       = "rideops_ai"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA   = "gold"


@dp.table(
    name=f"{CATALOG}.{GOLD_SCHEMA}.dim_date",
    comment="Gold: calendar dimension, range derived from fact data plus a 90-day forward buffer"
)
def dim_date():
    fhvhv_dates = (
        spark.read.table(f"{CATALOG}.{SILVER_SCHEMA}.sv_fhvhv_trips")
        .select(col("pickup_date").alias("d"))
    )
    event_dates = (
        spark.read.table(f"{CATALOG}.{SILVER_SCHEMA}.sv_ride_events")
        .select(to_date(col("event_timestamp")).alias("d"))
    )

    bounds = (
        fhvhv_dates.union(event_dates)
        .agg(
            min("d").alias("min_d"),
            greatest(max("d"), date_add(current_date(), 90)).alias("max_d"),
        )
    )

    date_range = bounds.select(
        explode(sequence(col("min_d"), col("max_d"), expr("interval 1 day"))).alias("full_date")
    )

    return (
        date_range
        .withColumn("date_key", (year("full_date") * 10000 + month("full_date") * 100 + dayofmonth("full_date")).cast("int"))
        .withColumn("year", year("full_date"))
        .withColumn("quarter", quarter("full_date"))
        .withColumn("month", month("full_date"))
        .withColumn("month_name", date_format("full_date", "MMMM"))
        .withColumn("day_of_month", dayofmonth("full_date"))
        .withColumn("day_of_week", dayofweek("full_date"))
        .withColumn("day_name", date_format("full_date", "EEEE"))
        .withColumn("week_of_year", weekofyear("full_date"))
        .withColumn("is_weekend", dayofweek("full_date").isin(1, 7))
        .select(
            "date_key", "full_date", "year", "quarter", "month", "month_name",
            "day_of_month", "day_of_week", "day_name", "week_of_year", "is_weekend",
        )
    )


@dp.table(
    name=f"{CATALOG}.{GOLD_SCHEMA}.dim_zone",
    comment="Gold: NYC zone dimension, the shared bridge between operational and historical facts"
)
def dim_zone():
    df = spark.read.table(f"{CATALOG}.{SILVER_SCHEMA}.sv_zone")

    w = Window.orderBy("zone_id")

    return (
        df
        .withColumn("zone_key", row_number().over(w))
        .select("zone_key", "zone_id", "borough", "zone_name")
    )