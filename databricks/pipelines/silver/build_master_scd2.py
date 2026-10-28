# ============================================================================
# SILVER LAYER: Master Data with SCD Type 2 History
# ----------------------------------------------------------------------------
# Covers sv_drivers, sv_vehicles, sv_passengers, and sv_quarantine.
#
# Why SCD2, and why create_auto_cdc_from_snapshot_flow specifically:
#   PostgreSQL master data arrives via ADF as a FULL SNAPSHOT every run, not
#   a change-data-feed with insert/update/delete markers. Databricks has two
#   different native CDC APIs for these two different shapes:
#     - create_auto_cdc_flow            -> for a true CDF source
#     - create_auto_cdc_from_snapshot_flow -> for a full-snapshot source
#   Ours is the second case. This function diffs each new snapshot against
#   the last one automatically and writes SCD2 history, no manual merge
#   logic, no manually computed effective_start/effective_end columns.
#
# sv_quarantine is a plain @dp.table (batch, full recompute each run), not a
# streaming table with append_flow, because every source feeding it is a
# batch read of a re-landed Postgres snapshot, not a change stream.
#
# Processing order (dependency-driven):
#   1. sv_drivers    (no FK dependencies)
#   2. sv_vehicles   (FK to drivers -> must come after step 1)
#   3. sv_passengers (FK to M1 reference tables, independent of 1 and 2)
#   4. sv_quarantine (unions invalid rows from all three, so it comes last)
# ============================================================================

from pyspark import pipelines as dp
from pyspark.sql.functions import *

CATALOG       = "rideops_ai"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"


def _quarantine_row(df, failed_rule_col, source_table: str):
    """Format invalid rows into the shared quarantine schema."""
    return df.select(
        lit(source_table).alias("source_table"),
        to_json(struct(*df.columns)).alias("record_json"),
        failed_rule_col.alias("failed_rule"),
        current_timestamp().alias("quarantined_at"),
    )


# ============================================================================
# DRIVERS (no FK dependencies, first in the chain)
# ============================================================================

@dp.view(name="drivers_validated")
def drivers_validated():
    df = spark.read.table(f"{CATALOG}.{BRONZE_SCHEMA}.drivers_raw")

    is_valid = col("driver_id").isNotNull() & col("driver_code").isNotNull()
    reason = when(~is_valid, "null_primary_key").otherwise(None)

    return (
        df
        .withColumn("rating", col("rating").cast("decimal(3,2)"))
        .withColumn("experience_years", col("experience_years").cast("int"))
        .withColumn("join_date", col("join_date").cast("date"))
        .withColumn("_is_valid", is_valid)
        .withColumn("_fail_reason", reason)
    )


@dp.view(name="drivers_clean")
def drivers_clean():
    return (
        spark.read.table("drivers_validated")
        .filter(col("_is_valid"))
        .drop("_is_valid", "_fail_reason")
    )


dp.create_streaming_table(
    name=f"{CATALOG}.{SILVER_SCHEMA}.sv_drivers",
    comment="Silver: drivers with full SCD Type 2 history"
)

dp.create_auto_cdc_from_snapshot_flow(
    target=f"{CATALOG}.{SILVER_SCHEMA}.sv_drivers",
    source="drivers_clean",
    keys=["driver_id"],
    stored_as_scd_type=2,
)


# ============================================================================
# VEHICLES (FK to drivers -> validated against drivers_clean, not Bronze)
# ============================================================================

@dp.view(name="vehicles_validated")
def vehicles_validated():
    df = spark.read.table(f"{CATALOG}.{BRONZE_SCHEMA}.vehicles_raw")

    valid_drivers = spark.read.table("drivers_clean").select(col("driver_id").alias("__vd"))
    valid_types = spark.read.table(f"{CATALOG}.{SILVER_SCHEMA}.sv_vehicle_types").select(
        col("vehicle_type_id").alias("__vt")
    )

    checked = (
        df
        .join(broadcast(valid_drivers), df["driver_id"] == col("__vd"), "left")
        .join(broadcast(valid_types), df["vehicle_type_id"] == col("__vt"), "left")
    )

    is_valid = (
        col("vehicle_id").isNotNull()
        & col("__vd").isNotNull()
        & col("__vt").isNotNull()
    )

    reason = (
        when(col("vehicle_id").isNull(), "null_primary_key")
        .when(col("__vd").isNull(), "unresolvable_driver_id")
        .when(col("__vt").isNull(), "unresolvable_vehicle_type_id")
        .otherwise(None)
    )

    return (
        checked
        .withColumn("manufacture_year", col("manufacture_year").cast("int"))
        .withColumn("last_service_date", col("last_service_date").cast("date"))
        .withColumn("_is_valid", is_valid)
        .withColumn("_fail_reason", reason)
        .drop("__vd", "__vt")
    )


@dp.view(name="vehicles_clean")
def vehicles_clean():
    return (
        spark.read.table("vehicles_validated")
        .filter(col("_is_valid"))
        .drop("_is_valid", "_fail_reason")
    )


dp.create_streaming_table(
    name=f"{CATALOG}.{SILVER_SCHEMA}.sv_vehicles",
    comment="Silver: vehicles with full SCD Type 2 history"
)

dp.create_auto_cdc_from_snapshot_flow(
    target=f"{CATALOG}.{SILVER_SCHEMA}.sv_vehicles",
    source="vehicles_clean",
    keys=["vehicle_id"],
    stored_as_scd_type=2,
)


# ============================================================================
# PASSENGERS (FK to M1 reference tables, independent of drivers/vehicles)
# ============================================================================

@dp.view(name="passengers_validated")
def passengers_validated():
    df = spark.read.table(f"{CATALOG}.{BRONZE_SCHEMA}.passengers_raw")

    valid_tiers = spark.read.table(f"{CATALOG}.{SILVER_SCHEMA}.sv_membership_tiers").select(
        col("membership_tier_id").alias("__tier")
    )
    valid_payments = spark.read.table(f"{CATALOG}.{SILVER_SCHEMA}.sv_payment_methods").select(
        col("payment_method_id").alias("__pay")
    )
    valid_zones = spark.read.table(f"{CATALOG}.{SILVER_SCHEMA}.sv_zone").select(
        col("zone_id").alias("__zone")
    )

    checked = (
        df
        .join(broadcast(valid_tiers), df["membership_tier_id"] == col("__tier"), "left")
        .join(broadcast(valid_payments), df["preferred_payment_method_id"] == col("__pay"), "left")
        .join(broadcast(valid_zones), df["home_zone_id"] == col("__zone"), "left")
    )

    is_valid = (
        col("passenger_id").isNotNull()
        & col("__tier").isNotNull()
        & col("__pay").isNotNull()
        & col("__zone").isNotNull()
    )

    reason = (
        when(col("passenger_id").isNull(), "null_primary_key")
        .when(col("__tier").isNull(), "unresolvable_membership_tier_id")
        .when(col("__pay").isNull(), "unresolvable_payment_method_id")
        .when(col("__zone").isNull(), "unresolvable_home_zone_id")
        .otherwise(None)
    )

    return (
        checked
        .withColumn("signup_date", col("signup_date").cast("date"))
        .withColumn("avg_driver_rating_given", col("avg_driver_rating_given").cast("decimal(3,2)"))
        .withColumn("_is_valid", is_valid)
        .withColumn("_fail_reason", reason)
        .drop("__tier", "__pay", "__zone")
    )


@dp.view(name="passengers_clean")
def passengers_clean():
    return (
        spark.read.table("passengers_validated")
        .filter(col("_is_valid"))
        .drop("_is_valid", "_fail_reason")
    )


dp.create_streaming_table(
    name=f"{CATALOG}.{SILVER_SCHEMA}.sv_passengers",
    comment="Silver: passengers with full SCD Type 2 history"
)

dp.create_auto_cdc_from_snapshot_flow(
    target=f"{CATALOG}.{SILVER_SCHEMA}.sv_passengers",
    source="passengers_clean",
    keys=["passenger_id"],
    stored_as_scd_type=2,
)


# ============================================================================
# sv_quarantine: unions invalid rows from all three sources
# Placed last since it depends on all three _validated views existing.
# ============================================================================

@dp.table(
    name=f"{CATALOG}.{SILVER_SCHEMA}.sv_quarantine",
    comment="Silver: cross-table validation failures from all sources (FK failures, not schema failures, those stay in Bronze quarantine)"
)
def sv_quarantine():
    drivers_bad = spark.read.table("drivers_validated").filter(~col("_is_valid"))
    vehicles_bad = spark.read.table("vehicles_validated").filter(~col("_is_valid"))
    passengers_bad = spark.read.table("passengers_validated").filter(~col("_is_valid"))

    return (
        _quarantine_row(drivers_bad, col("_fail_reason"), "drivers")
        .unionByName(_quarantine_row(vehicles_bad, col("_fail_reason"), "vehicles"))
        .unionByName(_quarantine_row(passengers_bad, col("_fail_reason"), "passengers"))
    )