# ============================================================================
# SILVER LAYER: Reference & Conformed Dimensions
# ----------------------------------------------------------------------------
# Covers Silver Step 1 (type/standardize) and Step 2 (dedupe) for the small,
# slow-changing reference tables plus sv_zone, the one dimension that bridges
# World A (operational) and World B (historical).
#
# These tables have NO foreign keys pointing outward, so there is nothing to
# validate here, no quarantine wiring needed at this milestone. That starts
# in M2, once sv_passengers/drivers/vehicles need something to check against.
# ============================================================================

from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.window import Window

CATALOG        = "rideops_ai"
BRONZE_SCHEMA  = "bronze"
SILVER_SCHEMA  = "silver"

# ----------------------------------------------------------------------------
# METADATA-DRIVEN TABLE LIST
# Every entry here is a pure reference table: read, cast, dedupe, done.
# Add a row, get a Silver table, no new function needed.
# ----------------------------------------------------------------------------
REFERENCE_TABLES = [
    # (bronze_table,             silver_table,          primary_key)
    ("vehicle_types_raw",        "sv_vehicle_types",      "vehicle_type_id"),
    ("membership_tiers_raw",     "sv_membership_tiers",   "membership_tier_id"),
    ("payment_methods_raw",      "sv_payment_methods",    "payment_method_id"),
    ("ride_status_raw",          "sv_ride_status",        "ride_status_id"),
    ("cancellation_reasons_raw", "sv_cancellation_reasons","cancellation_reason_id"),
]


def _make_reference_table(bronze_table: str, silver_table: str, primary_key: str):

    @dp.table(
        name=f"{CATALOG}.{SILVER_SCHEMA}.{silver_table}",
        comment=f"Silver: cleaned, deduped {silver_table}"
    )
    def _table():
        df = spark.read.table(f"{CATALOG}.{BRONZE_SCHEMA}.{bronze_table}")

        # ---- dedupe: keep the latest row per PK by ingest time ----
        # A window function, not a groupBy, because we need to keep every
        # column of the winning row, not just aggregate one.
        w = Window.partitionBy(primary_key).orderBy(col("_ingested_at").desc())

        deduped = (
            df
            .withColumn("_rn", row_number().over(w))
            .filter(col("_rn") == 1)
            .drop("_rn")
        )

        return (
            deduped
            # keep lineage, drop the noisier Bronze audit columns
            .withColumn("_silver_processed_at", current_timestamp())
            .drop("_ingested_at", "_source_table", "_source_file", "_file_modified_at")
        )

    return _table


for bronze_tbl, silver_tbl, pk in REFERENCE_TABLES:
    _make_reference_table(bronze_tbl, silver_tbl, pk)


# ----------------------------------------------------------------------------
# sv_zone: the conformed dimension bridging World A and World B
# ----------------------------------------------------------------------------
# Not folded into the generic loop above because it carries the one piece of
# validation that actually matters project-wide: every zone id referenced by
# either fact source must resolve here, or cross-world comparisons silently
# break. Kept as its own explicit table for that reason.
# ----------------------------------------------------------------------------
@dp.table(
    name=f"{CATALOG}.{SILVER_SCHEMA}.sv_zone",
    comment="Silver: cleaned NYC zone lookup, the shared dimension between operational and historical data"
)
@dp.expect_or_drop("valid_zone_id", "zone_id IS NOT NULL AND zone_id > 0")
def sv_zone():
    df = spark.read.table(f"{CATALOG}.{BRONZE_SCHEMA}.zone_lookup_raw")

    w = Window.partitionBy("zone_id").orderBy(col("_ingested_at").desc())

    deduped = (
        df
        .withColumn("_rn", row_number().over(w))
        .filter(col("_rn") == 1)
        .drop("_rn")
    )

    return (
        deduped
        .withColumn("borough", trim(col("borough")))
        .withColumn("zone_name", trim(col("zone_name")))
        .withColumn("_silver_processed_at", current_timestamp())
        .drop("_ingested_at", "_source_table", "_source_file", "_file_modified_at")
    )