# ============================================================================
# BRONZE LAYER: simulator backfill (training data) + safety ground truth
#               + zone geography
# ----------------------------------------------------------------------------
# Source: ADLS Gen2, landed by Azure Data Factory (PL_INGEST_SIM_BACKFILL):
#
#   rideops/landing/sim/<run>/gps_pings/*.parquet       -> sim_gps_pings_raw
#   rideops/landing/sim/<run>/route_plans/*.parquet     -> sim_route_plans_raw
#   rideops/labels/safety/<run>/incident_labels/        -> safety_labels_raw
#   rideops/labels/safety/<run>/behaviors/              -> safety_behaviors_raw
#   rideops/reference/zone_geo/zone_centroids.csv       -> zone_geo_raw
#
# Labels sit in their own folder and their own tables (ADR-010): the live
# stream never carries them, and the stream tables never join to them
# except in the ML training set.
#
# Metadata-driven, like read_postgres_reference.py: add a row to SIM_TABLES
# and a new Bronze table appears. The "*" in the path matches every run
# folder, so a new backfill run is picked up on the next pipeline update.
#
# ride_events of the backfill are landed but not ingested here: training the
# safety models needs GPS and labels only, and ingesting them would double
# the operational ride facts.
#
# Authentication: read-only SAS token in the Databricks secret scope
# "adls-creds". Databricks Free Edition has no Azure managed identity; on
# Azure Databricks this becomes a Unity Catalog external location.
# ============================================================================

from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp, lit, regexp_extract

CONTAINER = "rideops"

# (bronze table, path under the container, file format, description)
SIM_TABLES = [
    ("sim_gps_pings_raw",       "landing/sim/*/gps_pings/",           "GPS pings of simulator backfill runs"),
    ("sim_route_plans_raw",     "landing/sim/*/route_plans/",         "route plans of simulator backfill runs"),
    ("safety_labels_raw",       "labels/safety/*/incident_labels/",   "safety incident ground truth (never on the stream)"),
    ("safety_behaviors_raw",    "labels/safety/*/behaviors/",         "every simulated behaviour, incidents and normal, for error analysis"),
]


def _configure_adls() -> str:
    """Register the read-only SAS token for the storage account and return its name."""
    account = spark.conf.get("RideOps-Storage-Account", "strideopsdev1")
    host = f"{account}.dfs.core.windows.net"
    sas_token = dbutils.secrets.get(scope="adls-creds", key="rideops-adls-sas")

    spark.conf.set(f"fs.azure.account.auth.type.{host}", "SAS")
    spark.conf.set(f"fs.azure.sas.token.provider.type.{host}",
                   "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
    spark.conf.set(f"fs.azure.sas.fixed.token.{host}", sas_token)
    return account


def _with_lineage(df, source: str):
    return (
        df
        .withColumn("_source_file", col("_metadata.file_path"))
        .withColumn("_file_modified_at", col("_metadata.file_modification_time"))
        # which backfill run produced the row: .../sim/<run_id>/... or .../safety/<run_id>/...
        .withColumn("_run_id", regexp_extract(col("_metadata.file_path"), r"/(run_[^/]+)/", 1))
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source", lit(source))
    )


def _make_sim_table(table_name: str, path: str, description: str):

    @dp.table(
        name=table_name,
        comment=f"Bronze: {description}, landed from the simulator via ADF and Auto Loader"
    )
    def _table():
        account = _configure_adls()
        source = f"abfss://{CONTAINER}@{account}.dfs.core.windows.net/{path}"

        # No schemaLocation / checkpoint options: Lakeflow manages Auto Loader
        # state itself, so the token never needs write access to the container.
        df = (
            spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "parquet")
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
            .option("pathGlobFilter", "*.parquet")
            .load(source)
        )
        return _with_lineage(df, "simulator_backfill")

    return _table


for _name, _path, _description in SIM_TABLES:
    _make_sim_table(_name, _path, _description)


# ----------------------------------------------------------------------------
# Zone geography: 263 zones, one row each. Small and static, so a plain batch
# read, same pattern as the Postgres reference tables.
# Upload once: data/reference/zone_centroids.csv -> rideops/reference/zone_geo/
# ----------------------------------------------------------------------------
@dp.table(
    name="zone_geo_raw",
    comment="Bronze: NYC taxi zone centroids (lat/lon) built from the official TLC zone shapes"
)
def zone_geo_raw():
    account = _configure_adls()
    path = f"abfss://{CONTAINER}@{account}.dfs.core.windows.net/reference/zone_geo/"
    # Explicit schema: no inferSchema, which reads the file eagerly just to guess the types
    df = (
        spark.read.format("csv")
        .option("header", "true")
        .schema("LocationID INT, borough STRING, zone STRING, lat DOUBLE, lon DOUBLE")
        .load(path)
    )
    return (
        df
        .withColumn("_source_file", col("_metadata.file_path"))
        .withColumn("_file_modified_at", col("_metadata.file_modification_time"))
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source", lit("tlc_zone_shapes"))
    )
