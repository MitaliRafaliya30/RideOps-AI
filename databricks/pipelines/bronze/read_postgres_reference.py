# ============================================================================
# BRONZE LAYER: PostgreSQL Master & Reference Data, landed from ADLS via ADF
# ----------------------------------------------------------------------------
# Bronze responsibilities, and how this file satisfies each one:
#   1. Read from source      -> spark.read against the ADF landing path
#   2. Parse into structured -> Parquet is self-describing, no manual parsing
#      format                   needed (unlike the JSON events, which had no
#                                 embedded schema and needed explicit typing)
#   3. Validate SCHEMA        -> inherent to Parquet: a corrupt/mismatched
#                                 file fails the read outright rather than
#                                 silently landing bad data, which is the
#                                 right failure mode for a typed source
#   4. Store AS-IS             -> only metadata columns are added, no source
#                                  column is renamed, cast, or dropped here
#   5. Add METADATA            -> _ingested_at, _source, _source_table
#   6. Handle ERRORS            -> deliberately NOT quarantined here, see note
#                                  below
#   7. Track LINEAGE            -> _source_file, _file_modified_at
#
# On responsibility 6: no quarantine split for these tables. PK/FK validation
# for master and reference data was already assigned to Silver in the
# locked architecture ("Master data: PK not null, foreign keys resolvable"),
# the same reasoning that kept fhvhv_trips_raw as a single table with no
# split. Bronze here stays a faithful, unfiltered copy of what ADF landed.
# ============================================================================

from pyspark import pipelines as dp
from pyspark.sql.functions import *

# ----------------------------------------------------------------------------
# STORAGE AUTHENTICATION
# Storage account name comes from the pipeline configuration
# (RideOps-Storage-Account). Access is a READ-ONLY SAS token (r + l, with an
# expiry date) in the Databricks secret scope adls-creds, key rideops-adls-sas.
# The auth settings are applied per storage host, and registered inside the
# table function so the account name is read at run time, not at import time.
# ----------------------------------------------------------------------------
container = "rideops"


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


# ----------------------------------------------------------------------------
# METADATA-DRIVEN TABLE LIST
# This is the control list. Add a table name here and it gets its own
# Bronze table automatically, no new function to write.
# ----------------------------------------------------------------------------
REFERENCE_TABLES = [
    "passengers",
    "drivers",
    "vehicles",
    "vehicle_types",
    "membership_tiers",
    "payment_methods",
    "ride_status",
    "cancellation_reasons",
    "zone_lookup",
]


# ----------------------------------------------------------------------------
# TABLE FACTORY
# Builds one Bronze table definition for a given source table name.
# Called once per entry in REFERENCE_TABLES below.
# ----------------------------------------------------------------------------
def _make_bronze_table(table_name: str):

    @dp.table(
        name=f"{table_name}_raw",
        comment=f"Bronze: raw {table_name} snapshot, landed from PostgreSQL via ADF"
    )
    def _bronze_table():
        account = _configure_adls()
        source_path = f"abfss://{container}@{account}.dfs.core.windows.net/reference/{table_name}/"

        return (
            spark.read.format("parquet").load(source_path)
            # lineage (responsibility 7): exact file and when ADF wrote it
            .withColumn("_source_file", col("_metadata.file_path"))
            .withColumn("_file_modified_at", col("_metadata.file_modification_time"))
            # metadata (responsibility 5)
            .withColumn("_ingested_at", current_timestamp())
            .withColumn("_source", lit("postgresql"))
            .withColumn("_source_table", lit(table_name))
        )

    return _bronze_table


# Generate all 9 Bronze tables from the control list above
for _table in REFERENCE_TABLES:
    _make_bronze_table(_table)