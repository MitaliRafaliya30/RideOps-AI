# ============================================================================
# BRONZE LAYER: NYC FHVHV Historical Trips
# ============================================================================
# Source:
#   Azure Data Lake Storage Gen2 (ADLS)
#   Data is landed into ADLS by Azure Data Factory (ADF)
#
# Bronze responsibilities:
#   1. Read raw FHVHV files from ADLS
#   2. Ingest files incrementally using Auto Loader
#   3. Preserve the source data without business transformations
#   4. Add ingestion and file-level lineage metadata
#
# Auto Loader:
#   - Uses cloudFiles to discover and process files incrementally
#   - Tracks discovered files using streaming state/checkpoints
#   - Automatically detects new files added to the source directory
#   - Maintains schema information in the configured schema location
#
# Authentication:
#   - Read-only SAS token in Databricks secret scope adls-creds (key rideops-adls-sas)
#   - Never hardcoded in this source code
# ============================================================================

from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp, lit


# ============================================================================
# BRONZE STREAMING TABLE
# ============================================================================

@dp.table(
    name="fhvhv_trips_raw",
    comment=(
        "Bronze: raw NYC FHVHV trip records ingested from "
        "ADLS Gen2 via Azure Data Factory and Auto Loader"
    ),
    table_properties={
        "delta.feature.timestampNtz": "supported"
    }
)
def fhvhv_trips_raw():

    # ------------------------------------------------------------------------
    # 1. ADLS CONFIGURATION
    # ------------------------------------------------------------------------
    # The storage account name comes from the pipeline configuration
    # (RideOps-Storage-Account), so moving to another account is a settings
    # change, not a code change. Access is a READ-ONLY SAS token (permissions
    # r + l, with an expiry date) kept in the Databricks secret scope.
    # Databricks Free Edition has no Azure managed identity; on Azure
    # Databricks this becomes a Unity Catalog external location.
    # ------------------------------------------------------------------------

    storage_account = spark.conf.get("RideOps-Storage-Account", "strideopsdev1")
    container = "rideops"
    host = f"{storage_account}.dfs.core.windows.net"

    sas_token = dbutils.secrets.get(scope="adls-creds", key="rideops-adls-sas")

    spark.conf.set(f"fs.azure.account.auth.type.{host}", "SAS")
    spark.conf.set(f"fs.azure.sas.token.provider.type.{host}",
                   "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
    spark.conf.set(f"fs.azure.sas.fixed.token.{host}", sas_token)

    # ------------------------------------------------------------------------
    # 2. SOURCE PATH
    # ------------------------------------------------------------------------
    # ADF writes the FHVHV parquet files here:
    #   <account> / rideops / landing / fhvhv / fhvhv_tripdata_2026-05.parquet
    # ------------------------------------------------------------------------

    SOURCE_PATH = f"abfss://{container}@{host}/landing/fhvhv/"

    # ------------------------------------------------------------------------
    # 3. READ USING AUTO LOADER
    # ------------------------------------------------------------------------

    df = (
        spark.readStream
        .format("cloudFiles")

        # Source file format
        .option(
            "cloudFiles.format",
            "parquet"
        )

        # No schemaLocation option: Lakeflow manages Auto Loader schema state
        # itself, so the read-only token never needs write access.

        # Allow newly discovered columns to be added to the schema
        .option(
            "cloudFiles.schemaEvolutionMode",
            "addNewColumns"
        )

        .load(SOURCE_PATH)
    )

    # ------------------------------------------------------------------------
    # 4. BRONZE METADATA + LINEAGE
    # ------------------------------------------------------------------------
    # _metadata is provided by Auto Loader for file-level lineage.
    #
    # These columns tell us:
    #   _source_file       -> exact source file
    #   _file_modified_at  -> source file modification time
    #   _ingested_at       -> when Databricks ingested the record
    #   _source            -> source system
    # ------------------------------------------------------------------------

    df = (
        df
        .withColumn(
            "_source_file",
            col("_metadata.file_path")
        )
        .withColumn(
            "_file_modified_at",
            col("_metadata.file_modification_time")
        )
        .withColumn(
            "_ingested_at",
            current_timestamp()
        )
        .withColumn(
            "_source",
            lit("adls_adf_batch")
        )
    )

    # ------------------------------------------------------------------------
    # 5. RETURN BRONZE DATASET
    # ------------------------------------------------------------------------

    return df