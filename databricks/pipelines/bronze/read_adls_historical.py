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
#   - SAS token is stored in Databricks Secret Scope
#   - The SAS token is never hardcoded in this source code
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
    # Storage account and container containing the ADF-landed files.
    # ------------------------------------------------------------------------

    storage_account = "strideopsdev"
    container = "rideops"

    # Retrieve the SAS token from the Databricks secret scope.
    # The actual SAS token is NOT stored in the notebook.
    sas_token = dbutils.secrets.get(
        scope="adls-creds",
        key="nyc-fhvhv-sas"
    )

    # ------------------------------------------------------------------------
    # 2. CONFIGURE ADLS SAS AUTHENTICATION
    # ------------------------------------------------------------------------
    # Tell the ABFS driver:
    #   - authentication method = SAS
    #   - use FixedSASTokenProvider
    #   - retrieve the actual SAS token from the secret
    # ------------------------------------------------------------------------

    spark.conf.set(
        f"fs.azure.account.auth.type."
        f"{storage_account}.dfs.core.windows.net",
        "SAS"
    )

    spark.conf.set(
        f"fs.azure.sas.token.provider.type."
        f"{storage_account}.dfs.core.windows.net",
        "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider"
    )

    spark.conf.set(
        f"fs.azure.sas.fixed.token."
        f"{storage_account}.dfs.core.windows.net",
        sas_token
    )

    # ------------------------------------------------------------------------
    # 3. SOURCE PATH
    # ------------------------------------------------------------------------
    # ADF writes the FHVHV parquet files here.
    #
    # Azure structure:
    #
    # strideopsdev
    # └── rideops
    #     └── landing
    #         └── fhvhv
    #             ├── fhvhv_tripdata_2026-05.parquet
    #             ├── ...
    #             └── future files
    #
    # ------------------------------------------------------------------------

    SOURCE_PATH = (
        "abfss://rideops@strideopsdev.dfs.core.windows.net/"
        "landing/fhvhv/"
    )

    # ------------------------------------------------------------------------
    # 4. AUTO LOADER SCHEMA LOCATION
    # ------------------------------------------------------------------------
    # This is a separate directory used by Auto Loader to maintain
    # schema information discovered from the source files.
    #
    # It is deliberately NOT the same as the source data directory.
    # ------------------------------------------------------------------------

    SCHEMA_LOCATION = (
        "abfss://rideops@strideopsdev.dfs.core.windows.net/"
        "_schemas/fhvhv/"
    )

    # ------------------------------------------------------------------------
    # 5. READ USING AUTO LOADER
    # ------------------------------------------------------------------------

    df = (
        spark.readStream
        .format("cloudFiles")

        # Source file format
        .option(
            "cloudFiles.format",
            "parquet"
        )

        # Auto Loader schema metadata/state location
        .option(
            "cloudFiles.schemaLocation",
            SCHEMA_LOCATION
        )

        # Allow newly discovered columns to be added to the schema
        .option(
            "cloudFiles.schemaEvolutionMode",
            "addNewColumns"
        )

        .load(SOURCE_PATH)
    )

    # ------------------------------------------------------------------------
    # 6. BRONZE METADATA + LINEAGE
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
    # 7. RETURN BRONZE DATASET
    # ------------------------------------------------------------------------

    return df