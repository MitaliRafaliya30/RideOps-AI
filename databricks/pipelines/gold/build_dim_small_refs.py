# ============================================================================
# GOLD LAYER: Small Conformed Dimensions
# ----------------------------------------------------------------------------
# dim_payment_method, dim_ride_status, dim_cancellation_reason.
#
# Kept separate from dim_passenger/dim_vehicle (unlike membership_tier and
# vehicle_type in G2), because these three are referenced by FACTS directly,
# not folded into a single parent dimension. Multiple consumers each need
# their own conformed dimension to join against.
#
# One metadata-driven loop, not three near-identical functions, same
# reasoning as Silver's reference table build: identical transformation
# shape (read Silver table, add surrogate key, select), so it's defined
# once and applied to each entry in the control list.
# ============================================================================

from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.window import Window

CATALOG       = "rideops_ai"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA   = "gold"

# (silver_table, gold_table, natural_key, columns_to_carry)
SMALL_DIMS = [
    ("sv_payment_methods",     "dim_payment_method",     "payment_method_id",     ["payment_method_id", "payment_method_name"]),
    ("sv_ride_status",         "dim_ride_status",         "ride_status_id",        ["ride_status_id", "ride_status_name"]),
    ("sv_cancellation_reasons","dim_cancellation_reason", "cancellation_reason_id",["cancellation_reason_id", "reason_category"]),
]


def _make_small_dim(silver_table: str, gold_table: str, natural_key: str, columns: list):

    surrogate_key = gold_table.replace("dim_", "") + "_key"

    @dp.table(
        name=f"{CATALOG}.{GOLD_SCHEMA}.{gold_table}",
        comment=f"Gold: conformed {gold_table.replace('dim_', '')} dimension"
    )
    def _table():
        df = spark.read.table(f"{CATALOG}.{SILVER_SCHEMA}.{silver_table}")
        w = Window.orderBy(natural_key)

        return (
            df
            .withColumn(surrogate_key, row_number().over(w))
            .select(surrogate_key, *columns)
        )

    return _table


for silver_tbl, gold_tbl, nk, cols in SMALL_DIMS:
    _make_small_dim(silver_tbl, gold_tbl, nk, cols)