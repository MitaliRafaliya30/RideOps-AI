# Databricks notebook source
# MAGIC %md
# MAGIC # Score rider safety (M12)
# MAGIC
# MAGIC Reads every 60 s window from `silver.sv_ride_live_features`, scores it with the registered
# MAGIC detector `rideops_ai.ml.rideops_safety_detector@champion`, and writes:
# MAGIC
# MAGIC | Table | Grain | Use |
# MAGIC |---|---|---|
# MAGIC | `gold.safety_window_scores` | 1 row per ride per window | risk over time, model monitoring |
# MAGIC | `gold.fact_safety_alert` | 1 row per ride per alert type | the safety team's queue, dashboards |
# MAGIC
# MAGIC Runs after the pipeline (M15 chains them in one Job). Each run recomputes both tables from
# MAGIC Silver, so a re-run gives the same result (idempotent). The model is loaded by alias: promoting
# MAGIC or rolling back a model is an alias change in the registry, never a code change here.

# COMMAND ----------

# MAGIC %pip install lightgbm==4.7.0

# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------

import mlflow
from pyspark.sql import Window
from pyspark.sql import functions as F

CATALOG = "rideops_ai"
MODEL_URI = f"models:/{CATALOG}.ml.rideops_safety_detector@champion"
FEATURES_TABLE = f"{CATALOG}.silver.sv_ride_live_features"
SCORES_TABLE = f"{CATALOG}.gold.safety_window_scores"
ALERTS_TABLE = f"{CATALOG}.gold.fact_safety_alert"

KEYS = ["ride_id", "source", "window_index", "window_start", "window_end", "is_final_window"]
OUTPUT_DDL = ("risk_score DOUBLE, alert BOOLEAN, alert_source STRING, alert_type STRING, "
              "reasons STRING, model_version STRING")


def score_windows(features, predict_udf, feature_names):
    """Every window with the detector's output. Features go in as double, by name."""
    inputs = F.struct(*[F.col(c).cast("double").alias(c) for c in feature_names])
    scored = features.withColumn("_out", predict_udf(inputs)).select(*KEYS, "_out.*")
    # spark_udf turns a missing text value into the strings "nan" / "None":
    # windows without an alert must carry real NULLs, not the word "None"
    for c in ("alert_source", "alert_type", "reasons"):
        scored = scored.withColumn(c, F.when(F.col(c).isin("nan", "NaN", "None", ""), None).otherwise(F.col(c)))
    return scored.withColumn("scored_at", F.current_timestamp())


def alerts_from_scores(scores):
    """
    One alert per ride and alert type: the first window that fired. A stop
    that lasts 10 minutes fires 6+ windows in a row; the safety team needs
    one case, with when it was first detected and how strong it got.
    """
    fired = scores.where("alert")
    first = Window.partitionBy("ride_id", "alert_type").orderBy("window_end")
    whole = Window.partitionBy("ride_id", "alert_type")
    return (
        fired
        .withColumn("max_risk_score", F.max("risk_score").over(whole))
        .withColumn("alert_windows", F.count("*").over(whole))
        .withColumn("_rn", F.row_number().over(first))
        .where("_rn = 1")
        .select(
            F.sha2(F.concat_ws("|", "ride_id", "alert_type"), 256).alias("alert_id"),
            "ride_id", "source", "alert_type", "alert_source",
            F.col("window_end").alias("detected_at"),
            F.col("window_index").alias("detected_window_index"),
            "risk_score", "max_risk_score", "alert_windows", "reasons", "model_version", "scored_at",
        )
    )

# COMMAND ----------

if __name__ == "__main__" and "dbutils" in globals():       # skipped when tests import this file
    mlflow.set_registry_uri("databricks-uc")
    # Resolve the alias to a fixed version first: Spark caches the model package
    # by URI, so scoring "@champion" could reuse an older version after the alias moved
    name = MODEL_URI.split(":/")[1].split("@")[0]
    version = mlflow.MlflowClient().get_model_version_by_alias(name, "champion").version
    model_uri = f"models:/{name}/{version}"
    print(f"scoring with {model_uri}")
    feature_names = mlflow.models.Model.load(model_uri).signature.inputs.input_names()
    predict = mlflow.pyfunc.spark_udf(spark, model_uri, result_type=OUTPUT_DDL, env_manager="local")

    scores = score_windows(spark.read.table(FEATURES_TABLE), predict, feature_names)
    scores.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(SCORES_TABLE)

    alerts = alerts_from_scores(spark.read.table(SCORES_TABLE))
    alerts.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(ALERTS_TABLE)

    display(spark.read.table(ALERTS_TABLE).groupBy("source", "alert_type").count().orderBy("source", "alert_type"))
