# Databricks notebook source
# MAGIC %md
# MAGIC # Build safety Gold (M8)
# MAGIC
# MAGIC Runs right after `score_safety` (pipeline -> score -> this), so every table here reflects
# MAGIC the latest scores. Each run rebuilds all four tables from their inputs (idempotent).
# MAGIC
# MAGIC | Table | Grain | Use |
# MAGIC |---|---|---|
# MAGIC | `gold.dim_incident_type` | 1 row per alert type | meaning, severity, detector, action |
# MAGIC | `gold.fact_ride_safety` | 1 row per tracked ride | risk, first alert, zone, outcome vs ground truth |
# MAGIC | `gold.agg_safety_hourly` | zone x date x hour x source | alert volume and rate for dashboards |
# MAGIC | `gold.ml_model_performance` | model version x date x source | daily model monitoring |
# MAGIC
# MAGIC Ground truth (`silver.sv_safety_labels`) exists only for simulator runs. Live rides get alert
# MAGIC rates, never recall: nobody knows yet which of them were real incidents.

# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql import functions as F

CATALOG = "rideops_ai"
GOLD, SILVER, BRONZE = f"{CATALOG}.gold", f"{CATALOG}.silver", f"{CATALOG}.bronze"

# The week the model was trained on: its scores there are optimistic, and labelled as such
TRAINING_SOURCES = ["run_2026-05-04_7d_50000r_seed42"]
FAR_FUTURE = "9999-12-31"

INCIDENT_TYPES = [
    # alert_type, name, description, severity, detected_by, ground-truth types, recommended action, response target (min)
    ("unexpected_stop", "Unexpected stop", "Car standing still mid-trip for minutes: a long stop or a possible crash",
     "high", "model", "long_stop, possible_crash", "Call the rider; escalate to emergency services if no answer", 2),
    ("route_deviation", "Route deviation", "Car far from the planned route and not closing in on the destination",
     "medium", "model", "route_deviation", "Message the rider; call if the deviation keeps growing", 5),
    ("gps_spoofing", "GPS spoofing", "Physically impossible GPS jump: the phone's position is being faked",
     "medium", "rule", "gps_spoofing", "Flag the driver account for review; check on the rider", 10),
    ("wrong_dropoff", "Wrong drop-off", "Trip ended far from the requested destination",
     "medium", "rule", "wrong_dropoff, possible_crash", "Contact the rider to confirm they are safe", 5),
]


def dim_incident_type(spark):
    columns = ["alert_type", "alert_name", "description", "severity", "detected_by",
               "ground_truth_types", "recommended_action", "response_target_min"]
    df = spark.createDataFrame(INCIDENT_TYPES, columns)
    w = Window.orderBy(F.when(F.col("severity") == "high", 0).otherwise(1), "alert_type")
    return df.withColumn("incident_type_key", F.row_number().over(w)).select("incident_type_key", *columns)


def ride_outcomes(scores, labels):
    """
    Per ride, against ground truth. Same definitions as ml/metrics.py:
      incident window  overlaps [start_ts, end_ts] (no end: until the ride ends);
                       a wrong drop-off only shows on the ride's final window
      caught           an alert on an incident window; delay = first such window_end - start_ts
      false alarm      an alert on a clean window (windows after the incident are not counted)
    """
    l = labels.select("ride_id", "incident_type", "start_ts", "end_ts")
    w = scores.join(l, "ride_id", "left")
    end = F.coalesce(F.col("end_ts"), F.lit(FAR_FUTURE).cast("timestamp"))
    in_incident = F.when(F.col("incident_type").isNull(), F.lit(False)).when(
        F.col("incident_type") == "wrong_dropoff", F.col("is_final_window")
    ).otherwise((F.col("window_start") <= end) & (F.col("window_end") > F.col("start_ts")))
    after = ~in_incident & F.col("end_ts").isNotNull() & (F.col("window_start") > F.col("end_ts"))
    w = w.withColumn("_in", in_incident).withColumn("_after", after)
    return (
        w.groupBy("ride_id")
        .agg(
            F.first("incident_type", ignorenulls=True).alias("true_incident_type"),
            F.first("start_ts", ignorenulls=True).alias("incident_start"),
            F.max(F.col("alert") & F.col("_in")).alias("caught"),
            F.min(F.when(F.col("alert") & F.col("_in"), F.col("window_end"))).alias("_first_hit"),
            F.max(F.col("alert") & ~F.col("_in") & ~F.col("_after")).alias("false_alarm"),
        )
        .withColumn("detection_delay_s", F.greatest(
            F.lit(0.0), F.col("_first_hit").cast("double") - F.col("incident_start").cast("double")))
        .drop("_first_hit")
    )


def nearest_zone(plans, zone_geo, dim_zone):
    """Pickup zone of each ride: nearest of the 263 TLC zone centroids (approximate near zone borders)."""
    geo = zone_geo.select(F.col("LocationID").alias("zone_id"), F.col("lat").alias("z_lat"), F.col("lon").alias("z_lon"))
    d2 = (F.pow(F.col("pickup_lat") - F.col("z_lat"), 2)
          + F.pow((F.col("pickup_lon") - F.col("z_lon")) * F.cos(F.radians(F.col("pickup_lat"))), 2))
    best = Window.partitionBy("ride_id").orderBy(d2)
    return (
        plans.select("ride_id", "pickup_lat", "pickup_lon").crossJoin(F.broadcast(geo))
        .withColumn("_rn", F.row_number().over(best)).where("_rn = 1")
        .join(F.broadcast(dim_zone.select("zone_id", "zone_key")), "zone_id", "left")
        .select("ride_id", F.col("zone_key").alias("pickup_zone_key"))
    )


def fact_ride_safety(scores, alerts, labels, zones):
    labelled_sources = [r["source"] for r in labels.select("source").distinct().collect()]
    per_ride = (
        scores.groupBy("ride_id", "source")
        .agg(
            F.min("window_start").alias("trip_start"),
            F.count("*").alias("minutes_tracked"),
            F.max("risk_score").alias("max_risk_score"),
            F.sum(F.col("alert").cast("int")).alias("alert_windows"),
            F.max("model_version").alias("model_version"),
        )
    )
    first = Window.partitionBy("ride_id").orderBy("detected_at", "alert_type")
    per_alert = (
        alerts.withColumn("_rn", F.row_number().over(first))
        .groupBy("ride_id")
        .agg(
            F.count("*").alias("alert_count"),
            F.min("detected_at").alias("first_alert_at"),
            F.first(F.when(F.col("_rn") == 1, F.col("alert_type")), ignorenulls=True).alias("first_alert_type"),
            F.concat_ws(", ", F.array_sort(F.collect_set("alert_type"))).alias("alert_types"),
        )
    )
    has_truth = F.col("source").isin(labelled_sources) if labelled_sources else F.lit(False)
    # outcome = the ride's main story; false_alarm is kept separately, because an
    # incident ride can ALSO have had an alert before its incident started
    outcome = (F.when(~has_truth, F.lit(None))
               .when(F.col("true_incident_type").isNotNull() & F.col("caught"), "caught")
               .when(F.col("true_incident_type").isNotNull(), "missed")
               .when(F.col("false_alarm"), "false_alarm")
               .otherwise("clean"))
    return (
        per_ride
        .join(per_alert, "ride_id", "left")
        .join(ride_outcomes(scores, labels), "ride_id", "left")
        .join(zones, "ride_id", "left")
        .withColumn("date_key", F.date_format("trip_start", "yyyyMMdd").cast("int"))
        .withColumn("hour_of_day", F.hour("trip_start"))
        .withColumn("alert_count", F.coalesce("alert_count", F.lit(0)))
        .withColumn("has_alert", F.col("alert_count") > 0)
        .withColumn("minutes_to_first_alert",
                    (F.col("first_alert_at").cast("double") - F.col("trip_start").cast("double")) / 60)
        .withColumn("has_ground_truth", has_truth)
        .withColumn("outcome", outcome)
        .withColumn("data_role", F.when(F.col("source") == "live", "live")
                    .when(F.col("source").isin(TRAINING_SOURCES), "training data (optimistic)")
                    .otherwise("unseen simulation"))
        .select("ride_id", "source", "data_role", "date_key", "hour_of_day", "pickup_zone_key", "trip_start",
                "minutes_tracked", "max_risk_score", "has_alert", "alert_count", "alert_windows", "alert_types",
                "first_alert_type", "first_alert_at", "minutes_to_first_alert",
                "has_ground_truth", "true_incident_type", "outcome",
                F.when(F.col("has_ground_truth"), F.coalesce("false_alarm", F.lit(False))).alias("false_alarm"),
                "detection_delay_s", "model_version")
    )


def agg_safety_hourly(ride_safety, alerts):
    by_type = (
        alerts.join(ride_safety.select("ride_id", "pickup_zone_key", "date_key", "hour_of_day", "source"),
                    ["ride_id", "source"])
        .groupBy("pickup_zone_key", "date_key", "hour_of_day", "source")
        .pivot("alert_type", [t[0] for t in INCIDENT_TYPES]).count()
    )
    rides = (
        ride_safety.groupBy("pickup_zone_key", "date_key", "hour_of_day", "source")
        .agg(F.count("*").alias("rides_tracked"),
             F.sum(F.col("has_alert").cast("int")).alias("alerted_rides"),
             F.avg("max_risk_score").alias("avg_max_risk_score"))
    )
    keys = ["pickup_zone_key", "date_key", "hour_of_day", "source"]
    by_type = by_type.select(*[F.col(k).alias(f"_{k}") for k in keys], *[t[0] for t in INCIDENT_TYPES])
    # null-safe: rides whose zone is unknown must still get their alert counts
    out = rides.join(by_type, [rides[k].eqNullSafe(by_type[f"_{k}"]) for k in keys], "left").drop(*[f"_{k}" for k in keys])
    for t in INCIDENT_TYPES:
        out = out.withColumn(t[0], F.coalesce(F.col(t[0]), F.lit(0))).withColumnRenamed(t[0], f"alerts_{t[0]}")
    return out.withColumn("alerted_rides_per_1k", 1000 * F.col("alerted_rides") / F.col("rides_tracked"))


def ml_model_performance(ride_safety):
    """Daily monitoring per model version and source. Quality metrics only where ground truth exists."""
    truth = F.col("has_ground_truth")
    inc = F.col("true_incident_type").isNotNull()
    return (
        ride_safety.groupBy("model_version", "date_key", "source", "data_role")
        .agg(
            F.count("*").alias("rides"),
            F.sum(F.col("has_alert").cast("int")).alias("alerted_rides"),
            F.avg("max_risk_score").alias("avg_max_risk_score"),
            F.max(truth).alias("has_ground_truth"),
            F.sum(F.when(truth & inc, 1).otherwise(0)).alias("incidents"),
            F.sum(F.when(F.col("outcome") == "caught", 1).otherwise(0)).alias("caught"),
            F.sum(F.when(F.col("false_alarm"), 1).otherwise(0)).alias("false_alarm_rides"),
            F.percentile_approx(F.when(F.col("outcome") == "caught", F.col("detection_delay_s")), 0.5)
            .alias("median_detection_delay_s"),
        )
        .withColumn("alerted_rides_per_1k", 1000 * F.col("alerted_rides") / F.col("rides"))
        .withColumn("recall", F.when(F.col("has_ground_truth"), F.col("caught") / F.col("incidents")))
        .withColumn("false_alarms_per_1k", F.when(F.col("has_ground_truth"), 1000 * F.col("false_alarm_rides") / F.col("rides")))
        # share of alerted rides whose incident was caught (same definition as ml/metrics.py)
        .withColumn("precision", F.when(F.col("has_ground_truth") & (F.col("alerted_rides") > 0),
                                        F.col("caught") / F.col("alerted_rides")))
    )

# COMMAND ----------

if __name__ == "__main__" and "dbutils" in globals():       # skipped when tests import this file
    def save(df, name):
        df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{GOLD}.{name}")

    scores = spark.read.table(f"{GOLD}.safety_window_scores")
    alerts = spark.read.table(f"{GOLD}.fact_safety_alert")
    labels = spark.read.table(f"{SILVER}.sv_safety_labels")
    # 263-row static reference read straight from Bronze: nothing to clean, so no Silver copy
    zones = nearest_zone(spark.read.table(f"{SILVER}.sv_route_plans"),
                         spark.read.table(f"{BRONZE}.zone_geo_raw"), spark.read.table(f"{GOLD}.dim_zone"))

    save(dim_incident_type(spark), "dim_incident_type")
    save(fact_ride_safety(scores, alerts, labels, zones), "fact_ride_safety")
    ride_safety = spark.read.table(f"{GOLD}.fact_ride_safety")
    save(agg_safety_hourly(ride_safety, alerts), "agg_safety_hourly")
    save(ml_model_performance(ride_safety), "ml_model_performance")

    display(spark.read.table(f"{GOLD}.ml_model_performance")
            .groupBy("model_version", "source", "data_role")
            .agg(F.sum("rides").alias("rides"), F.sum("incidents").alias("incidents"), F.sum("caught").alias("caught"),
                 F.sum("false_alarm_rides").alias("false_alarm_rides"))
            .withColumn("recall", F.col("caught") / F.col("incidents"))
            .withColumn("false_alarms_per_1k", 1000 * F.col("false_alarm_rides") / F.col("rides")))
