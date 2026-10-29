# Databricks notebook source
# MAGIC %md
# MAGIC # Data quality checks (M9)
# MAGIC
# MAGIC Last step of the chain: pipeline -> `score_safety` -> `build_safety_gold` -> **this**.
# MAGIC Every check measures one number, compares it with stored thresholds, and gets a status:
# MAGIC `pass`, `warn`, `fail`, or `skipped` (too little data to judge). Results are APPENDED to
# MAGIC `gold.dq_check_results`, so the dashboard shows the quality history, not only today.
# MAGIC
# MAGIC Categories: reconciliation (nothing lost between layers), validity, completeness,
# MAGIC freshness, model inputs (feature drift vs the training week), model outputs.

# COMMAND ----------

import uuid

from pyspark.sql import functions as F

CATALOG = "rideops_ai"
BRONZE, SILVER, GOLD = f"{CATALOG}.bronze", f"{CATALOG}.silver", f"{CATALOG}.gold"
RESULTS_TABLE = f"{GOLD}.dq_check_results"

TRAINING_SOURCE = "run_2026-05-04_7d_50000r_seed42"
DRIFT_FEATURES = ["dist_off_route_max_m", "stop_run_max_s", "speed_mean_kmh", "time_vs_plan_last", "progress_pct_last"]
MIN_WINDOWS_FOR_DRIFT = 1000      # PSI on fewer windows is noise
MIN_RIDES_FOR_ALERT_RATE = 200
ALERT_RATE_BAND = (5.0, 50.0)     # alerted rides per 1k trips; training week: ~18.5

RESULT_DDL = ("check_name STRING, category STRING, table_name STRING, source STRING, value DOUBLE, "
              "warn_threshold DOUBLE, fail_threshold DOUBLE, status STRING, details STRING")


def check(name, category, table, value, warn=None, fail=None, source=None, details="", skip_reason=None):
    """One result row. Higher value = worse; status from the thresholds (warn/fail may be None)."""
    if skip_reason or value is None:
        status, details = "skipped", skip_reason or details
    elif fail is not None and value > fail:
        status = "fail"
    elif warn is not None and value > warn:
        status = "warn"
    else:
        status = "pass"
    as_float = lambda v: None if v is None else float(v)          # the results table stores DOUBLE
    return {"check_name": name, "category": category, "table_name": table, "source": source,
            "value": as_float(value), "warn_threshold": as_float(warn), "fail_threshold": as_float(fail),
            "status": status, "details": details}


def _share(df, condition):
    row = df.agg(F.count("*").alias("n"), F.sum(F.when(condition, 1).otherwise(0)).alias("k")).first()
    return (row["k"] or 0) / row["n"] if row["n"] else None, row["n"]


def psi(reference, current, column, bins=10):
    """Population Stability Index of `current` vs `reference` on decile bins of the reference."""
    probs = [i / bins for i in range(1, bins)]
    edges = sorted(set(reference.where(F.col(column).isNotNull()).approxQuantile(column, probs, 0.001)))
    if not edges:
        return 0.0
    bucket = F.lit(len(edges))
    for i, edge in reversed(list(enumerate(edges))):
        bucket = F.when(F.col(column) <= edge, i).otherwise(bucket)

    def shares(df):
        counts = {r["b"]: r["n"] for r in df.where(F.col(column).isNotNull())
                  .groupBy(bucket.alias("b")).count().withColumnRenamed("count", "n").collect()}
        total = sum(counts.values()) or 1
        return [max(counts.get(b, 0) / total, 1e-6) for b in range(len(edges) + 1)]

    import math
    e, a = shares(reference), shares(current)
    return float(sum((ai - ei) * math.log(ai / ei) for ai, ei in zip(a, e)))


# ----------------------------------------------------------------------------
# Checks, grouped by category. Each takes DataFrames and returns result rows.
# ----------------------------------------------------------------------------
def reconciliation_checks(live_messages, sim_pings, silver_pings, scores, ride_safety, features):
    live = live_messages.where(F.col("message_type") == "gps_ping").select("message_id", "ping_ts")
    bronze = live.unionByName(sim_pings.select("message_id", "ping_ts"))
    bronze_rows = bronze.count()
    valid = bronze.where(F.expr("try_cast(ping_ts AS TIMESTAMP)").isNotNull()).select("message_id").distinct().count()
    silver_rows = silver_pings.count()
    lost = valid - silver_rows
    scored, windows = scores.count(), features.count()
    scored_rides = scores.select("ride_id").distinct().count()
    gold_rides = ride_safety.count()
    return [
        check("gps_pings_bronze_to_silver", "reconciliation", "silver.sv_gps_pings", max(lost, 0) / max(valid, 1),
              warn=0.0, fail=0.001,
              details=f"bronze {bronze_rows:,} rows = {valid:,} unique valid pings + {bronze_rows - valid:,} duplicates or bad timestamps; silver {silver_rows:,}; lost {lost:,}"),
        check("every_window_scored", "reconciliation", "gold.safety_window_scores", 1 - scored / max(windows, 1),
              warn=0.0, fail=0.0, details=f"{scored:,} scored of {windows:,} feature windows (re-run score_safety if behind)"),
        check("gold_rides_equal_scored_rides", "reconciliation", "gold.fact_ride_safety",
              abs(gold_rides - scored_rides), warn=0, fail=0, details=f"{gold_rides:,} in fact_ride_safety vs {scored_rides:,} scored rides"),
    ]


def validity_checks(silver_pings, raw_messages, quarantine):
    jump, n = _share(silver_pings, F.col("is_jump"))
    outside, _ = _share(silver_pings, F.col("is_outside_nyc"))
    gap, _ = _share(silver_pings, F.col("is_gap"))
    q, total = quarantine.count(), raw_messages.count() + quarantine.count()
    return [
        check("gps_jump_share", "validity", "silver.sv_gps_pings", jump, warn=0.001, fail=0.01,
              details=f"impossible jumps (> 250 km/h) in {n:,} pings; spoofing creates a few, a broken feed many"),
        check("gps_outside_nyc_share", "validity", "silver.sv_gps_pings", outside, warn=0.001, fail=0.01),
        check("gps_gap_share", "validity", "silver.sv_gps_pings", gap, warn=0.01, fail=0.05, details="pings > 30 s after the previous one"),
        check("gps_quarantine_rate", "validity", "bronze.gps_messages_quarantine", q / max(total, 1), warn=0.005, fail=0.02,
              details=f"{q:,} of {total:,} live GPS messages broke the contract"),
    ]


def completeness_checks(silver_pings, features, labels):
    no_plan, _ = _share(silver_pings, ~F.col("has_route_plan"))
    no_route, _ = _share(features, F.col("dist_off_route_max_m").isNull())
    labelled = labels.select("ride_id").distinct()
    without_gps = labelled.join(features.select("ride_id").distinct(), "ride_id", "left_anti").count()
    n_labelled = labelled.count()
    return [
        check("pings_without_route_plan", "completeness", "silver.sv_gps_pings", no_plan, warn=0.001, fail=0.01,
              details="route features cannot be computed for these pings"),
        check("windows_missing_route_features", "completeness", "silver.sv_ride_live_features", no_route, warn=0.001, fail=0.01),
        check("labelled_rides_without_gps", "completeness", "silver.sv_safety_labels",
              without_gps / n_labelled if n_labelled else None, warn=0.0, fail=0.01,
              details=f"{without_gps} of {n_labelled} labelled rides have no GPS windows",
              skip_reason=None if n_labelled else "no labels"),
    ]


def freshness_checks(silver_pings, now):
    newest = silver_pings.where(F.col("source") == "live").agg(F.max("ping_ts").alias("t")).first()["t"]
    if newest is None:
        return [check("live_gps_freshness_min", "freshness", "silver.sv_gps_pings", None, skip_reason="no live pings yet")]
    minutes = (now - newest).total_seconds() / 60
    # warn only: the live simulator runs on demand in this project; in production this would page someone
    return [check("live_gps_freshness_min", "freshness", "silver.sv_gps_pings", minutes, warn=60, source="live",
                  details=f"newest live ping {newest}")]


def drift_checks(features):
    reference = features.where(F.col("source") == TRAINING_SOURCE)
    sources = [r["source"] for r in features.select("source").distinct().collect() if r["source"] != TRAINING_SOURCE]
    rows = []
    for source in sorted(sources):
        current = features.where(F.col("source") == source)
        n = current.count()
        for column in DRIFT_FEATURES:
            if n < MIN_WINDOWS_FOR_DRIFT:
                rows.append(check(f"drift_psi_{column}", "model_inputs", "silver.sv_ride_live_features", None, source=source,
                                  skip_reason=f"{n} windows < {MIN_WINDOWS_FOR_DRIFT}: too few to judge drift"))
            else:
                rows.append(check(f"drift_psi_{column}", "model_inputs", "silver.sv_ride_live_features",
                                  psi(reference, current, column), warn=0.1, fail=0.25, source=source,
                                  details=f"PSI vs training week on {n:,} windows"))
    return rows


def output_checks(ride_safety):
    rows = []
    stats = ride_safety.groupBy("source").agg(F.count("*").alias("rides"),
                                              F.sum(F.col("has_alert").cast("int")).alias("alerted")).collect()
    low, high = ALERT_RATE_BAND
    for r in sorted(stats, key=lambda r: r["source"]):
        rate = 1000 * r["alerted"] / r["rides"]
        if r["rides"] < MIN_RIDES_FOR_ALERT_RATE:
            rows.append(check("alerted_rides_per_1k", "model_outputs", "gold.fact_ride_safety", None, source=r["source"],
                              skip_reason=f"{r['rides']} rides < {MIN_RIDES_FOR_ALERT_RATE}: too few to judge"))
            continue
        # outside the expected band either way is suspicious: 0 alerts can mean a silent model
        distance = max(low - rate, rate - high, 0.0)
        rows.append(check("alerted_rides_per_1k", "model_outputs", "gold.fact_ride_safety", distance, warn=0.0,
                          fail=high, source=r["source"],
                          details=f"{rate:.1f} alerted rides per 1k (expected {low:g}-{high:g}); value = distance outside the band"))
    return rows


def run_all(t, now):
    return (reconciliation_checks(t["live_messages"], t["sim_pings"], t["silver_pings"], t["scores"], t["ride_safety"], t["features"])
            + validity_checks(t["silver_pings"], t["live_messages"], t["quarantine"])
            + completeness_checks(t["silver_pings"], t["features"], t["labels"])
            + freshness_checks(t["silver_pings"], now)
            + drift_checks(t["features"])
            + output_checks(t["ride_safety"]))


def to_dataframe(spark, rows, run_id, checked_at):
    return (spark.createDataFrame(rows, RESULT_DDL)
            .withColumn("run_id", F.lit(run_id))
            .withColumn("checked_at", F.lit(checked_at).cast("timestamp")))

# COMMAND ----------

if __name__ == "__main__" and "dbutils" in globals():       # skipped when tests import this file
    from datetime import datetime, timezone

    now = datetime.now(timezone.utc).replace(tzinfo=None)
    tables = {
        "live_messages": spark.read.table(f"{BRONZE}.gps_messages_raw"),
        "quarantine": spark.read.table(f"{BRONZE}.gps_messages_quarantine"),
        "sim_pings": spark.read.table(f"{BRONZE}.sim_gps_pings_raw"),
        "silver_pings": spark.read.table(f"{SILVER}.sv_gps_pings"),
        "features": spark.read.table(f"{SILVER}.sv_ride_live_features"),
        "labels": spark.read.table(f"{SILVER}.sv_safety_labels"),
        "scores": spark.read.table(f"{GOLD}.safety_window_scores"),
        "ride_safety": spark.read.table(f"{GOLD}.fact_ride_safety"),
    }
    results = to_dataframe(spark, run_all(tables, now), str(uuid.uuid4()), now)
    results.write.mode("append").option("mergeSchema", "true").saveAsTable(RESULTS_TABLE)

    display(results.select("status", "category", "check_name", "source", F.round("value", 4).alias("value"),
                           "warn_threshold", "fail_threshold", "details")
            .orderBy(F.expr("CASE status WHEN 'fail' THEN 0 WHEN 'warn' THEN 1 WHEN 'pass' THEN 2 ELSE 3 END"), "category"))
