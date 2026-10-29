"""
End-to-end test of M12 scoring on a local Spark session:

  small detector -> MLflow package (ml/serving.py) -> mlflow.pyfunc.spark_udf
  -> score_windows / alerts_from_scores (databricks/jobs/score_safety.py)

the same path the Databricks job takes, checked against the detector itself.
Skipped if a local Spark session cannot start.
"""

import importlib.util
import os
import sys
from pathlib import Path

import lightgbm as lgb
import mlflow
import numpy as np
import pandas as pd
import pytest

from ml.detector import MODEL_FEATURES, SafetyDetector, matrix
from ml.pyfunc_model import SafetyDetectorModel
from ml.serving import OUTPUT_DDL

ROOT = Path(__file__).resolve().parents[1]
JOB_FILE = ROOT / "databricks" / "jobs" / "score_safety.py"


@pytest.fixture(scope="module")
def spark():
    os.environ.pop("SPARK_HOME", None)
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    try:
        from pyspark.sql import SparkSession
        session = (SparkSession.builder.master("local[2]").appName("score-safety-tests")
                   .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "2")
                   .config("spark.sql.session.timeZone", "UTC").getOrCreate())
        session.range(1).count()
    except Exception as exc:
        pytest.skip(f"local Spark unavailable: {exc}")
    yield session
    session.stop()


@pytest.fixture(scope="module")
def job():
    spec = importlib.util.spec_from_file_location("score_safety", JOB_FILE)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def windows():
    """3 rides x 8 windows; ride r1 stops for a long time from window 4, r3 ends 2 km short."""
    rows = []
    for ride in ("r1", "r2", "r3"):
        for i in range(8):
            row = {f: 2.5 for f in MODEL_FEATURES}                      # a quiet trip: only the stop changes
            row.update(ride_id=ride, source="live", window_index=i, is_final_window=(i == 7),
                       window_start=pd.Timestamp("2026-05-11 08:00") + pd.Timedelta(minutes=i),
                       implied_speed_max_kmh=30.0, dist_off_route_max_m=8.0, dist_to_dropoff_last_m=10.0,
                       stop_run_max_s=(i - 3) * 60.0 if ride == "r1" and i >= 4 else 0.0)
            if ride == "r3" and i == 7:
                row["dist_to_dropoff_last_m"] = 2000.0
            rows.append(row)
    w = pd.DataFrame(rows)
    w["window_end"] = w["window_start"] + pd.Timedelta(minutes=1)
    return w


@pytest.fixture(scope="module")
def model_uri(tmp_path_factory, windows):
    rng = np.random.default_rng(0)
    train = pd.DataFrame({f: rng.random(3000) * 5 for f in MODEL_FEATURES})
    train["stop_run_max_s"] = rng.exponential(60, 3000)
    booster = lgb.LGBMClassifier(n_estimators=40, num_leaves=7, verbose=-1).fit(
        matrix(train), (train["stop_run_max_s"] > 150).astype(int)).booster_
    folder = tmp_path_factory.mktemp("mlflow")
    detector_dir = folder / "detector"
    SafetyDetector(booster=booster, threshold=0.5, version="test").save(detector_dir)

    mlflow.set_tracking_uri(f"sqlite:///{(folder / 'mlflow.db').as_posix()}")
    with mlflow.start_run():
        info = mlflow.pyfunc.log_model(
            name="detector", python_model=SafetyDetectorModel(), artifacts={"detector": str(detector_dir)},
            code_paths=[str(ROOT / "ml"), str(ROOT / "databricks" / "utilities")],
            input_example=matrix(windows).head(2))
    return info.model_uri


def test_model_travels_to_another_python_and_os(model_uri):
    # Two bugs met on Databricks (Linux, Python 3.12) after publishing from Windows / 3.11:
    #  - a wrapper class pickled BY VALUE carries 3.11 bytecode -> must be pickled by reference
    #  - "models from code" recorded the file as an absolute C:\ path -> must not be used
    local = Path(mlflow.artifacts.download_artifacts(model_uri))
    mlmodel = (local / "MLmodel").read_text()
    assert "model_code_path" not in mlmodel
    pickled = next(local.rglob("python_model.pkl")).read_bytes()
    assert b"ml.pyfunc_model" in pickled and b"_make_skeleton_class" not in pickled
    assert (local / "code" / "ml" / "pyfunc_model.py").exists()


def test_pyfunc_matches_detector(model_uri, windows):
    model = mlflow.pyfunc.load_model(model_uri)
    out = model.predict(matrix(windows))
    direct = SafetyDetector.load(Path(model._model_impl.context.artifacts["detector"])).alerts(windows)
    assert np.allclose(out["risk_score"], direct["risk_score"])
    assert out["alert"].tolist() == direct["alert"].tolist()
    assert (out["model_version"] == "test").all()


def test_spark_scoring_and_one_alert_per_ride_and_type(spark, job, model_uri, windows):
    udf = mlflow.pyfunc.spark_udf(spark, model_uri, result_type=OUTPUT_DDL, env_manager="local")
    scores = job.score_windows(spark.createDataFrame(windows), udf, MODEL_FEATURES).toPandas()
    assert len(scores) == len(windows)

    expected = SafetyDetector.load(Path(mlflow.pyfunc.load_model(model_uri)._model_impl.context.artifacts["detector"]))
    direct = windows[["ride_id", "window_index"]].join(expected.alerts(windows))
    merged = scores.merge(direct, on=["ride_id", "window_index"], suffixes=("", "_direct"))
    assert np.allclose(merged["risk_score"], merged["risk_score_direct"])
    assert (merged["alert"] == merged["alert_direct"]).all()

    alerts = job.alerts_from_scores(spark.createDataFrame(scores)).toPandas()
    assert not alerts.duplicated(["ride_id", "alert_type"]).any()
    assert "wrong_dropoff" in alerts.loc[alerts["ride_id"] == "r3", "alert_type"].tolist()   # deterministic rule
    stop = alerts[(alerts["ride_id"] == "r1") & (alerts["alert_type"] == "unexpected_stop")].iloc[0]
    assert stop["alert_windows"] >= 2
    fired = merged[(merged["ride_id"] == "r1") & merged["alert"] & (merged["alert_type"] == "unexpected_stop")]
    assert stop["detected_window_index"] == fired["window_index"].min()   # first window, not the strongest
