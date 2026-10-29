"""
Package the safety detector as an MLflow model and publish it to a registry.

  python -m ml.serving --target local        local MLflow (sqlite:///mlflow.db), to test the package
  python -m ml.serving --target databricks   Unity Catalog: rideops_ai.ml.rideops_safety_detector

Both set the alias @champion on the new version. The scoring job loads
"models:/<name>@champion", so promoting a new model is one alias change and
rolling back is moving the alias back; the job code never changes.

The package is self-contained: the detector files (LightGBM model.txt +
detector.json), the code it needs (ml/ and the shared utilities/ feature
module), and pinned requirements. Input: one row per 60 s window with the
model features (all as double); output: risk score, alert, type, reasons.

Databricks needs DATABRICKS_HOST and DATABRICKS_TOKEN (a personal access
token) in .env; the catalog schema rideops_ai.ml must exist.
"""

import argparse
import logging
import os

import lightgbm as lgb
import mlflow
import pandas as pd
from dotenv import load_dotenv
from mlflow.models import infer_signature

from geodata.config import REPO_ROOT
from ml.detector import MODEL_FEATURES, SafetyDetector, matrix

logger = logging.getLogger("ml.serving")

LOCAL_MODEL_NAME = "rideops_safety_detector"
UC_MODEL_NAME = "rideops_ai.ml.rideops_safety_detector"
ALIAS = "champion"

OUTPUT_DDL = ("risk_score DOUBLE, alert BOOLEAN, alert_source STRING, alert_type STRING, "
              "reasons STRING, model_version STRING")
OUTPUT_COLUMNS = [c.split()[0] for c in OUTPUT_DDL.split(", ")]


class SafetyDetectorModel(mlflow.pyfunc.PythonModel):
    """MLflow wrapper: windows in, alerts out."""

    def load_context(self, context):
        from ml.detector import SafetyDetector as _Detector     # resolved from the model's own code
        self.detector = _Detector.load(context.artifacts["detector"])

    def predict(self, context, model_input: pd.DataFrame, params=None) -> pd.DataFrame:
        out = self.detector.alerts(model_input.reset_index(drop=True))
        out["model_version"] = self.detector.version
        out["alert_source"] = out["alert_source"].astype(object)
        return out[OUTPUT_COLUMNS]


def _example_input(n_rides: int = 3) -> pd.DataFrame:
    """A few real windows from the local feature cache, as the model expects them (all double)."""
    from ml.config import DEFAULT_RUN, FEATURES_DIR
    from utilities.safety_features import history_features
    windows = pd.read_parquet(FEATURES_DIR / DEFAULT_RUN / "window_features.parquet")
    rides = windows["ride_id"].drop_duplicates().head(n_rides)
    return matrix(history_features(windows[windows["ride_id"].isin(rides)]))


def publish(target: str, detector_dir=None) -> str:
    """Log the packaged detector, register it, point @champion at it. Returns the model URI."""
    from ml.train import DETECTOR_PATH, EXPERIMENT, MODEL_DIR, TRACKING_URI
    detector_dir = detector_dir or DETECTOR_PATH
    detector = SafetyDetector.load(detector_dir)

    if target == "databricks":
        load_dotenv(REPO_ROOT / ".env")
        missing = [v for v in ("DATABRICKS_HOST", "DATABRICKS_TOKEN") if not os.getenv(v)]
        if missing:
            raise SystemExit(f"set {', '.join(missing)} in .env first")
        from databricks.sdk import WorkspaceClient
        user = WorkspaceClient().current_user.me().user_name
        mlflow.set_tracking_uri("databricks")
        mlflow.set_registry_uri("databricks-uc")
        mlflow.set_experiment(f"/Users/{user}/{EXPERIMENT}")
        name = UC_MODEL_NAME
    else:
        mlflow.set_tracking_uri(TRACKING_URI)
        mlflow.set_experiment(EXPERIMENT)
        name = LOCAL_MODEL_NAME

    example = _example_input()
    output = SafetyDetectorModel()
    output.detector = detector
    signature = infer_signature(example, output.predict(None, example))

    with mlflow.start_run(run_name=f"publish {detector.version}"):
        mlflow.log_params({"detector_version": detector.version, "threshold": detector.threshold,
                           "features": len(detector.features)})
        comparison = MODEL_DIR / "comparison.csv"
        if comparison.exists():
            hybrid = pd.read_csv(comparison).query("set == 'holdout' and detector == 'hybrid'")
            if len(hybrid):
                row = hybrid.iloc[0]
                mlflow.log_metrics({f"holdout_{k}": float(row[k]) for k in
                                    ("recall", "precision", "false_alarms_per_1k_trips", "median_delay_s")})
        info = mlflow.pyfunc.log_model(
            name="safety_detector",
            python_model=SafetyDetectorModel(),
            artifacts={"detector": str(detector_dir)},
            code_paths=[str(REPO_ROOT / "ml"), str(REPO_ROOT / "databricks" / "utilities")],
            pip_requirements=[f"lightgbm=={lgb.__version__}", "numpy", "pandas"],
            signature=signature,
            input_example=example.head(5),
            registered_model_name=name,
        )
    version = info.registered_model_version
    mlflow.MlflowClient().set_registered_model_alias(name, ALIAS, version)
    uri = f"models:/{name}@{ALIAS}"
    logger.info("published %s version %s as %s", name, version, uri)
    return uri


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--target", choices=["local", "databricks"], default="local")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    publish(args.target)


if __name__ == "__main__":
    main()
