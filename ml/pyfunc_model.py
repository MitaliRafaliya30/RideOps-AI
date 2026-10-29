"""
MLflow wrapper for the safety detector: windows in, alerts out.

Lives in its own module on purpose. MLflow pickles the wrapper; a class
pickled from __main__ (python -m ml.serving) is stored BY VALUE, with the
bytecode of the Python that saved it (3.11 on the laptop), which broke on
Databricks' Python 3.12. From a normal module it is stored BY REFERENCE
("import SafetyDetectorModel from ml.pyfunc_model"), and Databricks imports
it from the code shipped inside the model. ("Models from code" would avoid
pickling too, but MLflow on Windows records that file by its absolute
Windows path, which a Linux cluster cannot open.)

The detector itself (LightGBM model.txt + detector.json) is an artifact.
"""

import mlflow
import pandas as pd

OUTPUT_DDL = ("risk_score DOUBLE, alert BOOLEAN, alert_source STRING, alert_type STRING, "
              "reasons STRING, model_version STRING")
OUTPUT_COLUMNS = [c.split()[0] for c in OUTPUT_DDL.split(", ")]


class SafetyDetectorModel(mlflow.pyfunc.PythonModel):

    def load_context(self, context):
        from ml.detector import SafetyDetector
        self.detector = SafetyDetector.load(context.artifacts["detector"])

    def predict(self, context, model_input: pd.DataFrame, params=None) -> pd.DataFrame:
        out = self.detector.alerts(model_input.reset_index(drop=True))
        out["model_version"] = self.detector.version
        out["alert_source"] = out["alert_source"].astype(object)
        return out[OUTPUT_COLUMNS]

