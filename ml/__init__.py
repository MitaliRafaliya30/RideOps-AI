"""
Rider-safety ML: EDA, baseline, models and serving.

Features come from databricks/utilities/safety_features.py, imported as
`utilities.safety_features` exactly like the Silver pipeline does. In the
repo that folder is put on the path here; inside a logged MLflow model the
folder ships next to this package, so the import works there too.
"""

import sys
from pathlib import Path

_PIPELINE_ROOT = Path(__file__).resolve().parents[1] / "databricks"
if _PIPELINE_ROOT.is_dir() and str(_PIPELINE_ROOT) not in sys.path:
    sys.path.insert(0, str(_PIPELINE_ROOT))
