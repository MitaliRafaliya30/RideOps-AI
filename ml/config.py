"""
Paths and constants for the safety ML work (EDA, baseline, models).

Features are computed with databricks/utilities/safety_features.py, the same
file the Silver pipeline runs, so local training data and live features
can never disagree (training-serving skew).
"""

import sys

from geodata.config import REPO_ROOT

BACKFILL_DIR = REPO_ROOT / "data" / "backfill"
FEATURES_DIR = REPO_ROOT / "data" / "features"      # local feature cache (git-ignored)
IMAGES_DIR = REPO_ROOT / "docs" / "images" / "eda"

DEFAULT_RUN = "run_2026-05-04_7d_50000r_seed42"

# The pipeline root is databricks/: the shared module is imported as utilities.safety_features
_PIPELINE_ROOT = str(REPO_ROOT / "databricks")
if _PIPELINE_ROOT not in sys.path:
    sys.path.insert(0, _PIPELINE_ROOT)

# Time-based split of the 7-day backfill (2026-05-04 .. 2026-05-10):
# never train on the future. Days 1-5 train, day 6 validation, day 7 test.
TRAIN_END = "2026-05-09"     # exclusive
VALID_END = "2026-05-10"     # exclusive; test is everything after
