"""
Paths and constants for simulator v2.
"""

from geodata.config import REFERENCE_DIR, REPO_ROOT

# Real NYC trip files (one Parquet per month); glob so new months are picked up
FHVHV_GLOB = (REPO_ROOT / "data" / "fhvhv" / "fhvhv_tripdata_*.parquet").as_posix()
UBER_LICENSE = "HV0003"

# Outputs of calibrate.py
CALIBRATION_DIR = REFERENCE_DIR / "calibration"
OD_MATRIX_CSV = CALIBRATION_DIR / "od_matrix.csv"
SPEED_BY_HOUR_CSV = CALIBRATION_DIR / "speed_by_hour.csv"
DEMAND_BY_HOUR_CSV = CALIBRATION_DIR / "demand_by_hour.csv"
CALIBRATION_SUMMARY_JSON = CALIBRATION_DIR / "summary.json"

# Real trips used for speed calibration must be plausible
MIN_TRIP_SECONDS = 60
MIN_TRIP_MILES = 0.1
MAX_PLAUSIBLE_MPH = 80
