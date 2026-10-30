"""
Loads configuration from .env
Single place where all settings live, nothing hardcoded elsewhere.
"""

import os
from dotenv import load_dotenv

load_dotenv()

# Databricks connection
DATABRICKS_SERVER_HOSTNAME = os.getenv("DATABRICKS_SERVER_HOSTNAME")
DATABRICKS_HTTP_PATH = os.getenv("DATABRICKS_HTTP_PATH")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")

# Which catalog and schema the assistant is allowed to see
DATABRICKS_CATALOG = os.getenv("DATABRICKS_CATALOG", "rideops_ai")
DATABRICKS_SCHEMA = os.getenv("DATABRICKS_SCHEMA", "gold")

# LLM
OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY")
OPENROUTER_BASE_URL = "https://openrouter.ai/api/v1"
# Any OpenRouter model with tool calling. Default: Claude Haiku (paid, strongest
# here). Set LLM_MODEL in .env to switch, e.g. a free model while out of credits.
LLM_MODEL = os.getenv("LLM_MODEL", "~anthropic/claude-haiku-latest")

# Gold tables the assistant may query.
# Deliberately excludes Bronze and Silver, the assistant only sees
# curated, verified data, never raw or in-progress tables.
ALLOWED_TABLES = [
    "dim_date",
    "dim_zone",
    "dim_passenger",
    "dim_driver",
    "dim_vehicle",
    "dim_payment_method",
    "dim_ride_status",
    "dim_cancellation_reason",
    "fact_ride",
    "fact_ride_event",
    "fact_fhvhv_trip",
    "agg_zone_hourly",
    "agg_driver_daily",
    "agg_revenue_daily",
    # Rider safety (M8, M12, M9): alerts, model outputs, monitoring
    "dim_incident_type",
    "fact_ride_safety",
    "fact_safety_alert",
    "safety_window_scores",
    "agg_safety_hourly",
    "ml_model_performance",
    "dq_check_results",
]


def validate_config():
    """Fail fast if anything required is missing."""
    missing = []
    if not DATABRICKS_SERVER_HOSTNAME:
        missing.append("DATABRICKS_SERVER_HOSTNAME")
    if not DATABRICKS_HTTP_PATH:
        missing.append("DATABRICKS_HTTP_PATH")
    if not DATABRICKS_TOKEN:
        missing.append("DATABRICKS_TOKEN")
    if not OPENROUTER_API_KEY:
        missing.append("OPENROUTER_API_KEY")

    if missing:
        raise ValueError(f"Missing required config: {', '.join(missing)}")

    return True