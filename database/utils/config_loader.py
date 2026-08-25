"""
Configuration loader for RideOps AI Event Simulator

Hierarchy (highest to lowest precedence):
1. CLI arguments
2. Environment variables
3. YAML config file
4. Hardcoded defaults
"""

import os
import sys
import yaml
import logging
from pathlib import Path
from typing import Dict, Any, Optional
from dotenv import load_dotenv

logger = logging.getLogger(__name__)


# ==================== DEFAULT CONFIGURATION ====================

DEFAULT_CONFIG = {
    "simulator": {
        "duration_seconds": 3600,
        "rate_rides_per_second": 10,
        "seed": None,
    },
    "database": {
        "host": "localhost",
        "port": 5432,
        "user": "postgres",
        "password": "password",
        "database": "rideops_db",
        "connection_pool_size": 10,
        "connection_timeout_seconds": 30,
    },
    "defects": {
        "enabled": True,
        "defect_1": {"enabled": True, "rate": 0.05, "severity": "low"},
        "defect_2": {"enabled": True, "rate": 0.03, "severity": "low"},
        "defect_3": {"enabled": True, "rate": 0.02},
        "defect_4": {"enabled": True, "rate": 0.03, "severity": "low"},
        "defect_5": {"enabled": True, "rate": 0.02, "severity": "low"},
        "defect_6": {"enabled": True, "rate": 0.01},
        "defect_7": {"enabled": True, "rate": 0.04, "severity": "low"},
        "defect_8": {"enabled": True, "rate": 0.03, "severity": "low"},
    },
    "generation": {
        "completion_rate": 0.85,
    },
    "logging": {
        "log_level": "INFO",
        "log_file": "logs/simulator.log",
        "log_format": "json",
        "console_output": True,
        "console_level": "INFO",
    },
    "control": {
        "graceful_shutdown_timeout_seconds": 30,
        "fail_on_first_error": False,
        "max_consecutive_errors": 100,
    },
}


# ==================== CONFIGURATION LOADING ====================

def load_dotenv_vars():
    """Load environment variables from .env file"""
    load_dotenv()
    logger.info("Environment variables loaded from .env")


def find_config_file(config_path: Optional[str] = None) -> Optional[Path]:
    """
    Find configuration file in standard locations
    
    Search order:
    1. Explicit path (if provided)
    2. ./config/simulator_config.yaml
    3. /etc/rideops/simulator_config.yaml
    4. ~/.rideops/config.yaml
    """
    
    locations = []
    
    if config_path:
        locations.append(Path(config_path))
    
    locations.extend([
        Path("config/simulator_config.yaml"),
        Path("/etc/rideops/simulator_config.yaml"),
        Path.home() / ".rideops" / "config.yaml",
    ])
    
    for location in locations:
        if location.exists():
            logger.info(f"Found config file: {location}")
            return location
    
    logger.warning("No config file found in standard locations")
    return None


def load_yaml_config(config_path: Path) -> Dict[str, Any]:
    """Load YAML configuration file"""
    try:
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        logger.info(f"Loaded YAML config from {config_path}")
        return config or {}
    except Exception as e:
        logger.error(f"Failed to load config file {config_path}: {e}")
        raise


def expand_env_vars(config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Recursively expand ${VAR} environment variables in config
    
    Example: ${POSTGRES_PASSWORD} → value from env var POSTGRES_PASSWORD
    """
    
    def expand_value(value):
        if isinstance(value, str):
            # Replace ${VAR} with environment variable
            if value.startswith("${") and value.endswith("}"):
                var_name = value[2:-1]
                env_value = os.getenv(var_name)
                if env_value is None:
                    logger.warning(f"Environment variable not set: {var_name}")
                    return value
                return env_value
            return value
        elif isinstance(value, dict):
            return {k: expand_value(v) for k, v in value.items()}
        elif isinstance(value, list):
            return [expand_value(v) for v in value]
        else:
            return value
    
    return expand_value(config)


def deep_merge(base: Dict, override: Dict) -> Dict:
    """
    Deep merge override dict into base dict
    Override values take precedence
    """
    result = base.copy()
    
    for key, value in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = deep_merge(result[key], value)
        else:
            result[key] = value
    
    return result


def parse_cli_args(args: list) -> Dict[str, Any]:
    """Parse CLI arguments into config dict"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="RideOps AI Event Simulator",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python simulator.py                          # Run default scenario
  python simulator.py clean                    # Run clean scenario
  python simulator.py stress                   # Run stress test
  python simulator.py --duration=7200 --rate=20  # Custom settings
  python simulator.py --defects-enabled=false # Disable all defects
        """
    )
    
    # Positional argument: scenario
    parser.add_argument(
        "scenario",
        nargs="?",
        choices=["clean", "default", "stress", "demo"],
        help="Preset scenario to run"
    )
    
    # Simulator options
    parser.add_argument("--config", help="Config file path")
    parser.add_argument("--duration", type=int, help="Duration in seconds")
    parser.add_argument("--rate", type=int, help="Rides per second")
    parser.add_argument("--seed", type=int, help="RNG seed for reproducibility")
    
    # Defects
    parser.add_argument("--defects-enabled", type=lambda x: x.lower() == 'true', 
                       help="Enable all defects (true/false)")
    parser.add_argument("--defect-1-rate", type=float, help="Defect 1 rate (0-1)")
    parser.add_argument("--defect-1-severity", help="Defect 1 severity (low/med/high)")
    parser.add_argument("--defect-2-rate", type=float, help="Defect 2 rate")
    parser.add_argument("--defect-2-severity", help="Defect 2 severity")
    parser.add_argument("--defect-3-rate", type=float, help="Defect 3 rate")
    parser.add_argument("--defect-4-rate", type=float, help="Defect 4 rate")
    parser.add_argument("--defect-4-severity", help="Defect 4 severity")
    parser.add_argument("--defect-5-rate", type=float, help="Defect 5 rate")
    parser.add_argument("--defect-5-severity", help="Defect 5 severity")
    parser.add_argument("--defect-6-rate", type=float, help="Defect 6 rate")
    parser.add_argument("--defect-7-rate", type=float, help="Defect 7 rate")
    parser.add_argument("--defect-7-severity", help="Defect 7 severity")
    parser.add_argument("--defect-8-rate", type=float, help="Defect 8 rate")
    parser.add_argument("--defect-8-severity", help="Defect 8 severity")
    
    # Logging
    parser.add_argument("--log-level", help="Log level (DEBUG/INFO/WARN/ERROR)")
    parser.add_argument("--log-file", help="Log file path")
    
    # Help
    parser.add_argument("--version", "-v", action="version", version="RideOps AI Simulator v1.0.0")
    
    parsed = parser.parse_args(args)
    
    # Convert to nested dict, excluding None values
    config = {}
    
    if parsed.scenario:
        config["_scenario"] = parsed.scenario
    
    if parsed.config:
        config["_config_file"] = parsed.config
    
    if parsed.duration:
        config.setdefault("simulator", {})["duration_seconds"] = parsed.duration
    
    if parsed.rate:
        config.setdefault("simulator", {})["rate_rides_per_second"] = parsed.rate
    
    if parsed.seed is not None:
        config.setdefault("simulator", {})["seed"] = parsed.seed
    
    if parsed.defects_enabled is not None:
        config.setdefault("defects", {})["enabled"] = parsed.defects_enabled
    
    # Defect rates
    for i in range(1, 9):
        defect_key = f"defect_{i}"
        rate_arg = getattr(parsed, f"defect_{i}_rate", None)
        severity_arg = getattr(parsed, f"defect_{i}_severity", None)
        
        if rate_arg is not None or severity_arg is not None:
            config.setdefault("defects", {}).setdefault(defect_key, {})
            if rate_arg is not None:
                config["defects"][defect_key]["rate"] = rate_arg
            if severity_arg is not None:
                config["defects"][defect_key]["severity"] = severity_arg
    
    if parsed.log_level:
        config.setdefault("logging", {})["log_level"] = parsed.log_level.upper()
    
    if parsed.log_file:
        config.setdefault("logging", {})["log_file"] = parsed.log_file
    
    return config


def validate_config(config: Dict[str, Any]) -> bool:
    """Validate configuration values"""
    
    errors = []
    
    # Duration
    if config.get("simulator", {}).get("duration_seconds", 0) <= 0:
        errors.append("simulator.duration_seconds must be > 0")
    
    # Rate
    rate = config.get("simulator", {}).get("rate_rides_per_second", 0)
    if rate <= 0 or rate > 1000:
        errors.append("simulator.rate_rides_per_second must be between 1 and 1000")
    
    # Defect rates (0-1)
    for i in range(1, 9):
        defect = config.get("defects", {}).get(f"defect_{i}", {})
        rate = defect.get("rate")
        if rate is not None and not (0 <= rate <= 1):
            errors.append(f"defects.defect_{i}.rate must be between 0 and 1")
    
    # Completion rate
    completion = config.get("generation", {}).get("completion_rate")
    if completion is not None and not (0 <= completion <= 1):
        errors.append("generation.completion_rate must be between 0 and 1")
    
    # Log level
    log_level = config.get("logging", {}).get("log_level", "").upper()
    if log_level and log_level not in ["DEBUG", "INFO", "WARNING", "ERROR"]:
        errors.append(f"logging.log_level must be DEBUG/INFO/WARNING/ERROR, got {log_level}")
    
    if errors:
        logger.error("Configuration validation failed:")
        for error in errors:
            logger.error(f"  - {error}")
        return False
    
    logger.info("Configuration validated successfully")
    return True


def load_config(config_path: Optional[str] = None, cli_args: Optional[list] = None) -> Dict[str, Any]:
    """
    Load and merge configuration from all sources
    
    Precedence (highest to lowest):
    1. CLI arguments
    2. Environment variables
    3. YAML config file
    4. Hardcoded defaults
    """
    
    logger.info("Loading configuration...")
    
    # Load environment
    load_dotenv_vars()
    
    # Start with defaults
    config = DEFAULT_CONFIG.copy()
    logger.info("✓ Loaded defaults")
    
    # Load YAML config file
    yaml_config_path = config_path or os.getenv("SIMULATOR_CONFIG")
    if yaml_config_path or find_config_file():
        config_file = Path(yaml_config_path) if yaml_config_path else find_config_file()
        if config_file:
            try:
                yaml_config = load_yaml_config(config_file)
                yaml_config = expand_env_vars(yaml_config)
                config = deep_merge(config, yaml_config)
                logger.info(f"✓ Merged YAML config from {config_file}")
            except Exception as e:
                logger.error(f"Failed to load YAML config: {e}")
    
    # Parse CLI arguments
    cli_config = {}
    scenario_name = None
    
    if cli_args:
        cli_config = parse_cli_args(cli_args)
        
        # Handle scenario
        scenario_name = cli_config.pop("_scenario", None)
        cli_config.pop("_config_file", None)  # Remove internal keys
        
        if scenario_name and "scenarios" in config:
            scenario = config["scenarios"].get(scenario_name)
            if scenario:
                logger.info(f"✓ Applying scenario: {scenario_name}")
                logger.info(f"  Description: {scenario.get('description', 'N/A')}")
                
                # Build scenario config with proper structure
                scenario_config = {
                    "simulator": {},
                    "defects": {}
                }
                
                # Map scenario fields to appropriate config locations
                for key, value in scenario.items():
                    if key in ["description", "defect_overrides"]:
                        continue
                    
                    if key == "defects_enabled":
                        scenario_config["defects"]["enabled"] = value
                    elif key in ["seed", "duration_seconds", "rate_rides_per_second"]:
                        scenario_config["simulator"][key] = value
                    else:
                        # For other keys, put in simulator by default
                        scenario_config["simulator"][key] = value
                
                # Merge scenario into config
                config = deep_merge(config, scenario_config)
                
                # Apply defect overrides
                if "defect_overrides" in scenario:
                    for defect_id, overrides in scenario["defect_overrides"].items():
                        config["defects"][defect_id] = deep_merge(
                            config["defects"].get(defect_id, {}),
                            overrides
                        )
        
        # Merge CLI overrides (highest precedence)
        if cli_config:
            config = deep_merge(config, cli_config)
            logger.info("✓ Merged CLI arguments")
    
    # Validate
    if not validate_config(config):
        raise ValueError("Configuration validation failed")
    
    return config


def print_config_summary(config: Dict[str, Any]) -> None:
    """Print configuration summary to console"""
    
    print("\n" + "=" * 70)
    print("RideOps AI Event Simulator - Configuration")
    print("=" * 70)
    
    sim = config.get("simulator", {})
    print(f"\n[Simulator]")
    print(f"  Duration: {sim.get('duration_seconds', 0)} seconds")
    print(f"  Rate: {sim.get('rate_rides_per_second', 0)} rides/sec")
    print(f"  Seed: {sim.get('seed', 'random')}")
    
    defects = config.get("defects", {})
    print(f"\n[Defects]")
    print(f"  Enabled: {defects.get('enabled', False)}")
    if defects.get('enabled'):
        for i in range(1, 9):
            d = defects.get(f"defect_{i}", {})
            if d.get('enabled'):
                print(f"  Defect {i}: {d.get('rate', 0):.1%} rate")
    
    db = config.get("database", {})
    print(f"\n[Database]")
    print(f"  Host: {db.get('host')}:{db.get('port')}")
    print(f"  Database: {db.get('database')}")
    
    logging_cfg = config.get("logging", {})
    print(f"\n[Logging]")
    print(f"  Level: {logging_cfg.get('log_level')}")
    print(f"  File: {logging_cfg.get('log_file')}")
    
    print("\n" + "=" * 70 + "\n")