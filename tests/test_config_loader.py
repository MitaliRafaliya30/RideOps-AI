"""
Tests for database/utils/config_loader.py
Validates configuration loading, merging, and validation.
"""

import pytest
import os
from pathlib import Path
from database.utils.config_loader import (
    load_config,
    parse_cli_args,
    validate_config,
    deep_merge,
    expand_env_vars,
)


class TestConfigDefaults:
    """Test default configuration"""
    
    def test_load_defaults(self):
        """Verify default config loads"""
        config = load_config(cli_args=[])
        
        assert config["simulator"]["duration_seconds"] == 3600
        assert config["simulator"]["rate_rides_per_second"] == 10
        assert config["defects"]["enabled"] == True
        assert config["generation"]["completion_rate"] == 0.85
    
    def test_default_defect_rates(self):
        """Verify all 8 defects have default rates"""
        config = load_config(cli_args=[])
        
        for i in range(1, 9):
            defect = config["defects"][f"defect_{i}"]
            assert "rate" in defect
            assert 0 <= defect["rate"] <= 1


class TestCLIArgs:
    """Test CLI argument parsing"""
    
    def test_parse_duration(self):
        """Test parsing --duration argument"""
        config = load_config(cli_args=["--duration=7200"])
        assert config["simulator"]["duration_seconds"] == 7200
    
    def test_parse_rate(self):
        """Test parsing --rate argument"""
        config = load_config(cli_args=["--rate=20"])
        assert config["simulator"]["rate_rides_per_second"] == 20
    
    def test_parse_seed(self):
        """Test parsing --seed argument"""
        config = load_config(cli_args=["--seed=12345"])
        assert config["simulator"]["seed"] == 12345
    
    def test_parse_defects_enabled(self):
        """Test parsing --defects-enabled argument"""
        config_enabled = load_config(cli_args=["--defects-enabled=true"])
        assert config_enabled["defects"]["enabled"] == True
        
        config_disabled = load_config(cli_args=["--defects-enabled=false"])
        assert config_disabled["defects"]["enabled"] == False
    
    def test_parse_defect_rate(self):
        """Test parsing defect rate arguments"""
        config = load_config(cli_args=["--defect-1-rate=0.20"])
        assert config["defects"]["defect_1"]["rate"] == 0.20
    
    def test_parse_log_level(self):
        """Test parsing --log-level argument"""
        config = load_config(cli_args=["--log-level=DEBUG"])
        assert config["logging"]["log_level"] == "DEBUG"
    
    def test_parse_multiple_args(self):
        """Test parsing multiple arguments together"""
        config = load_config(cli_args=[
            "--duration=1800",
            "--rate=5",
            "--seed=99999",
            "--defects-enabled=false"
        ])
        
        assert config["simulator"]["duration_seconds"] == 1800
        assert config["simulator"]["rate_rides_per_second"] == 5
        assert config["simulator"]["seed"] == 99999
        assert config["defects"]["enabled"] == False


class TestScenarios:
    """Test scenario presets"""
    
    def test_scenario_clean(self):
        """Test clean scenario (no defects)"""
        config = load_config(cli_args=["clean"])
        
        assert config["simulator"]["seed"] == 11111
        assert config["simulator"]["duration_seconds"] == 1800
        assert config["defects"]["enabled"] == False
    
    def test_scenario_default(self):
        """Test default scenario"""
        config = load_config(cli_args=["default"])
        
        assert config["simulator"]["seed"] == 22222
        assert config["simulator"]["duration_seconds"] == 3600
        assert config["defects"]["enabled"] == True
    
    def test_scenario_stress(self):
        """Test stress scenario (high defects)"""
        config = load_config(cli_args=["stress"])
        
        assert config["simulator"]["seed"] == 33333
        assert config["simulator"]["rate_rides_per_second"] == 100
        assert config["defects"]["enabled"] == True
        # Check that defect rates were increased
        assert config["defects"]["defect_1"]["rate"] == 0.20
        assert config["defects"]["defect_7"]["rate"] == 0.15
    
    def test_scenario_demo(self):
        """Test demo scenario (short and seeded)"""
        config = load_config(cli_args=["demo"])
        
        assert config["simulator"]["seed"] == 99999
        assert config["simulator"]["duration_seconds"] == 300
        assert config["simulator"]["rate_rides_per_second"] == 5


class TestDeepMerge:
    """Test deep merge functionality"""
    
    def test_merge_simple_override(self):
        """Test merging simple values"""
        base = {"a": 1, "b": 2}
        override = {"b": 3, "c": 4}
        
        result = deep_merge(base, override)
        
        assert result == {"a": 1, "b": 3, "c": 4}
    
    def test_merge_nested_dicts(self):
        """Test merging nested dictionaries"""
        base = {"simulator": {"duration": 3600, "rate": 10}}
        override = {"simulator": {"duration": 7200}}
        
        result = deep_merge(base, override)
        
        assert result["simulator"]["duration"] == 7200
        assert result["simulator"]["rate"] == 10
    
    def test_merge_precedence(self):
        """Test that override takes precedence"""
        base = {"x": {"y": 1}}
        override = {"x": {"y": 2}}
        
        result = deep_merge(base, override)
        assert result["x"]["y"] == 2


class TestValidation:
    """Test configuration validation"""
    
    def test_validate_positive_duration(self):
        """Duration must be positive"""
        config = load_config(cli_args=["--duration=100"])
        assert validate_config(config) == True
    
    def test_validate_invalid_duration(self):
        """Duration <= 0 should fail"""
        config = {
            "simulator": {"duration_seconds": 0},
            "defects": {"enabled": False}
        }
        assert validate_config(config) == False
    
    def test_validate_rate_range(self):
        """Rate must be between 1 and 1000"""
        config = load_config(cli_args=["--rate=50"])
        assert validate_config(config) == True
    
    def test_validate_defect_rate_range(self):
        """Defect rates must be 0-1"""
        config = load_config(cli_args=["--defect-1-rate=0.5"])
        assert validate_config(config) == True


class TestEnvironmentVariables:
    """Test environment variable handling"""
    
    def test_expand_env_vars(self):
        """Test expanding ${VAR} in config"""
        os.environ["TEST_VAR"] = "test_value"
        
        config = {"key": "${TEST_VAR}"}
        expanded = expand_env_vars(config)
        
        assert expanded["key"] == "test_value"
    
    def test_expand_nested_env_vars(self):
        """Test expanding env vars in nested config"""
        os.environ["DB_HOST"] = "localhost"
        
        config = {"database": {"host": "${DB_HOST}"}}
        expanded = expand_env_vars(config)
        
        assert expanded["database"]["host"] == "localhost"


class TestConfigPrecedence:
    """Test configuration precedence"""
    
    def test_cli_overrides_defaults(self):
        """CLI arguments should override defaults"""
        config = load_config(cli_args=["--rate=50"])
        assert config["simulator"]["rate_rides_per_second"] == 50
    
    def test_scenario_applies_settings(self):
        """Scenario should apply its settings"""
        config = load_config(cli_args=["demo"])
        assert config["simulator"]["seed"] == 99999
        assert config["simulator"]["duration_seconds"] == 300


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])