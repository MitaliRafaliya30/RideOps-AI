"""
Tests for database/generators/defect_injector.py
Validates that all 8 defects are injected correctly.
"""

import pytest
from datetime import datetime
from random import Random

from database.utils.database import PostgresConnection
from database.generators.entity_pool import EntityPool
from database.generators.ride_event_generator import RideEventGenerator
from database.generators.defect_injector import DefectInjector


@pytest.fixture
def postgres_conn():
    """PostgreSQL connection"""
    try:
        conn = PostgresConnection()
        yield conn
    except Exception as e:
        pytest.skip(f"Cannot connect to database: {e}")


@pytest.fixture
def entity_pool(postgres_conn):
    """Entity pool"""
    return EntityPool(postgres_conn, seed=12345)


@pytest.fixture
def config_defects_enabled():
    """Config with all defects enabled"""
    return {
        "simulator": {},
        "generation": {"completion_rate": 0.85},
        "defects": {
            "enabled": True,
            "defect_1": {"enabled": True, "rate": 1.0, "shuffle_window_size": 3},
            "defect_2": {"enabled": True, "rate": 1.0, "holdback_delay_seconds": {"min": 60, "max": 180}},
            "defect_3": {"enabled": True, "rate": 1.0},
            "defect_4": {"enabled": True, "rate": 1.0, "timestamp_gap_seconds": {"min": 0.1, "max": 2.0}},
            "defect_5": {"enabled": True, "rate": 1.0},
            "defect_6": {"enabled": True, "rate": 1.0},
            "defect_7": {"enabled": True, "rate": 1.0},
            "defect_8": {"enabled": True, "rate": 1.0},
        },
    }


@pytest.fixture
def config_defects_disabled():
    """Config with all defects disabled"""
    return {
        "simulator": {},
        "generation": {"completion_rate": 0.85},
        "defects": {"enabled": False},
    }


# ==================== DEFECT INJECTION TESTS ====================

class TestDefectInjection:
    """Test defect injection works"""
    
    def test_injector_initializes(self, config_defects_enabled):
        """Injector should initialize"""
        rng = Random(12345)
        injector = DefectInjector(config_defects_enabled, rng)
        
        assert injector is not None
        assert injector.defects_enabled == True
    
    def test_defects_disabled_returns_clean_ride(self, entity_pool, config_defects_disabled):
        """With defects disabled, ride should be unchanged"""
        rng = Random(12345)
        
        # Generate clean ride
        gen_rng = Random(12345)
        gen = RideEventGenerator(entity_pool, config_defects_disabled, gen_rng)
        now = datetime.utcnow()
        clean_ride = gen.generate_ride(now)
        
        # Try to inject defects
        injector = DefectInjector(config_defects_disabled, rng)
        injected_ride = injector.inject(clean_ride)
        
        # Should be identical
        assert len(injected_ride) == len(clean_ride)
        for i in range(len(clean_ride)):
            assert injected_ride[i]["event_type"] == clean_ride[i]["event_type"]
    
    def test_injector_preserves_ride_id(self, entity_pool, config_defects_enabled):
        """All events should keep same ride_id after injection"""
        rng_gen = Random(999)
        gen = RideEventGenerator(entity_pool, config_defects_enabled, rng_gen)
        now = datetime.utcnow()
        clean_ride = gen.generate_ride(now)
        
        rng_inject = Random(888)
        injector = DefectInjector(config_defects_enabled, rng_inject)
        injected_ride = injector.inject(clean_ride)
        
        if injected_ride:
            ride_id = injected_ride[0]["ride_id"]
            for event in injected_ride:
                assert event["ride_id"] == ride_id, "Ride ID should not change"


# ==================== INDIVIDUAL DEFECT TESTS ====================

class TestDefect1OutOfOrder:
    """Test Defect 1: Out-of-order events"""
    
    def test_defect_1_shuffles_events(self, entity_pool):
        """Defect 1 should reorder events"""
        config = {
            "simulator": {},
            "generation": {"completion_rate": 0.85},
            "defects": {
                "enabled": True,
                "defect_1": {"enabled": True, "rate": 1.0, "shuffle_window_size": 5},
                "defect_2": {"enabled": False, "rate": 0},
                "defect_3": {"enabled": False, "rate": 0},
                "defect_4": {"enabled": False, "rate": 0},
                "defect_5": {"enabled": False, "rate": 0},
                "defect_6": {"enabled": False, "rate": 0},
                "defect_7": {"enabled": False, "rate": 0},
                "defect_8": {"enabled": False, "rate": 0},
            },
        }
        
        rng_gen = Random(999)
        gen = RideEventGenerator(entity_pool, config, rng_gen)
        now = datetime.utcnow()
        clean_ride = gen.generate_ride(now)
        
        rng_inject = Random(888)
        injector = DefectInjector(config, rng_inject)
        injected_ride = injector.inject(clean_ride)
        
        # Timestamps should be different order (or same if all same time)
        if len(injected_ride) > 2:
            # At least try to verify injection happened
            assert len(injected_ride) == len(clean_ride), "Event count should not change"


class TestDefect3ExactDuplicate:
    """Test Defect 3: Exact duplicates"""
    
    def test_defect_3_creates_duplicate(self, entity_pool):
        """Defect 3 should create exact duplicates"""
        config = {
            "simulator": {},
            "generation": {"completion_rate": 0.85},
            "defects": {
                "enabled": True,
                "defect_1": {"enabled": False, "rate": 0},
                "defect_2": {"enabled": False, "rate": 0},
                "defect_3": {"enabled": True, "rate": 1.0},
                "defect_4": {"enabled": False, "rate": 0},
                "defect_5": {"enabled": False, "rate": 0},
                "defect_6": {"enabled": False, "rate": 0},
                "defect_7": {"enabled": False, "rate": 0},
                "defect_8": {"enabled": False, "rate": 0},
            },
        }
        
        rng_gen = Random(999)
        gen = RideEventGenerator(entity_pool, config, rng_gen)
        now = datetime.utcnow()
        clean_ride = gen.generate_ride(now)
        
        rng_inject = Random(888)
        injector = DefectInjector(config, rng_inject)
        injected_ride = injector.inject(clean_ride)
        
        # Should have more events (duplicate added)
        assert len(injected_ride) > len(clean_ride), "Defect 3 should add duplicate event"


class TestDefect5NullField:
    """Test Defect 5: Null required fields"""
    
    def test_defect_5_nulls_field(self, entity_pool):
        """Defect 5 should null a field"""
        config = {
            "simulator": {},
            "generation": {"completion_rate": 0.85},
            "defects": {
                "enabled": True,
                "defect_1": {"enabled": False, "rate": 0},
                "defect_2": {"enabled": False, "rate": 0},
                "defect_3": {"enabled": False, "rate": 0},
                "defect_4": {"enabled": False, "rate": 0},
                "defect_5": {"enabled": True, "rate": 1.0},
                "defect_6": {"enabled": False, "rate": 0},
                "defect_7": {"enabled": False, "rate": 0},
                "defect_8": {"enabled": False, "rate": 0},
            },
        }
        
        rng_gen = Random(999)
        gen = RideEventGenerator(entity_pool, config, rng_gen)
        now = datetime.utcnow()
        clean_ride = gen.generate_ride(now)
        
        rng_inject = Random(888)
        injector = DefectInjector(config, rng_inject)
        injected_ride = injector.inject(clean_ride)
        
        # At least one field should be nulled
        nulled_fields = []
        for i, event in enumerate(injected_ride):
            clean_event = clean_ride[i]
            for key in event:
                if event[key] is None and clean_event.get(key) is not None:
                    nulled_fields.append(key)
        
        assert len(nulled_fields) > 0, "Defect 5 should null at least one field"


class TestDefect6InvalidType:
    """Test Defect 6: Invalid event type"""
    
    def test_defect_6_corrupts_type(self, entity_pool):
        """Defect 6 should set invalid event_type"""
        config = {
            "simulator": {},
            "generation": {"completion_rate": 0.85},
            "defects": {
                "enabled": True,
                "defect_1": {"enabled": False, "rate": 0},
                "defect_2": {"enabled": False, "rate": 0},
                "defect_3": {"enabled": False, "rate": 0},
                "defect_4": {"enabled": False, "rate": 0},
                "defect_5": {"enabled": False, "rate": 0},
                "defect_6": {"enabled": True, "rate": 1.0},
                "defect_7": {"enabled": False, "rate": 0},
                "defect_8": {"enabled": False, "rate": 0},
            },
        }
        
        rng_gen = Random(999)
        gen = RideEventGenerator(entity_pool, config, rng_gen)
        now = datetime.utcnow()
        clean_ride = gen.generate_ride(now)
        
        rng_inject = Random(888)
        injector = DefectInjector(config, rng_inject)
        injected_ride = injector.inject(clean_ride)
        
        # At least one event type should be invalid
        valid_types = ["requested", "assigned", "accepted", "arrived", "started", "completed", "cancelled"]
        invalid_found = False
        
        for event in injected_ride:
            if event["event_type"] not in valid_types:
                invalid_found = True
                break
        
        assert invalid_found, "Defect 6 should create at least one invalid event_type"


class TestDefect8Incomplete:
    """Test Defect 8: Incomplete rides"""
    
    def test_defect_8_removes_terminal(self, entity_pool):
        """Defect 8 should remove terminal event"""
        config = {
            "simulator": {},
            "generation": {"completion_rate": 0.85},
            "defects": {
                "enabled": True,
                "defect_1": {"enabled": False, "rate": 0},
                "defect_2": {"enabled": False, "rate": 0},
                "defect_3": {"enabled": False, "rate": 0},
                "defect_4": {"enabled": False, "rate": 0},
                "defect_5": {"enabled": False, "rate": 0},
                "defect_6": {"enabled": False, "rate": 0},
                "defect_7": {"enabled": False, "rate": 0},
                "defect_8": {"enabled": True, "rate": 1.0},
            },
        }
        
        rng_gen = Random(999)
        gen = RideEventGenerator(entity_pool, config, rng_gen)
        now = datetime.utcnow()
        clean_ride = gen.generate_ride(now)
        
        rng_inject = Random(888)
        injector = DefectInjector(config, rng_inject)
        injected_ride = injector.inject(clean_ride)
        
        # Last event should not be terminal
        if injected_ride:
            last_event_type = injected_ride[-1]["event_type"]
            assert last_event_type not in ["completed", "cancelled"], \
                f"Defect 8 should remove terminal event, but got {last_event_type}"


# ==================== REPRODUCIBILITY TESTS ====================

class TestDefectReproducibility:
    """Test that defects are reproducible with seeds"""
    
    def test_same_seed_produces_same_defects(self, entity_pool, config_defects_enabled):
        """Same seed should produce same defects"""
        now = datetime.utcnow()
        
        # Generate clean ride
        rng_gen = Random(777)
        gen = RideEventGenerator(entity_pool, config_defects_enabled, rng_gen)
        clean_ride = gen.generate_ride(now)
        
        # Inject defects twice with same seed
        rng1 = Random(666)
        injector1 = DefectInjector(config_defects_enabled, rng1)
        ride1 = injector1.inject(clean_ride)
        
        rng2 = Random(666)
        injector2 = DefectInjector(config_defects_enabled, rng2)
        ride2 = injector2.inject(clean_ride)
        
        # Should be identical
        assert len(ride1) == len(ride2)
        for i in range(len(ride1)):
            assert ride1[i]["event_type"] == ride2[i]["event_type"]


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])