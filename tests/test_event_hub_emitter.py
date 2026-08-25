"""
Tests for database/generators/event_hub_emitter.py
Validates event emission, batching, serialization, and metrics.
"""

import pytest
import json
from datetime import datetime
from random import Random

from database.generators.event_hub_emitter import EventHubEmitter
from database.utils.database import PostgresConnection
from database.generators.entity_pool import EntityPool
from database.generators.ride_event_generator import RideEventGenerator


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
def config_emission():
    """Config for emission"""
    return {
        "simulator": {},
        "generation": {"completion_rate": 0.85},
        "defects": {"enabled": False},
        "event_hubs": {
            "connection_string": "Endpoint=sb://test.servicebus.windows.net/",
            "hub_name": "ride-events",
            "partition_count": 8,
            "partition_key": "ride_id",
            "emission": {
                "batch_size": 1,
                "synchronous": True,
                "retry_max_attempts": 3,
                "retry_backoff_multiplier": 2,
                "timeout_seconds": 30,
            },
        },
    }


@pytest.fixture
def emitter(config_emission):
    """Event hub emitter"""
    rng = Random(12345)
    return EventHubEmitter(config_emission, rng)


# ==================== INITIALIZATION TESTS ====================

class TestEmitterInitialization:
    """Test emitter initialization"""
    
    def test_emitter_initializes(self, config_emission):
        """Emitter should initialize"""
        rng = Random(12345)
        emitter = EventHubEmitter(config_emission, rng)
        
        assert emitter is not None
        assert emitter.hub_name == "ride-events"
        assert emitter.batch_size == 1
    
    def test_emitter_defaults(self, config_emission):
        """Emitter should have correct defaults"""
        rng = Random(12345)
        config = {"simulator": {}, "defects": {}}
        emitter = EventHubEmitter(config, rng)
        
        assert emitter.batch_size == 1
        assert emitter.retry_max_attempts == 3
        assert emitter.synchronous == True


# ==================== SERIALIZATION TESTS ====================

class TestEventSerialization:
    """Test event serialization"""
    
    def test_serialize_event(self, emitter):
        """Events should serialize to JSON"""
        event = {
            "event_id": "123",
            "ride_id": "abc",
            "event_type": "requested",
            "event_timestamp": "2024-01-01T12:00:00",
            "passenger_id": 1,
            "driver_id": None,
        }
        
        serialized = emitter._serialize_event(event)
        
        # Should be valid JSON
        parsed = json.loads(serialized)
        assert parsed["event_id"] == "123"
        assert parsed["ride_id"] == "abc"
    
    def test_serialize_with_decimal(self, emitter):
        """Events with Decimal should serialize"""
        from decimal import Decimal
        
        event = {
            "event_id": "123",
            "ride_id": "abc",
            "event_type": "completed",
            "base_passenger_fare": Decimal("12.50"),
            "tips": Decimal("2.50"),
        }
        
        serialized = emitter._serialize_event(event)
        parsed = json.loads(serialized)
        
        # Decimals should be converted to strings
        assert parsed["base_passenger_fare"] == "12.50"
        assert parsed["tips"] == "2.50"


# ==================== BATCHING TESTS ====================

class TestBatching:
    """Test event batching"""
    
    def test_batch_size_1(self, emitter):
        """Batch size 1 should not batch"""
        events = ["e1", "e2", "e3"]
        batches = emitter._create_batches(events)
        
        assert len(batches) == 3
        assert all(len(b) == 1 for b in batches)
    
    def test_batch_size_10(self, config_emission):
        """Batch size 10 should group events"""
        config = config_emission.copy()
        config["event_hubs"]["emission"]["batch_size"] = 10
        
        rng = Random(12345)
        emitter = EventHubEmitter(config, rng)
        
        events = [f"e{i}" for i in range(25)]
        batches = emitter._create_batches(events)
        
        assert len(batches) == 3  # 10 + 10 + 5
        assert len(batches[0]) == 10
        assert len(batches[1]) == 10
        assert len(batches[2]) == 5


# ==================== SENDING TESTS ====================

class TestEventSending:
    """Test event sending"""
    
    def test_send_single_event(self, entity_pool, emitter):
        """Should send a single event"""
        rng = Random(999)
        config = {
            "simulator": {},
            "generation": {"completion_rate": 0.85},
            "defects": {"enabled": False},
            "event_hubs": {"emission": {"batch_size": 1}},
        }
        gen = RideEventGenerator(entity_pool, config, rng)
        
        now = datetime.utcnow()
        ride = gen.generate_ride(now)
        
        result = emitter.send(ride)
        assert result == True
    
    def test_send_empty_ride(self, emitter):
        """Sending empty ride should succeed"""
        result = emitter.send([])
        assert result == True
    
    def test_send_updates_metrics(self, entity_pool, emitter):
        """Sending should update metrics"""
        rng = Random(999)
        config = {
            "simulator": {},
            "generation": {"completion_rate": 0.85},
            "defects": {"enabled": False},
            "event_hubs": {"emission": {"batch_size": 1}},
        }
        gen = RideEventGenerator(entity_pool, config, rng)
        
        now = datetime.utcnow()
        ride = gen.generate_ride(now)
        
        initial_sent = emitter.events_sent
        emitter.send(ride)
        
        # Should have sent some events
        assert emitter.events_sent > initial_sent
        assert emitter.bytes_sent > 0


# ==================== METRICS TESTS ====================

class TestMetrics:
    """Test metrics tracking"""
    
    def test_metrics_initialized(self, emitter):
        """Metrics should be initialized"""
        metrics = emitter.get_metrics()
        
        assert "events_sent" in metrics
        assert "events_failed" in metrics
        assert "bytes_sent" in metrics
        assert "batches_sent" in metrics
        assert "events_per_second" in metrics
        assert "success_rate" in metrics
    
    def test_metrics_after_send(self, entity_pool, emitter):
        """Metrics should update after send"""
        rng = Random(999)
        config = {
            "simulator": {},
            "generation": {"completion_rate": 0.85},
            "defects": {"enabled": False},
            "event_hubs": {"emission": {"batch_size": 1}},
        }
        gen = RideEventGenerator(entity_pool, config, rng)
        
        now = datetime.utcnow()
        
        # Send 5 rides
        for _ in range(5):
            ride = gen.generate_ride(now)
            if ride:
                emitter.send(ride)
        
        metrics = emitter.get_metrics()
        
        # Should have sent at least some events
        assert metrics["events_sent"] > 0
        assert metrics["events_per_second"] >= 0
        assert metrics["success_rate"] >= 0
    
    def test_success_rate_calculation(self, config_emission):
        """Success rate should be calculated correctly"""
        rng = Random(12345)
        emitter = EventHubEmitter(config_emission, rng)
        
        # Simulate some sends
        emitter.events_sent = 90
        emitter.events_failed = 10
        
        metrics = emitter.get_metrics()
        
        assert metrics["success_rate"] == 0.9


# ==================== BATCHING WITH MULTIPLE RIDES ====================

class TestMultipleRideEmission:
    """Test emitting multiple rides"""
    
    def test_emit_multiple_rides(self, entity_pool, config_emission):
        """Should emit multiple rides successfully"""
        rng_gen = Random(999)
        gen = RideEventGenerator(entity_pool, config_emission, rng_gen)
        
        rng_emit = Random(888)
        emitter = EventHubEmitter(config_emission, rng_emit)
        
        now = datetime.utcnow()
        
        # Send 10 rides
        for _ in range(10):
            ride = gen.generate_ride(now)
            if ride:
                result = emitter.send(ride)
                assert result == True
        
        # Should have sent all
        assert emitter.events_sent > 0
        assert emitter.events_failed == 0
    
    def test_emit_with_larger_batches(self, entity_pool, config_emission):
        """Should batch multiple events together"""
        config = config_emission.copy()
        config["event_hubs"]["emission"]["batch_size"] = 5
        
        rng_gen = Random(999)
        gen = RideEventGenerator(entity_pool, config, rng_gen)
        
        rng_emit = Random(888)
        emitter = EventHubEmitter(config, rng_emit)
        
        now = datetime.utcnow()
        
        # Send rides
        for _ in range(5):
            ride = gen.generate_ride(now)
            if ride:
                emitter.send(ride)
        
        # With batch size 5, should have fewer batches than events
        assert emitter.batches_sent < emitter.events_sent


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])