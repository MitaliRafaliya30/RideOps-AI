"""
Tests for database/generators/ride_event_generator.py
Validates ride generation, state machine, fares, and timing.
"""

import pytest
from datetime import datetime
from random import Random

from database.utils.database import PostgresConnection
from database.generators.entity_pool import EntityPool
from database.generators.ride_event_generator import RideEventGenerator
from database.utils.constants import EVENT_TYPES


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
    """Entity pool from database"""
    return EntityPool(postgres_conn, seed=12345)


@pytest.fixture
def config():
    """Default config"""
    return {
        "simulator": {"duration_seconds": 3600, "rate_rides_per_second": 10},
        "generation": {"completion_rate": 0.85},
        "defects": {"enabled": False},
    }


@pytest.fixture
def generator(entity_pool, config):
    """Ride event generator"""
    rng = Random(12345)
    return RideEventGenerator(entity_pool, config, rng)


# ==================== GENERATION TESTS ====================

class TestGeneration:
    """Test ride generation"""
    
    def test_generate_single_ride(self, generator):
        """Generate one ride"""
        now = datetime.utcnow()
        ride = generator.generate_ride(now)
        
        assert len(ride) >= 5, "Ride should have at least 5 events"
        assert ride[0]["event_type"] == "requested", "First event should be requested"
    
    def test_ride_has_valid_structure(self, generator):
        """Verify ride events have required fields"""
        now = datetime.utcnow()
        ride = generator.generate_ride(now)
        
        required_fields = [
            "event_id", "ride_id", "event_type", "event_timestamp",
            "passenger_id", "payment_method_id"
        ]
        
        for event in ride:
            for field in required_fields:
                assert field in event, f"Event missing field: {field}"
                assert event[field] is not None, f"Field is null: {field}"
    
    def test_ride_id_consistent(self, generator):
        """All events in ride should have same ride_id"""
        now = datetime.utcnow()
        ride = generator.generate_ride(now)
        
        ride_id = ride[0]["ride_id"]
        for event in ride:
            assert event["ride_id"] == ride_id
    
    def test_events_chronological(self, generator):
        """Events should be in chronological order"""
        now = datetime.utcnow()
        ride = generator.generate_ride(now)
        
        for i in range(len(ride) - 1):
            ts1 = datetime.fromisoformat(ride[i]["event_timestamp"])
            ts2 = datetime.fromisoformat(ride[i+1]["event_timestamp"])
            assert ts1 <= ts2, f"Events not chronological: {ride[i]['event_type']} vs {ride[i+1]['event_type']}"


# ==================== STATE MACHINE TESTS ====================

class TestStateMachine:
    """Test state machine transitions"""
    
    def test_completed_ride_flow(self, generator):
        """Test a completed ride has correct flow"""
        now = datetime.utcnow()
        ride = generator.generate_ride(now)
        
        # Try to find a completed ride
        ride_types = [e["event_type"] for e in ride]
        
        if "completed" in ride_types:
            # Should be: requested → assigned → accepted → arrived → started → completed
            expected_sequence = ["requested", "assigned", "accepted", "arrived", "started", "completed"]
            assert ride_types == expected_sequence, f"Completed ride has wrong sequence: {ride_types}"
    
    def test_cancelled_ride_has_terminal_state(self, generator):
        """Cancelled rides should end with cancelled event"""
        # Keep generating until we find a cancelled ride
        for _ in range(100):
            now = datetime.utcnow()
            ride = generator.generate_ride(now)
            ride_types = [e["event_type"] for e in ride]
            
            if "cancelled" in ride_types:
                assert ride_types[-1] == "cancelled", "Last event should be cancelled"
                return
        
        # If we get here, we didn't find a cancelled ride (just skip)
        pytest.skip("No cancelled rides generated in 100 tries")
    
    def test_driver_null_before_assigned(self, generator):
        """Driver should be null in requested, present from assigned onward"""
        now = datetime.utcnow()
        ride = generator.generate_ride(now)
        
        assert ride[0]["event_type"] == "requested"
        assert ride[0]["driver_id"] is None, "Requested event should have no driver"
        
        assert ride[1]["event_type"] == "assigned"
        assert ride[1]["driver_id"] is not None, "Assigned event should have driver"
    
    def test_vehicle_null_before_assigned(self, generator):
        """Vehicle should be null in requested, present from assigned onward"""
        now = datetime.utcnow()
        ride = generator.generate_ride(now)
        
        assert ride[0]["vehicle_id"] is None, "Requested event should have no vehicle"
        assert ride[1]["vehicle_id"] is not None, "Assigned event should have vehicle"


# ==================== FARE TESTS ====================

class TestFares:
    """Test fare calculation"""
    
    def test_completed_ride_has_fares(self, generator):
        """Completed rides should have all fare components"""
        now = datetime.utcnow()
        
        for _ in range(20):
            ride = generator.generate_ride(now)
            
            if ride[-1]["event_type"] == "completed":
                completed = ride[-1]
                
                fare_fields = [
                    "base_passenger_fare", "tolls", "bcf", "sales_tax",
                    "congestion_surcharge", "airport_fee", "cbd_congestion_fee", "tips"
                ]
                
                for field in fare_fields:
                    assert field in completed, f"Missing fare field: {field}"
                    assert completed[field] is not None, f"Fare field is null: {field}"
                
                return
        
        pytest.skip("No completed rides generated")
    
    def test_cancelled_ride_no_fares(self, generator):
        """Cancelled rides should NOT have fares"""
        now = datetime.utcnow()
        
        for _ in range(50):
            ride = generator.generate_ride(now)
            
            if ride[-1]["event_type"] == "cancelled":
                completed = ride[-1]
                
                assert completed["base_passenger_fare"] is None
                assert completed["tips"] is None
                
                return
        
        pytest.skip("No cancelled rides generated")
    
    def test_trip_miles_positive(self, generator):
        """Completed rides should have positive trip_miles"""
        now = datetime.utcnow()
        
        for _ in range(20):
            ride = generator.generate_ride(now)
            
            if ride[-1]["event_type"] == "completed":
                completed = ride[-1]
                trip_miles = completed["trip_miles"]
                
                assert trip_miles is not None
                assert float(trip_miles) > 0, "Trip miles should be positive"
                assert float(trip_miles) < 25, "Trip miles should be realistic (<25 miles)"
                
                return
        
        pytest.skip("No completed rides generated")


# ==================== REPRODUCIBILITY TESTS ====================

class TestReproducibility:
    """Test that seeded generator produces reproducible rides"""
    
    def test_same_seed_produces_same_rides(self, postgres_conn, config):
        """Same seed should produce identical rides"""
        
        # Generate with seed 999
        pool1 = EntityPool(postgres_conn, seed=999)
        rng1 = Random(999)
        gen1 = RideEventGenerator(pool1, config, rng1)
        now = datetime.utcnow()
        ride1 = gen1.generate_ride(now)
        
        # Generate again with seed 999 and fresh pool
        pool2 = EntityPool(postgres_conn, seed=999)
        rng2 = Random(999)
        gen2 = RideEventGenerator(pool2, config, rng2)
        ride2 = gen2.generate_ride(now)
        
        # Compare key fields that ARE reproducible (passenger, driver, not ride_id)
        assert len(ride1) == len(ride2), "Same seed should produce same number of events"
        
        for i in range(len(ride1)):
            # Compare reproducible fields (not ride_id or event_id)
            assert ride1[i]["passenger_id"] == ride2[i]["passenger_id"], f"Event {i}: passenger mismatch"
            assert ride1[i]["driver_id"] == ride2[i]["driver_id"], f"Event {i}: driver mismatch"
            assert ride1[i]["event_type"] == ride2[i]["event_type"], f"Event {i}: event type mismatch"
            assert ride1[i]["pickup_location_id"] == ride2[i]["pickup_location_id"]
            assert ride1[i]["dropoff_location_id"] == ride2[i]["dropoff_location_id"]
    
    def test_different_seed_produces_different_rides(self, entity_pool, config):
        """Different seeds should produce different rides"""
        
        rng1 = Random(111)
        gen1 = RideEventGenerator(entity_pool, config, rng1)
        now = datetime.utcnow()
        ride1 = gen1.generate_ride(now)
        
        rng2 = Random(222)
        gen2 = RideEventGenerator(entity_pool, config, rng2)
        ride2 = gen2.generate_ride(now)
        
        # At least something should be different (probably passenger or driver)
        ids_match = (
            ride1[1]["passenger_id"] == ride2[1]["passenger_id"] and
            ride1[1]["driver_id"] == ride2[1]["driver_id"]
        )
        
        assert not ids_match, "Different seeds should likely produce different entity selections"


# ==================== COMPLETION RATE TESTS ====================

class TestCompletionRate:
    """Test completion vs cancellation rates"""
    
    def test_completion_rate_respected(self, entity_pool):
        """Completion rate should match config"""
        config = {"simulator": {}, "generation": {"completion_rate": 0.85}, "defects": {}}
        rng = Random(12345)
        generator = RideEventGenerator(entity_pool, config, rng)
        
        now = datetime.utcnow()
        num_rides = 200
        completed_count = 0
        valid_rides = 0
        
        for _ in range(num_rides):
            ride = generator.generate_ride(now)
            
            # Skip empty rides (no vehicle for driver)
            if not ride:
                continue
            
            valid_rides += 1
            if ride[-1]["event_type"] == "completed":
                completed_count += 1
        
        if valid_rides == 0:
            pytest.skip("No valid rides generated")
        
        completion_rate = completed_count / valid_rides
        
        # Should be close to 85% (allow ±10% variance)
        assert 0.75 <= completion_rate <= 0.95, f"Completion rate {completion_rate:.1%} outside expected range"


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])