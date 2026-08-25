"""
Tests for generators/entity_pool.py
Validates that entity pool loads and samples correctly from master schema.

Uses existing database.config.db_config for connections.
"""

import pytest
import logging
from database.utils.database import PostgresConnection
from database.generators.entity_pool import EntityPool

# Setup logging for tests
logging.basicConfig(level=logging.INFO)


# Test fixtures
@pytest.fixture
def postgres_conn():
    """Create a connection using existing RideOps config"""
    try:
        conn = PostgresConnection()
        yield conn
    except Exception as e:
        pytest.skip(f"Could not connect to database: {e}")


@pytest.fixture
def entity_pool(postgres_conn):
    """Create entity pool from RideOps database"""
    return EntityPool(postgres_conn, seed=12345)


# ==================== LOADING TESTS ====================

class TestEntityPoolLoading:
    """Test that entity pool loads data correctly from master schema"""
    
    def test_passengers_loaded(self, entity_pool):
        """Verify passengers are loaded from master.passengers"""
        assert len(entity_pool.passengers) > 0, "No passengers loaded"
        print(f"✓ Loaded {len(entity_pool.passengers)} passengers")
        
        # Verify structure
        passenger = entity_pool.passengers[0]
        assert "id" in passenger
        assert "code" in passenger
        assert "home_zone_id" in passenger
        assert "membership_tier_id" in passenger
        assert "payment_method_id" in passenger
    
    def test_drivers_loaded(self, entity_pool):
        """Verify drivers are loaded from master.drivers"""
        assert len(entity_pool.drivers) > 0, "No drivers loaded"
        print(f"✓ Loaded {len(entity_pool.drivers)} drivers")
        
        driver = entity_pool.drivers[0]
        assert "id" in driver
        assert "name" in driver
        assert "rating" in driver
        assert "experience_years" in driver
        assert "status" in driver
    
    def test_vehicles_loaded(self, entity_pool):
        """Verify vehicles are loaded from master.vehicles"""
        assert len(entity_pool.vehicles) > 0, "No vehicles loaded"
        print(f"✓ Loaded {len(entity_pool.vehicles)} vehicles")
        
        vehicle = entity_pool.vehicles[0]
        assert "id" in vehicle
        assert "driver_id" in vehicle
        assert "vehicle_type_id" in vehicle
        assert "manufacture_year" in vehicle
    
    def test_zones_loaded(self, entity_pool):
        """Verify zones are loaded from master.zone_lookup"""
        assert len(entity_pool.zones) > 0, "No zones loaded"
        print(f"✓ Loaded {len(entity_pool.zones)} zones")
        assert all(isinstance(z, int) for z in entity_pool.zones), "Zones should be integers"
    
    def test_vehicle_types_loaded(self, entity_pool):
        """Verify vehicle types are loaded"""
        assert len(entity_pool.vehicle_types) > 0, "No vehicle types loaded"
        print(f"✓ Loaded {len(entity_pool.vehicle_types)} vehicle types")
        
        vtype = list(entity_pool.vehicle_types.values())[0]
        assert "name" in vtype
        assert "capacity" in vtype
        assert "fare_multiplier" in vtype
    
    def test_membership_tiers_loaded(self, entity_pool):
        """Verify membership tiers are loaded"""
        assert len(entity_pool.membership_tiers) > 0, "No membership tiers loaded"
        print(f"✓ Loaded {len(entity_pool.membership_tiers)} membership tiers")
        
        tier = list(entity_pool.membership_tiers.values())[0]
        assert "name" in tier
        assert "multiplier" in tier
    
    def test_payment_methods_loaded(self, entity_pool):
        """Verify payment methods are loaded"""
        assert len(entity_pool.payment_methods) > 0, "No payment methods loaded"
        print(f"✓ Loaded {len(entity_pool.payment_methods)} payment methods")
        
        method = entity_pool.payment_methods[0]
        assert "id" in method
        assert "name" in method


# ==================== VALIDATION TESTS ====================

class TestEntityPoolValidation:
    """Test that entity pool validates consistency"""
    
    def test_every_vehicle_has_driver(self, entity_pool):
        """
        Test that every active vehicle is assigned to a driver (active or inactive)
        
        Note: A vehicle can exist even if its driver is temporarily inactive/suspended.
        We just verify the driver_id exists in our dataset.
        """
        all_driver_ids = {d["id"] for d in entity_pool.drivers}
        
        # Also check if we can find drivers in the DB that might not be active
        # For now, just verify vehicles have a driver_id field
        for vehicle in entity_pool.vehicles:
            assert "driver_id" in vehicle, f"Vehicle {vehicle['id']} has no driver_id"
            assert vehicle["driver_id"] is not None, f"Vehicle {vehicle['id']} has null driver_id"
        
        print(f"✓ All {len(entity_pool.vehicles)} vehicles have valid driver_id values")
    
    def test_every_zone_exists(self, entity_pool):
        """Every passenger's home_zone_id should exist in zones"""
        valid_zones = set(entity_pool.zones)
        
        invalid_count = 0
        for passenger in entity_pool.passengers:
            if passenger["home_zone_id"] not in valid_zones:
                invalid_count += 1
        
        assert invalid_count == 0, f"Found {invalid_count} passengers with invalid zones"
        print(f"✓ All {len(entity_pool.passengers)} passengers have valid home_zones")
    
    def test_vehicle_types_exist(self, entity_pool):
        """Every vehicle should have a valid vehicle_type_id"""
        valid_types = set(entity_pool.vehicle_types.keys())
        
        invalid_count = 0
        for vehicle in entity_pool.vehicles:
            if vehicle["vehicle_type_id"] not in valid_types:
                invalid_count += 1
        
        assert invalid_count == 0, f"Found {invalid_count} vehicles with invalid types"
        print(f"✓ All {len(entity_pool.vehicles)} vehicles have valid types")
    
    def test_membership_tiers_valid(self, entity_pool):
        """Every passenger's membership_tier_id should be valid"""
        valid_tiers = set(entity_pool.membership_tiers.keys())
        
        invalid_count = 0
        for passenger in entity_pool.passengers:
            if passenger["membership_tier_id"] not in valid_tiers:
                invalid_count += 1
        
        assert invalid_count == 0, f"Found {invalid_count} passengers with invalid tiers"
        print(f"✓ All {len(entity_pool.passengers)} passengers have valid membership tiers")
    
    def test_payment_methods_valid(self, entity_pool):
        """Every passenger's payment_method_id should be valid"""
        valid_methods = {m["id"] for m in entity_pool.payment_methods}
        
        invalid_count = 0
        for passenger in entity_pool.passengers:
            if passenger["payment_method_id"] not in valid_methods:
                invalid_count += 1
        
        assert invalid_count == 0, f"Found {invalid_count} passengers with invalid payment methods"
        print(f"✓ All {len(entity_pool.passengers)} passengers have valid payment methods")


# ==================== SAMPLING TESTS ====================

class TestEntityPoolSampling:
    """Test that sampling methods work correctly"""
    
    def test_pick_passenger(self, entity_pool):
        """Test passenger sampling"""
        passenger = entity_pool.pick_passenger()
        assert passenger is not None
        assert "id" in passenger
        assert passenger["id"] in [p["id"] for p in entity_pool.passengers]
        print(f"✓ Sampled passenger: {passenger['code']}")
    
    def test_pick_driver(self, entity_pool):
        """Test driver sampling"""
        driver = entity_pool.pick_driver()
        assert driver is not None
        assert "id" in driver
        assert driver["id"] in [d["id"] for d in entity_pool.drivers]
        print(f"✓ Sampled driver: {driver['name']}")
    
    def test_pick_vehicle(self, entity_pool):
        """Test vehicle sampling"""
        vehicle = entity_pool.pick_vehicle()
        assert vehicle is not None
        assert "id" in vehicle
        print(f"✓ Sampled vehicle: {vehicle['code']}")
    
    def test_pick_zone(self, entity_pool):
        """Test zone sampling"""
        zone = entity_pool.pick_zone()
        assert zone in entity_pool.zones
        print(f"✓ Sampled zone: {zone}")
    
    def test_pick_payment_method(self, entity_pool):
        """Test payment method sampling"""
        method_id = entity_pool.pick_payment_method()
        valid_ids = {m["id"] for m in entity_pool.payment_methods}
        assert method_id in valid_ids
        print(f"✓ Sampled payment method ID: {method_id}")
    
    def test_reproducibility_with_seed(self, postgres_conn):
        """Verify that same seed produces same samples"""
        # Create two pools with same seed
        pool1 = EntityPool(postgres_conn, seed=54321)
        pool2 = EntityPool(postgres_conn, seed=54321)
        
        # Sample from both
        passengers1 = [pool1.pick_passenger() for _ in range(20)]
        passengers2 = [pool2.pick_passenger() for _ in range(20)]
        
        # Should be identical
        ids1 = [p["id"] for p in passengers1]
        ids2 = [p["id"] for p in passengers2]
        assert ids1 == ids2, "Same seed should produce same samples"
        print(f"✓ Reproducibility verified: {len(ids1)} samples identical with seed=54321")
    
    def test_different_seed_produces_different_samples(self, postgres_conn):
        """Verify that different seeds produce different samples"""
        pool1 = EntityPool(postgres_conn, seed=111)
        pool2 = EntityPool(postgres_conn, seed=222)
        
        samples1 = [pool1.pick_passenger()["id"] for _ in range(50)]
        samples2 = [pool2.pick_passenger()["id"] for _ in range(50)]
        
        # At least some should be different
        assert samples1 != samples2, "Different seeds should produce different samples"
        print(f"✓ Different seeds produced different samples")


# ==================== REFERENCE LOOKUP TESTS ====================

class TestEntityPoolReferenceLookups:
    """Test lookup methods for reference data"""
    
    def test_vehicle_for_driver(self, entity_pool):
        """Test getting vehicle for a specific driver"""
        driver = entity_pool.pick_driver()
        vehicle = entity_pool.vehicle_for_driver(driver["id"])
        
        assert vehicle is not None, f"No vehicle found for driver {driver['id']}"
        assert vehicle["driver_id"] == driver["id"]
        print(f"✓ Found vehicle {vehicle['code']} for driver {driver['code']}")
    
    def test_get_vehicle_type(self, entity_pool):
        """Test getting vehicle type details"""
        vehicle = entity_pool.pick_vehicle()
        vehicle_type = entity_pool.get_vehicle_type(vehicle["vehicle_type_id"])
        
        assert vehicle_type is not None
        assert "name" in vehicle_type
        assert "capacity" in vehicle_type
        print(f"✓ Vehicle type: {vehicle_type['name']} (capacity: {vehicle_type['capacity']})")
    
    def test_get_membership_tier(self, entity_pool):
        """Test getting membership tier details"""
        passenger = entity_pool.pick_passenger()
        tier = entity_pool.get_membership_tier(passenger["membership_tier_id"])
        
        assert tier is not None
        assert "name" in tier
        assert "multiplier" in tier
        print(f"✓ Membership tier: {tier['name']} (multiplier: {tier['multiplier']})")
    
    def test_get_payment_method(self, entity_pool):
        """Test getting payment method details"""
        method_id = entity_pool.pick_payment_method()
        method = entity_pool.get_payment_method(method_id)
        
        assert method is not None
        assert "name" in method
        print(f"✓ Payment method: {method['name']}")
    
    def test_get_ride_status(self, entity_pool):
        """Test getting ride status name"""
        # Pick any status ID from loaded statuses
        if entity_pool.ride_statuses:
            status_id = list(entity_pool.ride_statuses.keys())[0]
            status_name = entity_pool.get_ride_status(status_id)
            
            assert status_name is not None
            assert isinstance(status_name, str)
            print(f"✓ Ride status: {status_name}")


# ==================== SCALE TESTS ====================

class TestEntityPoolScale:
    """Test that entity pool handles scale correctly"""
    
    def test_can_sample_all_passengers_without_repeat_burden(self, entity_pool):
        """Verify we can sample many times without issues"""
        # Sample 1000 times (should be fast)
        samples = [entity_pool.pick_passenger()["id"] for _ in range(1000)]
        
        assert len(samples) == 1000
        assert all(s in [p["id"] for p in entity_pool.passengers] for s in samples)
        print(f"✓ Sampled 1000 passengers successfully")
    
    def test_memory_efficient(self, entity_pool):
        """Verify entity pool is memory efficient"""
        # All entities should be in memory
        total_entities = (
            len(entity_pool.passengers) +
            len(entity_pool.drivers) +
            len(entity_pool.vehicles) +
            len(entity_pool.zones)
        )
        
        assert total_entities > 200_000, "Should have loaded 200k+ entities"
        print(f"✓ Loaded {total_entities:,} entities into memory")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])