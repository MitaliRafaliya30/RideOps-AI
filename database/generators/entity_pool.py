"""
Entity Pool: In-memory cache of passengers, drivers, vehicles, zones, and reference data.
Loaded once at startup from PostgreSQL. Used by the generator to sample entities.

Adapted to match RideOps AI schema with master.* tables.
"""

import logging
from random import Random
from typing import List, Dict, Optional

logger = logging.getLogger(__name__)


class EntityPool:
    """
    In-memory cache of all entities needed for ride generation.
    Loaded once at startup; never re-queried during generation.
    
    Matches your actual database schema:
    - master.passengers (200k)
    - master.drivers (20k)
    - master.vehicles (20k, 1:1 with drivers)
    - master.zone_lookup (263 zones)
    - master.vehicle_types, membership_tiers, payment_methods (reference)
    """
    
    def __init__(self, postgres_conn, seed: int = None):
        """
        Initialize entity pool from PostgreSQL master schema
        
        Args:
            postgres_conn: PostgresConnection instance
            seed: RNG seed for reproducibility
        """
        self.db = postgres_conn
        self.rng = Random(seed)
        
        logger.info("Loading entity pool from PostgreSQL (master schema)...")
        
        # Load all entities from master schema
        self.passengers = self._load_passengers()
        self.drivers = self._load_drivers()
        self.vehicles = self._load_vehicles()
        self.zones = self._load_zones()
        self.vehicle_types = self._load_vehicle_types()
        self.membership_tiers = self._load_membership_tiers()
        self.payment_methods = self._load_payment_methods()
        self.ride_statuses = self._load_ride_statuses()
        
        # Validate consistency
        self._validate()
        
        logger.info(f"Entity pool loaded successfully:")
        logger.info(f"  ✓ {len(self.passengers)} passengers")
        logger.info(f"  ✓ {len(self.drivers)} drivers")
        logger.info(f"  ✓ {len(self.vehicles)} vehicles")
        logger.info(f"  ✓ {len(self.zones)} zones")
        logger.info(f"  ✓ {len(self.vehicle_types)} vehicle types")
        logger.info(f"  ✓ {len(self.membership_tiers)} membership tiers")
        logger.info(f"  ✓ {len(self.payment_methods)} payment methods")
    
    # ==================== LOADERS ====================
    
    def _load_passengers(self) -> List[Dict]:
        """
        Load active passengers from master.passengers
        
        Schema: passenger_id, passenger_code, membership_tier_id, 
                preferred_payment_method_id, home_zone_id, is_active
        """
        sql = """
        SELECT passenger_id, passenger_code, membership_tier_id, 
               preferred_payment_method_id, home_zone_id
        FROM master.passengers
        WHERE is_active = true
        ORDER BY passenger_id
        """
        rows = self.db.query(sql)
        
        passengers = [
            {
                "id": row[0],
                "code": row[1],
                "membership_tier_id": row[2],
                "payment_method_id": row[3],
                "home_zone_id": row[4]
            }
            for row in rows
        ]
        
        logger.info(f"Loaded {len(passengers)} active passengers from master.passengers")
        return passengers
    
    def _load_drivers(self) -> List[Dict]:
        """
        Load active drivers from master.drivers
        
        Schema: driver_id, driver_code, driver_name, rating, 
                experience_years, join_date, status
        """
        sql = """
        SELECT driver_id, driver_code, driver_name, rating, 
               experience_years, join_date, status
        FROM master.drivers
        WHERE status = 'ACTIVE'
        ORDER BY driver_id
        """
        rows = self.db.query(sql)
        
        drivers = [
            {
                "id": row[0],
                "code": row[1],
                "name": row[2],
                "rating": row[3],
                "experience_years": row[4],
                "join_date": row[5],
                "status": row[6]
            }
            for row in rows
        ]
        
        logger.info(f"Loaded {len(drivers)} active drivers from master.drivers")
        return drivers
    
    def _load_vehicles(self) -> List[Dict]:
        """
        Load active vehicles from master.vehicles
        
        Schema: vehicle_id, vehicle_code, driver_id, vehicle_type_id, 
                manufacture_year, last_service_date, is_active
        
        Note: 1:1 relationship with drivers (vehicle_id = driver_id)
        """
        sql = """
        SELECT vehicle_id, vehicle_code, driver_id, vehicle_type_id, 
               manufacture_year, last_service_date
        FROM master.vehicles
        WHERE is_active = true
        ORDER BY vehicle_id
        """
        rows = self.db.query(sql)
        
        vehicles = [
            {
                "id": row[0],
                "code": row[1],
                "driver_id": row[2],
                "vehicle_type_id": row[3],
                "manufacture_year": row[4],
                "last_service_date": row[5]
            }
            for row in rows
        ]
        
        logger.info(f"Loaded {len(vehicles)} active vehicles from master.vehicles")
        return vehicles
    
    def _load_zones(self) -> List[int]:
        """
        Load all NYC zones from master.zone_lookup
        
        Schema: zone_id, borough, zone_name
        """
        sql = "SELECT zone_id FROM master.zone_lookup ORDER BY zone_id"
        rows = self.db.query(sql)
        
        zones = [row[0] for row in rows]
        logger.info(f"Loaded {len(zones)} zones from master.zone_lookup")
        return zones
    
    def _load_vehicle_types(self) -> Dict[int, Dict]:
        """
        Load vehicle types reference data from master.vehicle_types
        
        Schema: vehicle_type_id, vehicle_type_name, capacity, base_fare_multiplier
        """
        sql = """
        SELECT vehicle_type_id, vehicle_type_name, capacity, base_fare_multiplier
        FROM master.vehicle_types
        ORDER BY vehicle_type_id
        """
        rows = self.db.query(sql)
        
        vehicle_types = {
            row[0]: {
                "id": row[0],
                "name": row[1],
                "capacity": row[2],
                "fare_multiplier": row[3]
            }
            for row in rows
        }
        
        logger.info(f"Loaded {len(vehicle_types)} vehicle types from master.vehicle_types")
        return vehicle_types
    
    def _load_membership_tiers(self) -> Dict[int, Dict]:
        """
        Load membership tiers reference data from master.membership_tiers
        
        Schema: membership_tier_id, membership_name, reward_points_multiplier
        """
        sql = """
        SELECT membership_tier_id, membership_name, reward_points_multiplier
        FROM master.membership_tiers
        ORDER BY membership_tier_id
        """
        rows = self.db.query(sql)
        
        tiers = {
            row[0]: {
                "id": row[0],
                "name": row[1],
                "multiplier": row[2]
            }
            for row in rows
        }
        
        logger.info(f"Loaded {len(tiers)} membership tiers from master.membership_tiers")
        return tiers
    
    def _load_payment_methods(self) -> List[Dict]:
        """
        Load payment methods from master.payment_methods
        
        Schema: payment_method_id, payment_method_name
        """
        sql = """
        SELECT payment_method_id, payment_method_name
        FROM master.payment_methods
        ORDER BY payment_method_id
        """
        rows = self.db.query(sql)
        
        methods = [
            {
                "id": row[0],
                "name": row[1]
            }
            for row in rows
        ]
        
        logger.info(f"Loaded {len(methods)} payment methods from master.payment_methods")
        return methods
    
    def _load_ride_statuses(self) -> Dict[int, str]:
        """
        Load ride statuses from master.ride_status
        
        Schema: ride_status_id, ride_status_name
        """
        sql = """
        SELECT ride_status_id, ride_status_name
        FROM master.ride_status
        ORDER BY ride_status_id
        """
        rows = self.db.query(sql)
        
        statuses = {row[0]: row[1] for row in rows}
        logger.info(f"Loaded {len(statuses)} ride statuses from master.ride_status")
        return statuses
    
    # ==================== VALIDATION ====================
    
    def _validate(self):
        """Validate entity consistency across relationships"""
        logger.info("Validating entity pool consistency...")
        
        errors = []
        
        # Every vehicle must have a valid active driver
        driver_ids = {d["id"] for d in self.drivers}
        invalid_vehicles = [v for v in self.vehicles if v["driver_id"] not in driver_ids]
        if invalid_vehicles:
            msg = f"Found {len(invalid_vehicles)} vehicles with missing/inactive drivers"
            logger.warning(msg)
            errors.append(msg)
        
        # Every passenger's home_zone must exist
        valid_zones = set(self.zones)
        invalid_passengers = [p for p in self.passengers if p["home_zone_id"] not in valid_zones]
        if invalid_passengers:
            msg = f"Found {len(invalid_passengers)} passengers with invalid home_zone_id"
            logger.warning(msg)
            errors.append(msg)
        
        # Every vehicle's type must exist
        valid_types = set(self.vehicle_types.keys())
        invalid_vehicles_type = [v for v in self.vehicles if v["vehicle_type_id"] not in valid_types]
        if invalid_vehicles_type:
            msg = f"Found {len(invalid_vehicles_type)} vehicles with invalid vehicle_type_id"
            logger.warning(msg)
            errors.append(msg)
        
        # Every passenger's membership tier must exist
        valid_tiers = set(self.membership_tiers.keys())
        invalid_passengers_tier = [p for p in self.passengers if p["membership_tier_id"] not in valid_tiers]
        if invalid_passengers_tier:
            msg = f"Found {len(invalid_passengers_tier)} passengers with invalid membership_tier_id"
            logger.warning(msg)
            errors.append(msg)
        
        # Every passenger's payment method must exist
        valid_methods = {m["id"] for m in self.payment_methods}
        invalid_passengers_payment = [p for p in self.passengers if p["payment_method_id"] not in valid_methods]
        if invalid_passengers_payment:
            msg = f"Found {len(invalid_passengers_payment)} passengers with invalid payment_method_id"
            logger.warning(msg)
            errors.append(msg)
        
        if errors:
            logger.warning(f"Validation found {len(errors)} issue(s)")
        else:
            logger.info("✓ All entity relationships valid")
    
    # ==================== SAMPLING ====================
    
    def pick_passenger(self) -> Dict:
        """Randomly pick a passenger (uniform distribution)"""
        if not self.passengers:
            raise ValueError("No passengers available to pick")
        return self.rng.choice(self.passengers)
    
    def pick_driver(self) -> Dict:
        """Randomly pick a driver (uniform distribution)"""
        if not self.drivers:
            raise ValueError("No drivers available to pick")
        return self.rng.choice(self.drivers)
    
    def pick_vehicle(self) -> Dict:
        """Randomly pick a vehicle (uniform distribution)"""
        if not self.vehicles:
            raise ValueError("No vehicles available to pick")
        return self.rng.choice(self.vehicles)
    
    def pick_zone(self) -> int:
        """Randomly pick a zone (uniform distribution)"""
        if not self.zones:
            raise ValueError("No zones available to pick")
        return self.rng.choice(self.zones)
    
    def pick_payment_method(self) -> int:
        """Randomly pick a payment method"""
        if not self.payment_methods:
            raise ValueError("No payment methods available to pick")
        return self.rng.choice(self.payment_methods)["id"]
    
    def vehicle_for_driver(self, driver_id: int) -> Optional[Dict]:
        """
        Get the vehicle assigned to a specific driver
        
        Note: In your schema, there's a 1:1 relationship (vehicle_id = driver_id)
        """
        for vehicle in self.vehicles:
            if vehicle["driver_id"] == driver_id:
                return vehicle
        logger.warning(f"No vehicle found for driver_id {driver_id}")
        return None
    
    def get_vehicle_type(self, vehicle_type_id: int) -> Optional[Dict]:
        """Get vehicle type details"""
        return self.vehicle_types.get(vehicle_type_id)
    
    def get_membership_tier(self, tier_id: int) -> Optional[Dict]:
        """Get membership tier details"""
        return self.membership_tiers.get(tier_id)
    
    def get_payment_method(self, method_id: int) -> Optional[Dict]:
        """Get payment method details"""
        for method in self.payment_methods:
            if method["id"] == method_id:
                return method
        return None
    
    def get_ride_status(self, status_id: int) -> Optional[str]:
        """Get ride status name"""
        return self.ride_statuses.get(status_id)