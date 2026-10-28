"""
Ride Event Generator for RideOps AI Simulator

Generates clean (defect-free) ride events by walking the state machine.
One ride = one lifecycle from request to completion/cancellation.
"""

import uuid
import logging
from datetime import datetime, timedelta
from decimal import Decimal
from random import Random
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

from database.utils.constants import EVENT_TYPES, CANCELLATION_REASONS
from database.utils.timing_model import TimingModel
from database.utils.fare_calculator import FareCalculator
from database.generators.entity_pool import EntityPool

if TYPE_CHECKING:
    from simulation.trip_planner import Trip, TripPlanner


logger = logging.getLogger(__name__)

# JFK (132) and LaGuardia (138): pickups or dropoffs here pay the airport fee
AIRPORT_ZONE_IDS = {132, 138}


class RideEventGenerator:
    """
    Clean ride event generator
    
    Generates complete ride lifecycles (sequences of events)
    without defects. One ride = 5-7 events depending on completion.
    """
    
    def __init__(self, entity_pool: EntityPool, config: Dict[str, Any], rng: Random,
                 trip_planner: Optional["TripPlanner"] = None):
        """
        Initialize generator

        Args:
            entity_pool: Loaded entity pool (passengers, drivers, vehicles, zones)
            config: Configuration dict from config_loader
            rng: Seeded Random instance
            trip_planner: v2 only. When set, zones follow the real NYC
                origin-destination pattern and completed trips drive a real
                road route (distance, duration and fare come from the route).
                When None, v1 behaviour: random zones and random distance.
        """
        self.pool = entity_pool
        self.config = config
        self.rng = rng
        self.timing = TimingModel(rng)
        self.trip_planner = trip_planner

        self.completion_rate = config.get("generation", {}).get("completion_rate", 0.85)

        logger.info("RideEventGenerator initialized")

    # ==================== MAIN GENERATION ====================

    def generate_ride(self, birth_timestamp: datetime) -> List[Dict[str, Any]]:
        """
        Generate one complete ride (sequence of events)

        Args:
            birth_timestamp: When the ride was requested (UTC datetime)

        Returns:
            List of event dicts, in chronological order
        """
        events, _ = self.generate_ride_with_trip(birth_timestamp)
        return events

    def generate_ride_with_trip(self, birth_timestamp: datetime) -> Tuple[List[Dict[str, Any]], Optional["Trip"]]:
        """
        Same as generate_ride, plus the driven Trip for completed rides.

        The Trip (route + timestamp per route point) is what the GPS
        generator replays. It is None for cancelled rides and in v1 mode.
        """

        ride_id = str(uuid.uuid4())
        events = []
        trip = None

        # Step 1: Requested event
        passenger = self.pool.pick_passenger()
        if self.trip_planner:
            pickup_zone, dropoff_zone = self.trip_planner.sample_od(self.rng)
        else:
            pickup_zone = self.pool.pick_zone()
            dropoff_zone = self.pool.pick_zone()
        payment_method = self.pool.pick_payment_method()
        
        requested_event = self._create_event(
            event_type="requested",
            ride_id=ride_id,
            timestamp=birth_timestamp,
            passenger_id=passenger["id"],
            driver_id=None,
            vehicle_id=None,
            pickup_zone_id=pickup_zone,
            dropoff_zone_id=dropoff_zone,
            payment_method_id=payment_method,
        )
        events.append(requested_event)
        
        # Step 2: Assigned event
        assign_wait = self.timing.time_request_to_assign()
        assigned_timestamp = birth_timestamp + timedelta(seconds=assign_wait)
        
        driver = self.pool.pick_driver()
        vehicle = self.pool.vehicle_for_driver(driver["id"])
        
        if not vehicle:
            # No vehicle for driver (shouldn't happen, but handle it)
            logger.warning(f"No vehicle for driver {driver['id']}, skipping ride")
            return [], None
        
        assigned_event = self._create_event(
            event_type="assigned",
            ride_id=ride_id,
            timestamp=assigned_timestamp,
            passenger_id=passenger["id"],
            driver_id=driver["id"],
            vehicle_id=vehicle["id"],
            pickup_zone_id=pickup_zone,
            dropoff_zone_id=dropoff_zone,
            payment_method_id=payment_method,
        )
        events.append(assigned_event)
        
        # Step 3: Determine if ride completes or cancels.
        # Uses generation.completion_rate from config (TimingModel's own
        # constant used to win here, so the config value had no effect).
        if not self.rng.random() < self.completion_rate:
            # Ride gets cancelled
            cancel_stage = self.timing.cancellation_stage()
            
            if cancel_stage == "assigned":
                # Cancel at assigned (driver never responded)
                cancel_timestamp = assigned_timestamp + timedelta(seconds=30)
                cancellation_reason_id = self.rng.choice(list(CANCELLATION_REASONS.keys()))
                
                cancelled_event = self._create_event(
                    event_type="cancelled",
                    ride_id=ride_id,
                    timestamp=cancel_timestamp,
                    passenger_id=passenger["id"],
                    driver_id=driver["id"],  # Driver was assigned
                    vehicle_id=None,
                    pickup_zone_id=pickup_zone,
                    dropoff_zone_id=dropoff_zone,
                    payment_method_id=payment_method,
                    cancellation_reason_id=cancellation_reason_id,
                )
                events.append(cancelled_event)
                
            elif cancel_stage == "accepted":
                # Cancel at accepted (driver accepted, then cancelled)
                accept_wait = self.timing.time_assign_to_accept()
                accepted_timestamp = assigned_timestamp + timedelta(seconds=accept_wait)
                
                accepted_event = self._create_event(
                    event_type="accepted",
                    ride_id=ride_id,
                    timestamp=accepted_timestamp,
                    passenger_id=passenger["id"],
                    driver_id=driver["id"],
                    vehicle_id=vehicle["id"],
                    pickup_zone_id=pickup_zone,
                    dropoff_zone_id=dropoff_zone,
                    payment_method_id=payment_method,
                )
                events.append(accepted_event)
                
                # Cancel shortly after
                cancel_timestamp = accepted_timestamp + timedelta(seconds=60)
                cancellation_reason_id = self.rng.choice(list(CANCELLATION_REASONS.keys()))
                
                cancelled_event = self._create_event(
                    event_type="cancelled",
                    ride_id=ride_id,
                    timestamp=cancel_timestamp,
                    passenger_id=passenger["id"],
                    driver_id=driver["id"],
                    vehicle_id=vehicle["id"],
                    pickup_zone_id=pickup_zone,
                    dropoff_zone_id=dropoff_zone,
                    payment_method_id=payment_method,
                    cancellation_reason_id=cancellation_reason_id,
                )
                events.append(cancelled_event)
                
            elif cancel_stage == "arrived":
                # Cancel at arrived (driver at pickup, passenger no-show)
                accept_wait = self.timing.time_assign_to_accept()
                accepted_timestamp = assigned_timestamp + timedelta(seconds=accept_wait)
                
                accepted_event = self._create_event(
                    event_type="accepted",
                    ride_id=ride_id,
                    timestamp=accepted_timestamp,
                    passenger_id=passenger["id"],
                    driver_id=driver["id"],
                    vehicle_id=vehicle["id"],
                    pickup_zone_id=pickup_zone,
                    dropoff_zone_id=dropoff_zone,
                    payment_method_id=payment_method,
                )
                events.append(accepted_event)
                
                # Driver travels to pickup
                arrive_wait = self.timing.time_accept_to_arrive()
                arrived_timestamp = accepted_timestamp + timedelta(seconds=arrive_wait)
                
                arrived_event = self._create_event(
                    event_type="arrived",
                    ride_id=ride_id,
                    timestamp=arrived_timestamp,
                    passenger_id=passenger["id"],
                    driver_id=driver["id"],
                    vehicle_id=vehicle["id"],
                    pickup_zone_id=pickup_zone,
                    dropoff_zone_id=dropoff_zone,
                    payment_method_id=payment_method,
                )
                events.append(arrived_event)
                
                # Cancel (no-show)
                cancel_timestamp = arrived_timestamp + timedelta(seconds=120)
                cancellation_reason_id = 4  # "Passenger No Show"
                
                cancelled_event = self._create_event(
                    event_type="cancelled",
                    ride_id=ride_id,
                    timestamp=cancel_timestamp,
                    passenger_id=passenger["id"],
                    driver_id=driver["id"],
                    vehicle_id=vehicle["id"],
                    pickup_zone_id=pickup_zone,
                    dropoff_zone_id=dropoff_zone,
                    payment_method_id=payment_method,
                    cancellation_reason_id=cancellation_reason_id,
                )
                events.append(cancelled_event)
            
            return events, None
        
        # Ride completes (not cancelled)
        # Step 4: Accepted
        accept_wait = self.timing.time_assign_to_accept()
        accepted_timestamp = assigned_timestamp + timedelta(seconds=accept_wait)
        
        accepted_event = self._create_event(
            event_type="accepted",
            ride_id=ride_id,
            timestamp=accepted_timestamp,
            passenger_id=passenger["id"],
            driver_id=driver["id"],
            vehicle_id=vehicle["id"],
            pickup_zone_id=pickup_zone,
            dropoff_zone_id=dropoff_zone,
            payment_method_id=payment_method,
        )
        events.append(accepted_event)
        
        # Step 5: Arrived
        arrive_wait = self.timing.time_accept_to_arrive()
        arrived_timestamp = accepted_timestamp + timedelta(seconds=arrive_wait)
        
        arrived_event = self._create_event(
            event_type="arrived",
            ride_id=ride_id,
            timestamp=arrived_timestamp,
            passenger_id=passenger["id"],
            driver_id=driver["id"],
            vehicle_id=vehicle["id"],
            pickup_zone_id=pickup_zone,
            dropoff_zone_id=dropoff_zone,
            payment_method_id=payment_method,
        )
        events.append(arrived_event)
        
        # Step 6: Started
        start_wait = self.timing.time_arrive_to_start()
        started_timestamp = arrived_timestamp + timedelta(seconds=start_wait)
        
        started_event = self._create_event(
            event_type="started",
            ride_id=ride_id,
            timestamp=started_timestamp,
            passenger_id=passenger["id"],
            driver_id=driver["id"],
            vehicle_id=vehicle["id"],
            pickup_zone_id=pickup_zone,
            dropoff_zone_id=dropoff_zone,
            payment_method_id=payment_method,
        )
        events.append(started_event)
        
        # Step 7: Completed
        # v2: distance and duration come from the real road route at this
        # hour's traffic. v1: random distance, fixed average speed.
        if self.trip_planner:
            trip = self.trip_planner.build_trip(pickup_zone, dropoff_zone, started_timestamp, self.rng)
            trip_miles = round(trip.distance_miles, 2)
            trip_duration_seconds = int(round(trip.duration_s))
        else:
            trip_miles = self.timing.trip_distance()
            trip_duration_seconds = self.timing.time_trip(trip_miles)

        completed_timestamp = started_timestamp + timedelta(seconds=trip_duration_seconds)

        # Calculate fare
        is_airport = pickup_zone in AIRPORT_ZONE_IDS or dropoff_zone in AIRPORT_ZONE_IDS
        has_congestion = pickup_zone in [41, 42, 43] or dropoff_zone in [41, 42, 43]
        
        fare_breakdown = FareCalculator.calculate_fare(
            trip_miles=trip_miles,
            trip_seconds=trip_duration_seconds,
            has_congestion_surcharge=has_congestion,
            is_airport=is_airport,
            has_cbd_congestion=False,
            rng=self.rng,
        )
        
        completed_event = self._create_event(
            event_type="completed",
            ride_id=ride_id,
            timestamp=completed_timestamp,
            passenger_id=passenger["id"],
            driver_id=driver["id"],
            vehicle_id=vehicle["id"],
            pickup_zone_id=pickup_zone,
            dropoff_zone_id=dropoff_zone,
            payment_method_id=payment_method,
            trip_miles=trip_miles,
            fare_breakdown=fare_breakdown,
        )
        events.append(completed_event)
        
        return events, trip
    
    # ==================== EVENT CREATION ====================
    
    def _create_event(
        self,
        event_type: str,
        ride_id: str,
        timestamp: datetime,
        passenger_id: int,
        driver_id: int = None,
        vehicle_id: int = None,
        pickup_zone_id: int = None,
        dropoff_zone_id: int = None,
        payment_method_id: int = None,
        cancellation_reason_id: int = None,
        trip_miles: float = None,
        fare_breakdown: Dict[str, Decimal] = None,
    ) -> Dict[str, Any]:
        """
        Create a single event dict
        
        Args:
            All event fields as parameters
        
        Returns:
            Event dict ready for emission
        """
        
        event = {
            "event_id": str(uuid.uuid4()),
            "ride_id": ride_id,
            "event_type": event_type,
            "event_timestamp": timestamp.isoformat(),
            "passenger_id": passenger_id,
            "driver_id": driver_id,
            "vehicle_id": vehicle_id,
            "pickup_location_id": pickup_zone_id,
            "dropoff_location_id": dropoff_zone_id,
            "payment_method_id": payment_method_id,
            "cancellation_reason_id": cancellation_reason_id,
            "trip_miles": trip_miles,
        }
        
        # Add fare components if completed
        if fare_breakdown:
            event.update({
                "base_passenger_fare": float(fare_breakdown["base_passenger_fare"]),
                "tolls": float(fare_breakdown["tolls"]),
                "bcf": float(fare_breakdown["bcf"]),
                "sales_tax": float(fare_breakdown["sales_tax"]),
                "congestion_surcharge": float(fare_breakdown["congestion_surcharge"]),
                "airport_fee": float(fare_breakdown["airport_fee"]),
                "cbd_congestion_fee": float(fare_breakdown["cbd_congestion_fee"]),
                "tips": float(fare_breakdown["tips"]),
                "driver_pay": float(fare_breakdown["driver_pay"]),
            })
        else:
            # Null fares for non-completed events
            event.update({
                "base_passenger_fare": None,
                "tolls": None,
                "bcf": None,
                "sales_tax": None,
                "congestion_surcharge": None,
                "airport_fee": None,
                "cbd_congestion_fee": None,
                "tips": None,
                "driver_pay": None,
            })
        
        return event