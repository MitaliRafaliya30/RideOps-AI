"""
Timing model for RideOps AI Event Simulator

Defines realistic time delays between state transitions in a ride lifecycle.
All times in seconds.
"""

import random
from random import Random
from typing import Tuple


class TimingModel:
    """Realistic timing for ride state transitions"""
    
    def __init__(self, rng: Random):
        """
        Initialize timing model with seeded RNG
        
        Args:
            rng: Random instance (seeded for reproducibility)
        """
        self.rng = rng
    
    # ==================== TIMING WINDOWS ====================
    # All in seconds
    
    REQUEST_TO_ASSIGN = (5, 30)           # Driver finds and accepts
    ASSIGN_TO_ACCEPT = (5, 10)            # Driver confirms acceptance
    ACCEPT_TO_ARRIVE = (20, 120)          # Driver travels to pickup
    ARRIVE_TO_START = (10, 30)            # Passenger gets in car
    
    # START_TO_COMPLETED calculated from trip distance/speed
    # (not a timing window, derived from trip_time)
    
    # ==================== CANCELLATION DISTRIBUTION ====================
    
    COMPLETION_RATE = 0.85                # 85% complete, 15% cancel
    
    # When do cancellations happen?
    # 70% at arrived (passenger no-show)
    # 20% at accepted (driver changes mind)
    # 10% at assigned (no driver)
    CANCELLATION_STAGES = {
        "arrived": 0.70,
        "accepted": 0.20,
        "assigned": 0.10,
    }
    
    # ==================== DISTANCE ESTIMATION ====================
    
    AVERAGE_SPEED_MPH = 15               # Average speed in NYC (with traffic)
    SPEED_VARIATION = 0.2                # ±20% variation
    
    # ==================== TIMING METHODS ====================
    
    def time_request_to_assign(self) -> int:
        """Time from request to driver assignment (seconds)"""
        return self.rng.randint(self.REQUEST_TO_ASSIGN[0], self.REQUEST_TO_ASSIGN[1])
    
    def time_assign_to_accept(self) -> int:
        """Time from assignment to driver acceptance (seconds)"""
        return self.rng.randint(self.ASSIGN_TO_ACCEPT[0], self.ASSIGN_TO_ACCEPT[1])
    
    def time_accept_to_arrive(self) -> int:
        """Time from acceptance to driver arrival at pickup (seconds)"""
        return self.rng.randint(self.ACCEPT_TO_ARRIVE[0], self.ACCEPT_TO_ARRIVE[1])
    
    def time_arrive_to_start(self) -> int:
        """Time from driver arrival to ride start (passenger boarding) (seconds)"""
        return self.rng.randint(self.ARRIVE_TO_START[0], self.ARRIVE_TO_START[1])
    
    def time_trip(self, trip_miles: float) -> int:
        """
        Calculate trip duration from distance
        
        Formula: distance / speed * 3600 (seconds), plus jitter
        
        Args:
            trip_miles: Distance in miles
        
        Returns:
            Trip duration in seconds
        """
        # Base time: distance / speed * 3600
        base_time_seconds = (trip_miles / self.AVERAGE_SPEED_MPH) * 3600
        
        # Add jitter: ±20%
        jitter = self.rng.uniform(-self.SPEED_VARIATION, self.SPEED_VARIATION)
        jittered_time = base_time_seconds * (1 + jitter)
        
        return int(jittered_time)
    
    # ==================== COMPLETION/CANCELLATION ====================
    
    def should_complete_ride(self) -> bool:
        """
        Determine if ride completes or gets cancelled
        
        Returns:
            True if ride completes, False if cancelled
        """
        return self.rng.random() < self.COMPLETION_RATE
    
    def cancellation_stage(self) -> str:
        """
        Determine at which stage ride gets cancelled
        
        Returns:
            Stage name: 'assigned', 'accepted', or 'arrived'
        """
        stages = list(self.CANCELLATION_STAGES.keys())
        weights = list(self.CANCELLATION_STAGES.values())
        
        return self.rng.choices(stages, weights=weights, k=1)[0]
    
    # ==================== DISTANCE GENERATION ====================
    
    def trip_distance(self) -> float:
        """
        Generate realistic NYC trip distance in miles
        
        NYC taxi trips typically range from 0.5 to 20+ miles
        Most are 2-8 miles
        
        Returns:
            Distance in miles
        """
        # Use log-normal distribution (realistic for taxi distances)
        # This gives more weight to shorter trips with occasional long trips
        mu = 1.6      # Mean of log
        sigma = 0.7   # StdDev of log
        
        distance = self.rng.lognormvariate(mu, sigma)
        
        # Clamp to realistic range
        return max(0.5, min(distance, 25.0))