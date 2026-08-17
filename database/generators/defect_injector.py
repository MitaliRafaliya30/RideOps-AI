"""
Defect Injector for RideOps AI Event Simulator

Injects 8 realistic defects into clean ride event streams.
Sits DOWNSTREAM of the generator - wraps it without modifying it.
"""

import logging
import random
from random import Random
from typing import List, Dict, Any
from decimal import Decimal

from database.utils.constants import EVENT_TYPES, CANCELLATION_REASONS

logger = logging.getLogger(__name__)


class DefectInjector:
    """
    Injects defects into clean ride streams
    
    Defect rates are configurable (0-1.0).
    All defects use the same seeded RNG for reproducibility.
    Each defect is independent (can combine with others).
    """
    
    def __init__(self, config: Dict[str, Any], rng: Random):
        """
        Initialize defect injector
        
        Args:
            config: Configuration dict with defect rates
            rng: Seeded Random instance for reproducibility
        """
        self.config = config
        self.rng = rng
        
        # Extract defect settings from config
        defects_cfg = config.get("defects", {})
        self.defects_enabled = defects_cfg.get("enabled", False)
        
        self.defect_1 = defects_cfg.get("defect_1", {})
        self.defect_2 = defects_cfg.get("defect_2", {})
        self.defect_3 = defects_cfg.get("defect_3", {})
        self.defect_4 = defects_cfg.get("defect_4", {})
        self.defect_5 = defects_cfg.get("defect_5", {})
        self.defect_6 = defects_cfg.get("defect_6", {})
        self.defect_7 = defects_cfg.get("defect_7", {})
        self.defect_8 = defects_cfg.get("defect_8", {})
        
        logger.info("DefectInjector initialized")
        if self.defects_enabled:
            logger.info(f"Defects enabled: {sum([d.get('enabled', True) for d in [self.defect_1, self.defect_2, self.defect_3, self.defect_4, self.defect_5, self.defect_6, self.defect_7, self.defect_8]])}/8")
    
    # ==================== MAIN INJECTION ====================
    
    def inject(self, ride: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Take a clean ride, inject defects, return corrupted ride
        
        Args:
            ride: List of event dicts (from clean generator)
        
        Returns:
            List of event dicts with defects applied
        """
        
        if not self.defects_enabled or not ride:
            return ride
        
        # Copy ride to avoid modifying original
        events = [dict(e) for e in ride]
        
        # Apply defects in order
        # Each defect decides independently whether to hit this ride
        
        # Defect 1: Out-of-order (shuffle within same ride)
        if self._should_inject(self.defect_1):
            events = self._inject_out_of_order(events)
        
        # Defect 2: Late arrival (hold terminal event)
        if self._should_inject(self.defect_2):
            events = self._inject_late_arrival(events)
        
        # Defect 3: Exact duplicate (re-emit same event)
        if self._should_inject(self.defect_3):
            events = self._inject_exact_duplicate(events)
        
        # Defect 4: Near-duplicate (new event_id, same ride+type)
        if self._should_inject(self.defect_4):
            events = self._inject_near_duplicate(events)
        
        # Defect 5: Null required field
        if self._should_inject(self.defect_5):
            events = self._inject_null_field(events)
        
        # Defect 6: Invalid event_type
        if self._should_inject(self.defect_6):
            events = self._inject_invalid_type(events)
        
        # Defect 7: Out-of-range values
        if self._should_inject(self.defect_7):
            events = self._inject_out_of_range(events)
        
        # Defect 8: Incomplete ride (no terminal)
        if self._should_inject(self.defect_8):
            events = self._inject_incomplete_ride(events)
        
        return events
    
    # ==================== HELPER METHODS ====================
    
    def _should_inject(self, defect_cfg: Dict) -> bool:
        """Determine if defect should be injected based on rate"""
        if not defect_cfg.get("enabled", True):
            return False
        
        rate = defect_cfg.get("rate", 0)
        return self.rng.random() < rate
    
    # ==================== DEFECT 1: OUT-OF-ORDER ====================
    
    def _inject_out_of_order(self, events: List[Dict]) -> List[Dict]:
        """
        Defect 1: Shuffle events within a window

        Simulates: events arriving out of order on the stream
        Impact: Silver layer must reorder by event_timestamp
        """

        if len(events) <= 2:
            return events

        window_size = self.defect_1.get("shuffle_window_size", 5)

        # Bound window_size to available events
        max_window_size = max(1, len(events) - 2)
        window_size = min(window_size, max_window_size)

        # Shuffle a window of events (not the first/last)
        max_start = len(events) - window_size - 1
        if max_start < 0:
            max_start = 0
        start_idx = max(1, self.rng.randint(0, max_start)) if max_start > 0 else 1
        end_idx = min(len(events) - 1, start_idx + window_size)

        window = events[start_idx:end_idx]
        self.rng.shuffle(window)

        events[start_idx:end_idx] = window

        return events
    
    # ==================== DEFECT 2: LATE ARRIVAL ====================
    
    def _inject_late_arrival(self, events: List[Dict]) -> List[Dict]:
        """
        Defect 2: Simulate late-arriving terminal event

        Simulates: Network delay causing final event timestamp to be much later
        than when it was actually emitted (event appears out of order)
        Impact: Silver layer must handle late arrivals with watermarking
        """

        if len(events) < 2:
            return events

        # Get the last (terminal) event
        terminal = events[-1]

        # Increase its timestamp to simulate arrival delay
        severity = self.defect_2.get("severity", "low")
        delay_range = self.defect_2.get("holdback_delay_seconds", {"min": 60, "max": 180})

        if severity == "high":
            delay_range = {"min": 300, "max": 900}  # 5-15 minutes

        delay = self.rng.randint(delay_range.get("min", 60), delay_range.get("max", 180))

        # Parse timestamp and add delay
        from datetime import datetime, timedelta
        ts = datetime.fromisoformat(terminal["event_timestamp"])
        delayed_ts = ts + timedelta(seconds=delay)
        terminal["event_timestamp"] = delayed_ts.isoformat()

        return events
    
    # ==================== DEFECT 3: EXACT DUPLICATE ====================
    
    def _inject_exact_duplicate(self, events: List[Dict]) -> List[Dict]:
        """
        Defect 3: Re-emit same event identically
        
        Simulates: Duplicate message from queue/broker
        Impact: Silver layer must dedupe on event_id
        """
        
        if not events:
            return events
        
        # Pick random event to duplicate
        event_to_dup = self.rng.choice(events)
        
        # Insert duplicate after original
        idx = events.index(event_to_dup)
        events.insert(idx + 1, dict(event_to_dup))
        
        return events
    
    # ==================== DEFECT 4: NEAR-DUPLICATE ====================
    
    def _inject_near_duplicate(self, events: List[Dict]) -> List[Dict]:
        """
        Defect 4: New event_id, same ride_id + event_type
        
        Simulates: Retry with new ID but same semantic event
        Impact: Silver layer must dedupe on (ride_id, event_type)
        """
        
        if not events:
            return events
        
        # Pick random event to near-duplicate
        event_to_dup = self.rng.choice(events)
        
        # Create copy with new event_id
        duplicate = dict(event_to_dup)
        import uuid
        duplicate["event_id"] = str(uuid.uuid4())
        
        # Slightly offset timestamp
        from datetime import datetime, timedelta
        ts = datetime.fromisoformat(duplicate["event_timestamp"])
        gap = self.defect_4.get("timestamp_gap_seconds", {"min": 0.1, "max": 2.0})
        offset = self.rng.uniform(gap.get("min", 0.1), gap.get("max", 2.0))
        offset_ts = ts + timedelta(seconds=offset)
        duplicate["event_timestamp"] = offset_ts.isoformat()
        
        # Insert after original
        idx = events.index(event_to_dup)
        events.insert(idx + 1, duplicate)
        
        return events
    
    # ==================== DEFECT 5: NULL REQUIRED FIELD ====================
    
    def _inject_null_field(self, events: List[Dict]) -> List[Dict]:
        """
        Defect 5: Null out a required field
        
        Simulates: Malformed event missing critical data
        Impact: Silver layer quarantines event (fails validation)
        """
        
        if not events:
            return events
        
        # Pick random event
        event = self.rng.choice(events)
        
        # Pick required field for this event type
        event_type = event.get("event_type")
        
        # Fields that can be null without breaking everything
        nullable_fields = {
            "requested": ["payment_method_id"],
            "assigned": ["driver_id", "vehicle_id"],
            "accepted": ["driver_id"],
            "arrived": ["driver_id"],
            "started": ["driver_id"],
            "completed": ["trip_miles"],
            "cancelled": ["cancellation_reason_id"],
        }
        
        fields = nullable_fields.get(event_type, ["driver_id"])
        if fields:
            field = self.rng.choice(fields)
            event[field] = None
        
        return events
    
    # ==================== DEFECT 6: INVALID EVENT_TYPE ====================
    
    def _inject_invalid_type(self, events: List[Dict]) -> List[Dict]:
        """
        Defect 6: Replace event_type with invalid value
        
        Simulates: Corrupted event_type
        Impact: Silver layer quarantines (invalid enum)
        """
        
        if not events:
            return events
        
        # Pick random event (but not first)
        event = self.rng.choice(events)
        
        # Invalid types
        invalid_types = ["bounced", "lost", "zombie", "phantom", "corrupted", "unknown"]
        event["event_type"] = self.rng.choice(invalid_types)
        
        return events
    
    # ==================== DEFECT 7: OUT-OF-RANGE VALUES ====================
    
    def _inject_out_of_range(self, events: List[Dict]) -> List[Dict]:
        """
        Defect 7: Negative or absurd values in fares/distance
        
        Simulates: Data corruption in calculated fields
        Impact: Silver layer quarantines (logic validation fails)
        """
        
        if not events:
            return events
        
        # Only applies to completed events
        completed_events = [e for e in events if e.get("event_type") == "completed"]
        if not completed_events:
            return events
        
        event = self.rng.choice(completed_events)
        
        # Corrupt a fare or distance field
        corrupt_fields = ["trip_miles", "base_passenger_fare", "tips", "driver_pay"]
        corrupt_fields = [f for f in corrupt_fields if f in event and event[f] is not None]
        
        if corrupt_fields:
            field = self.rng.choice(corrupt_fields)
            
            # Negative value or absurdly high
            if self.rng.random() < 0.5:
                event[field] = str(Decimal("-10.50"))  # Negative
            else:
                event[field] = str(Decimal("99999.99"))  # Absurdly high
        
        return events
    
    # ==================== DEFECT 8: INCOMPLETE RIDE ====================
    
    def _inject_incomplete_ride(self, events: List[Dict]) -> List[Dict]:
        """
        Defect 8: Stop emitting before terminal state
        
        Simulates: Ride never reaches completion/cancellation (in-flight forever)
        Impact: Silver layer flags as stale, tracks incomplete rides
        """
        
        if len(events) <= 2:
            return events
        
        # Remove terminal event
        terminal = events[-1]
        
        # Only if it's actually terminal
        if terminal.get("event_type") in ["completed", "cancelled"]:
            events = events[:-1]
            
            # Optionally add a flag
            if events:
                events[-1]["_incomplete_ride"] = True
        
        return events
    
    # ==================== DEFECT STACKING RULES ====================
    
    # Note: In v1, we allow most defects to stack (combine).
    # Some combinations don't make sense:
    # - Invalid type + null type cannot combine (nonsensical)
    # - Out-of-range + late cannot combine (we skip in config)
    #
    # But these are handled by configuration, not code.
    # If defect rates sum to > 100%, rides will have multiple defects.