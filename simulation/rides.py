"""
Build every message one ride produces.

  ride_events     lifecycle events for hub `ride-events` (v1 defects applied)
  route_plan      one message for hub `ride-gps`, when the trip starts
  gps_pings       every 10 s for hub `ride-gps`
  incident_labels ground truth for training (never on the stream)
  behaviors       everything that happened, for error analysis

Each stream message carries the time it should be sent (`emit_at`), so the
live runner can replay a ride in real time instead of sending it at once.
"""

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, List, Optional

from database.generators.defect_injector import DefectInjector
from database.generators.ride_event_generator import RideEventGenerator
from simulation.gps import GpsGenerator

RIDE_EVENTS_HUB = "ride-events"
RIDE_GPS_HUB = "ride-gps"


def parse_ts(value: str) -> datetime:
    """Timestamps from both streams: v1 events have no zone, v2 messages end with Z (both UTC)."""
    return datetime.fromisoformat(value[:-1] if value.endswith("Z") else value)


@dataclass
class StreamMessage:
    hub: str
    ride_id: str
    emit_at: datetime
    body: Dict


@dataclass
class RideMessages:
    ride_id: str
    stream: List[StreamMessage] = field(default_factory=list)
    incident_labels: List[Dict] = field(default_factory=list)
    behaviors: List[Dict] = field(default_factory=list)

    def bodies(self, hub: str, message_type: Optional[str] = None) -> List[Dict]:
        return [
            m.body for m in self.stream
            if m.hub == hub and (message_type is None or m.body.get("message_type") == message_type)
        ]


class RideFactory:

    def __init__(self, generator: RideEventGenerator, defect_injector: Optional[DefectInjector], gps: GpsGenerator):
        self.generator = generator
        self.defect_injector = defect_injector
        self.gps = gps

    def make(self, birth_ts: datetime) -> Optional[RideMessages]:
        events, trip = self.generator.generate_ride_with_trip(birth_ts)
        if not events:
            return None
        ride_id = events[0]["ride_id"]
        out = RideMessages(ride_id)

        # Driver comes from the clean events, before defects can null it out
        driver_id = next((e["driver_id"] for e in events if e.get("driver_id") is not None), None)

        sent_events = self.defect_injector.inject(events) if self.defect_injector else events
        for event in sent_events:
            emit_at = self._event_time(event, birth_ts) + timedelta(seconds=event.get("_emit_delay_s") or 0)
            # Fields starting with "_" are simulator internals (late-send delay,
            # incomplete-ride marker). Sending them would leak the answer to the
            # pipeline and the model, so they never leave the simulator.
            body = {k: v for k, v in event.items() if not k.startswith("_")}
            out.stream.append(StreamMessage(RIDE_EVENTS_HUB, ride_id, emit_at, body))

        if trip is not None:
            plan = self.gps.route_plan_message(trip, ride_id, driver_id)
            out.stream.append(StreamMessage(RIDE_GPS_HUB, ride_id, parse_ts(plan["planned_at"]), plan))
            for ping in self.gps.pings(trip, ride_id, driver_id):
                out.stream.append(StreamMessage(RIDE_GPS_HUB, ride_id, parse_ts(ping["ping_ts"]), ping))
            out.incident_labels = self.gps.incident_labels(trip, ride_id)
            out.behaviors = self.gps.behavior_records(trip, ride_id)
        return out

    @staticmethod
    def _event_time(event: Dict, birth_ts: datetime) -> datetime:
        """
        Events are sent when they happen. A late-arrival defect adds
        `_emit_delay_s` on top; a corrupted timestamp falls back to the ride start.
        """
        try:
            return parse_ts(event["event_timestamp"])
        except (KeyError, TypeError, ValueError):
            return birth_ts

