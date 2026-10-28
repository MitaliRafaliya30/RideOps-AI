"""
Turn a driven Trip into the messages a driver's phone would send.

  route_plan  once, when the trip starts (docs/DATA_DESIGN.md, section 2.2)
  gps_ping    every `interval_s` seconds until the dropoff (section 2.3)

Position at any moment is interpolated along the route using the trip's
per-point timestamps, so the car is slow in traffic and fast on highways.
Real GPS is not exact: each reported fix is shifted by a few meters of
random error, and the phone reports how accurate it thinks it is. During
a dropout no pings are sent; during spoofing the reported position is fake.

Ground truth (incident labels, all behaviours) is produced here too, but
goes to a separate training store, never onto the stream.
"""

import json
import math
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from random import Random
from typing import Dict, List, Tuple

from geodata.router import haversine_m
from simulation.timeline import segment_at
from simulation.trip_planner import Trip

SCHEMA_VERSION = 1
METERS_PER_DEG_LAT = 111_320.0


def utc_iso(ts: datetime) -> str:
    """ISO-8601 in UTC with a Z suffix (naive datetimes are treated as UTC)."""
    if ts.tzinfo is not None:
        ts = ts.astimezone(timezone.utc).replace(tzinfo=None)
    return ts.isoformat(timespec="milliseconds") + "Z"


def bearing_deg(a: Tuple[float, float], b: Tuple[float, float]) -> float:
    """Compass heading from a to b: 0 = north, 90 = east."""
    lat1, lat2 = math.radians(a[0]), math.radians(b[0])
    dlon = math.radians(b[1] - a[1])
    x = math.sin(dlon) * math.cos(lat2)
    y = math.cos(lat1) * math.sin(lat2) - math.sin(lat1) * math.cos(lat2) * math.cos(dlon)
    return (math.degrees(math.atan2(x, y)) + 360) % 360


@dataclass
class Fix:
    """True state of the car at one moment, before GPS error is added."""
    elapsed_s: float
    lat: float
    lon: float
    speed_kmh: float
    heading_deg: float


class TripTrack:
    """Where the car is, how fast, and which way, at any time during a trip."""

    def __init__(self, trip: Trip):
        self.coords = trip.track_coords
        self.times = trip.track_times_s

    def at(self, elapsed_s: float) -> Fix:
        elapsed_s = min(max(elapsed_s, 0.0), self.times[-1])
        i, f = segment_at(self.coords, self.times, elapsed_s)
        a, b = self.coords[i], self.coords[i + 1]
        span = self.times[i + 1] - self.times[i]
        lat = a[0] + (b[0] - a[0]) * f
        lon = a[1] + (b[1] - a[1]) * f
        speed = haversine_m(a, b) / span * 3.6 if span > 0 else 0.0
        return Fix(elapsed_s, lat, lon, speed, self._heading(i))

    def _heading(self, i: int) -> float:
        """Direction of travel; while stopped, keep the last direction moved."""
        for j in range(i, -1, -1):
            a, b = self.coords[j], self.coords[j + 1]
            if a != b:
                return bearing_deg(a, b)
        return 0.0


class GpsGenerator:

    def __init__(self, rng: Random, interval_s: float = 10.0, noise_m: float = 4.0, speed_noise: float = 0.05):
        self.rng = rng
        self.interval_s = interval_s
        self.noise_m = noise_m              # std dev of the GPS position error
        self.speed_noise = speed_noise      # relative std dev of reported speed

    # ---------------- route plan ----------------

    def route_plan_message(self, trip: Trip, ride_id: str, driver_id: int) -> Dict:
        pickup, dropoff = trip.route.coords[0], trip.route.coords[-1]
        return {
            "message_type": "route_plan",
            "schema_version": SCHEMA_VERSION,
            "message_id": str(uuid.UUID(int=self.rng.getrandbits(128))),
            "ride_id": ride_id,
            "driver_id": driver_id,
            "planned_at": utc_iso(trip.start_ts),
            "pickup_lat": pickup[0],
            "pickup_lon": pickup[1],
            "dropoff_lat": dropoff[0],
            "dropoff_lon": dropoff[1],
            "route_polyline": trip.route.encoded_polyline,
            "planned_distance_m": round(trip.distance_m, 1),
            "planned_duration_s": int(round(trip.planned_duration_s)),
        }

    # ---------------- pings ----------------

    def ping_times(self, trip: Trip) -> List[float]:
        """
        Every interval from the start, plus one final ping at the dropoff.
        Nothing is sent during a GPS dropout (no fix in a tunnel).
        """
        times, t = [], 0.0
        while t < trip.duration_s:
            if t == 0.0 or not any(start <= t < end for start, end in trip.dropout_windows):
                times.append(t)
            t += self.interval_s
        times.append(trip.duration_s)
        return times

    def ping_message(self, trip: Trip, ride_id: str, driver_id: int, seq_no: int, fix: Fix) -> Dict:
        lat, lon = self._add_position_error(fix.lat, fix.lon)
        for start, end, dlat, dlon in trip.spoof_windows:
            if start <= fix.elapsed_s < end:
                lat, lon = lat + dlat, lon + dlon        # spoofed: phone reports a fake place
        speed = max(0.0, fix.speed_kmh * (1 + self.rng.gauss(0, self.speed_noise)))
        return {
            "message_type": "gps_ping",
            "schema_version": SCHEMA_VERSION,
            "message_id": str(uuid.UUID(int=self.rng.getrandbits(128))),
            "ride_id": ride_id,
            "driver_id": driver_id,
            "seq_no": seq_no,
            "ping_ts": utc_iso(trip.start_ts + timedelta(seconds=fix.elapsed_s)),
            "lat": round(lat, 6),
            "lon": round(lon, 6),
            "speed_kmh": round(speed, 1),
            "heading_deg": round(fix.heading_deg, 1),
            "accuracy_m": round(max(self.noise_m, 1.0) * self.rng.uniform(0.8, 2.0), 1),
        }

    def pings(self, trip: Trip, ride_id: str, driver_id: int) -> List[Dict]:
        track = TripTrack(trip)
        return [
            self.ping_message(trip, ride_id, driver_id, seq_no, track.at(t))
            for seq_no, t in enumerate(self.ping_times(trip), start=1)
        ]

    # ---------------- ground truth ----------------

    def incident_labels(self, trip: Trip, ride_id: str) -> List[Dict]:
        """Training labels (docs/DATA_DESIGN.md, section 2.4). Never sent on the stream."""
        return [
            {
                "incident_id": str(uuid.UUID(int=self.rng.getrandbits(128))),
                "ride_id": ride_id,
                "incident_type": b.kind,
                "start_ts": utc_iso(trip.start_ts + timedelta(seconds=b.start_s)),
                "end_ts": utc_iso(trip.start_ts + timedelta(seconds=b.end_s)) if b.end_s is not None else None,
                "severity": b.severity,
                "params": json.dumps(b.params),
            }
            for b in trip.incidents
        ]

    def behavior_records(self, trip: Trip, ride_id: str) -> List[Dict]:
        """Everything that happened on the trip, incidents and normal behaviour, for error analysis."""
        return [
            {
                "ride_id": ride_id,
                "kind": b.kind,
                "is_incident": b.is_incident,
                "start_ts": utc_iso(trip.start_ts + timedelta(seconds=b.start_s)),
                "end_ts": utc_iso(trip.start_ts + timedelta(seconds=b.end_s)) if b.end_s is not None else None,
                "params": json.dumps(b.params),
            }
            for b in trip.behaviors
        ]

    def _add_position_error(self, lat: float, lon: float) -> Tuple[float, float]:
        if not self.noise_m:
            return lat, lon
        north_m = self.rng.gauss(0, self.noise_m)
        east_m = self.rng.gauss(0, self.noise_m)
        return (
            lat + north_m / METERS_PER_DEG_LAT,
            lon + east_m / (METERS_PER_DEG_LAT * math.cos(math.radians(lat))),
        )
