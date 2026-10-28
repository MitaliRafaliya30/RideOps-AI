"""
Turn a driven Trip into the messages a driver's phone would send.

  route_plan  once, when the trip starts (docs/DATA_DESIGN.md, section 2.2)
  gps_ping    every `interval_s` seconds until the dropoff (section 2.3)

Position at any moment is interpolated along the route using the trip's
per-point timestamps, so the car is slow in traffic and fast on highways.
Real GPS is not exact: each reported fix is shifted by a few meters of
random error, and the phone reports how accurate it thinks it is.
"""

import bisect
import math
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from random import Random
from typing import Dict, List, Tuple

from geodata.router import haversine_m
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
        self.coords = trip.route.coords
        self.times = trip.point_times_s

    def at(self, elapsed_s: float) -> Fix:
        elapsed_s = min(max(elapsed_s, 0.0), self.times[-1])
        i = bisect.bisect_right(self.times, elapsed_s) - 1
        i = min(i, len(self.coords) - 2)
        a, b = self.coords[i], self.coords[i + 1]
        t0, t1 = self.times[i], self.times[i + 1]

        span = t1 - t0
        f = (elapsed_s - t0) / span if span > 0 else 1.0
        lat = a[0] + (b[0] - a[0]) * f
        lon = a[1] + (b[1] - a[1]) * f
        speed = haversine_m(a, b) / span * 3.6 if span > 0 else 0.0
        return Fix(elapsed_s, lat, lon, speed, bearing_deg(a, b))


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
        """Every interval from the start, plus one final ping at the dropoff."""
        times, t = [], 0.0
        while t < trip.duration_s:
            times.append(t)
            t += self.interval_s
        times.append(trip.duration_s)
        return times

    def ping_message(self, trip: Trip, ride_id: str, driver_id: int, seq_no: int, fix: Fix) -> Dict:
        lat, lon = self._add_position_error(fix.lat, fix.lon)
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

    def _add_position_error(self, lat: float, lon: float) -> Tuple[float, float]:
        if not self.noise_m:
            return lat, lon
        north_m = self.rng.gauss(0, self.noise_m)
        east_m = self.rng.gauss(0, self.noise_m)
        return (
            lat + north_m / METERS_PER_DEG_LAT,
            lon + east_m / (METERS_PER_DEG_LAT * math.cos(math.radians(lat))),
        )
