"""
Plan realistic trips: where people travel, which road they take, how long it takes.

  Calibration   real NYC origin-destination shares and traffic factors by hour
  TripPlanner   samples a pickup/dropoff pair, routes it on real roads and
                stretches free-flow time by the traffic of that hour

A Trip carries a timestamp for every point of its actual track, which the GPS
generator interpolates to produce pings.
"""

import bisect
import itertools
import math
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from random import Random
from typing import List, Optional, Tuple

import pandas as pd

from geodata.router import RoutePlan, Router, haversine_m
from simulation.config import OD_MATRIX_CSV, SPEED_BY_HOUR_CSV
from simulation.behaviors import BehaviorInjector, TripShape
from simulation.timeline import Behavior, Timeline

METERS_PER_MILE = 1609.34

# Trip-to-trip variation around the hourly traffic factor (lognormal sigma).
# Same OD at the same hour is not always the same speed: lights, incidents, driver.
DEFAULT_TRAFFIC_NOISE = 0.15


class Calibration:

    def __init__(self, od: Optional[pd.DataFrame] = None, speeds: Optional[pd.DataFrame] = None):
        od = od if od is not None else pd.read_csv(OD_MATRIX_CSV)
        speeds = speeds if speeds is not None else pd.read_csv(SPEED_BY_HOUR_CSV)

        # Cumulative weights make each draw O(log n) instead of O(n) over 58k pairs
        self._pairs: List[Tuple[int, int]] = list(zip(od["pickup_zone"].astype(int), od["dropoff_zone"].astype(int)))
        self._cum_weights = list(itertools.accumulate(od["share"].astype(float)))
        self._factors = {
            (row.day_type, int(row.hour)): float(row.traffic_factor)
            for row in speeds.itertuples()
        }

    def sample_od(self, rng: Random) -> Tuple[int, int]:
        r = rng.random() * self._cum_weights[-1]
        return self._pairs[bisect.bisect_right(self._cum_weights, r)]

    def traffic_factor(self, ts: datetime) -> float:
        day_type = "weekend" if ts.isoweekday() >= 6 else "weekday"
        return self._factors[(day_type, ts.hour)]


@dataclass
class Trip:
    """
    One completed trip: the plan the driver was given, and what actually happened.

    `route` is the plan (sent as the route_plan message). The track is the
    movement that produced the GPS: it equals the plan for a normal trip, and
    differs when there was a detour, a stop, a crash or an early end.
    """
    route: RoutePlan
    start_ts: datetime
    track_coords: List[Tuple[float, float]]   # actual (lat, lon) points
    track_times_s: List[float]                # actual seconds from start, one per point
    planned_duration_s: float                 # what navigation predicts: route at the hour's typical traffic
    behaviors: List[Behavior] = field(default_factory=list)
    spoof_windows: List[Tuple[float, float, float, float]] = field(default_factory=list)
    dropout_windows: List[Tuple[float, float]] = field(default_factory=list)

    @property
    def pickup_zone(self) -> int:
        return self.route.pickup_zone

    @property
    def dropoff_zone(self) -> int:
        return self.route.dropoff_zone

    @property
    def duration_s(self) -> float:
        return self.track_times_s[-1]

    @property
    def distance_m(self) -> float:
        return sum(haversine_m(a, b) for a, b in zip(self.track_coords, self.track_coords[1:]))

    @property
    def distance_miles(self) -> float:
        return self.distance_m / METERS_PER_MILE

    @property
    def end_ts(self) -> datetime:
        return self.start_ts + timedelta(seconds=self.duration_s)

    @property
    def incidents(self) -> List[Behavior]:
        return [b for b in self.behaviors if b.is_incident]


class TripPlanner:

    def __init__(self, router: Router, calibration: Calibration, traffic_noise: float = DEFAULT_TRAFFIC_NOISE,
                 injector: Optional[BehaviorInjector] = None):
        """
        injector: when set, trips get normal driving messiness and, at the
        configured rates, safety incidents. When None, the car follows the
        plan exactly at the hour's traffic speed.
        """
        self.router = router
        self.calibration = calibration
        self.traffic_noise = traffic_noise
        self.injector = injector

    def sample_od(self, rng: Random) -> Tuple[int, int]:
        return self.calibration.sample_od(rng)

    def build_trip(self, pickup_zone: int, dropoff_zone: int, start_ts: datetime, rng: Random,
                   incident: Optional[str] = "auto") -> Trip:
        route = self.router.plan(pickup_zone, dropoff_zone, rng)

        typical = self.calibration.traffic_factor(start_ts)
        actual = typical
        if self.traffic_noise:
            # Lognormal with median 1, so the hourly factor stays the typical value
            actual *= math.exp(rng.gauss(0.0, self.traffic_noise))

        if self.injector:
            shape = self.injector.build(route, actual, rng, incident=incident)
        else:
            shape = TripShape(Timeline(route.coords, [t * actual for t in route.point_times_s]))

        return Trip(
            route=route,
            start_ts=start_ts,
            track_coords=shape.timeline.coords,
            track_times_s=shape.timeline.times,
            planned_duration_s=route.duration_s * typical,
            behaviors=shape.timeline.behaviors,
            spoof_windows=shape.spoof_windows,
            dropout_windows=shape.dropout_windows,
        )
