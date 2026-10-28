"""
Plan realistic trips: where people travel, which road they take, how long it takes.

  Calibration   real NYC origin-destination shares and traffic factors by hour
  TripPlanner   samples a pickup/dropoff pair, routes it on real roads and
                stretches free-flow time by the traffic of that hour

A Trip carries a timestamp for every point of the route, which the GPS
generator interpolates to produce pings.
"""

import bisect
import itertools
import math
from dataclasses import dataclass
from datetime import datetime, timedelta
from random import Random
from typing import List, Optional, Tuple

import pandas as pd

from geodata.router import RoutePlan, Router
from simulation.config import OD_MATRIX_CSV, SPEED_BY_HOUR_CSV

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
    route: RoutePlan
    start_ts: datetime
    point_times_s: List[float]     # real seconds from start, one per route point

    @property
    def pickup_zone(self) -> int:
        return self.route.pickup_zone

    @property
    def dropoff_zone(self) -> int:
        return self.route.dropoff_zone

    @property
    def duration_s(self) -> float:
        return self.point_times_s[-1]

    @property
    def distance_m(self) -> float:
        return self.route.distance_m

    @property
    def distance_miles(self) -> float:
        return self.distance_m / METERS_PER_MILE

    @property
    def end_ts(self) -> datetime:
        return self.start_ts + timedelta(seconds=self.duration_s)


class TripPlanner:

    def __init__(self, router: Router, calibration: Calibration, traffic_noise: float = DEFAULT_TRAFFIC_NOISE):
        self.router = router
        self.calibration = calibration
        self.traffic_noise = traffic_noise

    def sample_od(self, rng: Random) -> Tuple[int, int]:
        return self.calibration.sample_od(rng)

    def build_trip(self, pickup_zone: int, dropoff_zone: int, start_ts: datetime, rng: Random) -> Trip:
        route = self.router.plan(pickup_zone, dropoff_zone, rng)

        factor = self.calibration.traffic_factor(start_ts)
        if self.traffic_noise:
            # Lognormal with median 1, so the hourly factor stays the typical value
            factor *= math.exp(rng.gauss(0.0, self.traffic_noise))

        return Trip(route, start_ts, [t * factor for t in route.point_times_s])
