"""
Make trips realistically messy, and inject the 5 safety incidents.

Normal behaviour (every trip, never labelled as an incident):
  traffic_stop        lights and jams, 8-120 s; total trip time is kept the
                      same, because the calibrated traffic already includes stops
  gps_dropout         no fix for 20-60 s (tunnels, tall buildings)
  innocent_long_stop  2-4 min stop that is NOT an incident    (hard negative)
  innocent_detour     200-600 m away from the plan, harmless   (hard negative)

Safety incidents (at most one per trip, ~2% of trips by default):
  route_deviation  1-3 km off the planned route, then on to the dropoff
  long_stop        4-15 min stop mid-trip
  wrong_dropoff    trip ends early, at least 800 m short of the dropoff
  possible_crash   from > 25 km/h to 0, then stationary until the trip ends
  gps_spoofing     reported position jumps 2-10 km away for 30-120 s

The hard negatives sit just below the incident thresholds on purpose, so a
model cannot score well by learning "any stop" or "any detour".
"""

import math
from dataclasses import dataclass, field
from random import Random
from typing import Dict, List, Optional, Tuple

import networkx as nx

from geodata.router import RoutePlan, Router, haversine_m
from simulation.timeline import Behavior, Timeline

INCIDENT_TYPES = ("route_deviation", "long_stop", "wrong_dropoff", "possible_crash", "gps_spoofing")
METERS_PER_DEG_LAT = 111_320.0


@dataclass
class BehaviorConfig:
    incident_rates: Dict[str, float] = field(default_factory=lambda: {
        "route_deviation": 0.006,
        "long_stop": 0.005,
        "wrong_dropoff": 0.003,
        "possible_crash": 0.002,
        "gps_spoofing": 0.004,
    })
    traffic_stops_per_km: float = 0.6
    traffic_stop_median_s: float = 25.0
    max_stop_share: float = 0.35            # stops never take more than this share of a trip
    dropout_prob: float = 0.08
    innocent_long_stop_prob: float = 0.03
    innocent_detour_prob: float = 0.03

    @property
    def total_incident_rate(self) -> float:
        return sum(self.incident_rates.values())


@dataclass
class TripShape:
    """What actually happened on a trip, before it becomes GPS messages."""
    timeline: Timeline
    spoof_windows: List[Tuple[float, float, float, float]] = field(default_factory=list)  # start, end, dlat, dlon
    dropout_windows: List[Tuple[float, float]] = field(default_factory=list)


def _poisson(lam: float, rng: Random) -> int:
    threshold, k, p = math.exp(-lam), 0, 1.0
    while True:
        p *= rng.random()
        if p <= threshold:
            return k
        k += 1


def _severity(value: float, low_below: float, medium_below: float) -> str:
    return "low" if value < low_below else "medium" if value < medium_below else "high"


class BehaviorInjector:

    def __init__(self, router: Router, config: Optional[BehaviorConfig] = None):
        self.router = router
        self.config = config or BehaviorConfig()

    def choose_incident(self, rng: Random) -> Optional[str]:
        r, cumulative = rng.random(), 0.0
        for kind in INCIDENT_TYPES:
            cumulative += self.config.incident_rates.get(kind, 0.0)
            if r < cumulative:
                return kind
        return None

    def build(self, route: RoutePlan, traffic_factor: float, rng: Random,
              incident: Optional[str] = "auto") -> TripShape:
        """
        Actual movement for a planned route. `incident` is chosen by rate when
        "auto"; tests and backfill runs can force a type or None.
        """
        cfg = self.config
        if incident == "auto":
            incident = self.choose_incident(rng)

        # 1. Which roads were driven: the plan, or a detour
        timeline = None
        if incident == "route_deviation":
            timeline = self._detour(route, traffic_factor, rng, 1000, 3000, "route_deviation", True)
            if timeline is None:
                incident = None                         # no suitable detour (e.g. very short trip)
        elif rng.random() < cfg.innocent_detour_prob:
            timeline = self._detour(route, traffic_factor, rng, 200, 600, "innocent_detour", False)
        if timeline is None:
            timeline = Timeline(route.coords, [t * traffic_factor for t in route.point_times_s])

        # 2. Normal stops, then the stop-type incidents
        self._traffic_stops(timeline, rng)
        if rng.random() < cfg.innocent_long_stop_prob:
            self._pause(timeline, rng, rng.uniform(120, 240), "innocent_long_stop", False)
        if incident == "long_stop":
            seconds = rng.uniform(240, 900)
            self._pause(timeline, rng, seconds, "long_stop", True, _severity(seconds, 420, 600))
        elif incident == "possible_crash":
            self._crash(timeline, rng)
        elif incident == "wrong_dropoff":
            self._wrong_dropoff(timeline, rng)

        # 3. Things that change what the phone reports, not where the car is
        shape = TripShape(timeline)
        if incident == "gps_spoofing":
            self._spoofing(shape, rng)
        if rng.random() < cfg.dropout_prob:
            start = rng.uniform(0.1, 0.9) * timeline.duration_s
            end = start + rng.uniform(20, 60)
            shape.dropout_windows.append((start, end))
            timeline.behaviors.append(Behavior("gps_dropout", False, start, end))
        return shape

    # ---------------- normal behaviour ----------------

    def _traffic_stops(self, timeline: Timeline, rng: Random) -> None:
        cfg = self.config
        n = _poisson(timeline.distance_m / 1000 * cfg.traffic_stops_per_km, rng)
        if n == 0:
            return
        stops = [min(max(cfg.traffic_stop_median_s * math.exp(rng.gauss(0, 0.6)), 8.0), 120.0) for _ in range(n)]
        total = timeline.duration_s
        if sum(stops) > cfg.max_stop_share * total:
            stops = [s * cfg.max_stop_share * total / sum(stops) for s in stops]

        # Compress moving time so stops fit inside the calibrated duration
        timeline.scale((total - sum(stops)) / total)
        moving = timeline.duration_s
        # Latest first, so earlier insertion points are not shifted
        for at, seconds in sorted(((rng.uniform(0.05, 0.95) * moving, s) for s in stops), reverse=True):
            timeline.insert_pause(at, seconds)
            timeline.behaviors.append(Behavior("traffic_stop", False, at, at + seconds))

    def _pause(self, timeline: Timeline, rng: Random, seconds: float, kind: str,
               is_incident: bool, severity: str = "none") -> None:
        at = rng.uniform(0.2, 0.8) * timeline.duration_s
        timeline.insert_pause(at, seconds)
        timeline.behaviors.append(Behavior(kind, is_incident, at, at + seconds, severity, {"stop_s": round(seconds)}))

    def _detour(self, route: RoutePlan, factor: float, rng: Random, min_m: float, max_m: float,
                kind: str, is_incident: bool) -> Optional[Timeline]:
        n = len(route.nodes)
        if n < 6:
            return None
        k = rng.randint(int(n * 0.2), max(int(n * 0.2), int(n * 0.6)))
        branch = route.nodes[k]
        target = self.router.random_node_near(branch, min_m, max_m, rng,
                                              avoid=route.coords[::5], avoid_m=min_m * 0.5)
        if target is None:
            return None
        try:
            out = self.router.route_nodes(branch, target)
            back = self.router.route_nodes(target, route.nodes[-1])
        except nx.NetworkXNoPath:
            return None

        actual = self.router.path_plan(route.nodes[:k] + out + back[1:])
        timeline = Timeline(actual.coords, [t * factor for t in actual.point_times_s])
        detour_m = haversine_m(self.router.node_latlon(branch), self.router.node_latlon(target))
        severity = _severity(detour_m, 1500, 2500) if is_incident else "none"
        timeline.behaviors.append(Behavior(
            kind, is_incident,
            start_s=timeline.times[actual.node_point_index[k]],
            end_s=timeline.times[actual.node_point_index[k + len(out) - 1]],
            severity=severity,
            params={"detour_m": round(detour_m)},
        ))
        return timeline

    # ---------------- incidents ----------------

    def _crash(self, timeline: Timeline, rng: Random) -> None:
        """Sudden stop from speed; the car never moves again."""
        total = timeline.duration_s
        window = [i for i in range(len(timeline.times) - 1)
                  if 0.2 * total <= timeline.times[i] <= 0.8 * total and timeline.times[i + 1] > timeline.times[i]]
        if not window:
            window = list(range(len(timeline.times) - 1))
        fast = [i for i in window if timeline.segment_speed_kmh(i) > 25]
        i = rng.choice(fast) if fast else max(window, key=timeline.segment_speed_kmh)
        speed = timeline.segment_speed_kmh(i)

        at = rng.uniform(timeline.times[i], timeline.times[i + 1])
        timeline.truncate(at)
        timeline.extend_stationary(rng.uniform(480, 1200))
        timeline.behaviors.append(Behavior("possible_crash", True, at, None, "high",
                                           {"speed_before_kmh": round(speed, 1)}))

    def _wrong_dropoff(self, timeline: Timeline, rng: Random) -> None:
        """Trip ends early, well short of the planned dropoff."""
        dropoff = timeline.coords[-1]
        for _ in range(10):
            at = rng.uniform(0.4, 0.85) * timeline.duration_s
            short_m = haversine_m(timeline.position_at(at), dropoff)
            if short_m >= 800:
                timeline.truncate(at)
                timeline.behaviors.append(Behavior("wrong_dropoff", True, at, at,
                                                   _severity(short_m, 1500, 3000),
                                                   {"short_of_dropoff_m": round(short_m)}))
                return

    def _spoofing(self, shape: TripShape, rng: Random) -> None:
        """Reported position jumps far away for a short window, then returns."""
        timeline = shape.timeline
        start = rng.uniform(0.3, 0.7) * timeline.duration_s
        end = min(start + rng.uniform(30, 120), timeline.duration_s)
        jump_m, bearing = rng.uniform(2000, 10000), rng.uniform(0, 2 * math.pi)
        lat = timeline.position_at(start)[0]
        dlat = jump_m * math.cos(bearing) / METERS_PER_DEG_LAT
        dlon = jump_m * math.sin(bearing) / (METERS_PER_DEG_LAT * math.cos(math.radians(lat)))
        shape.spoof_windows.append((start, end, dlat, dlon))
        timeline.behaviors.append(Behavior("gps_spoofing", True, start, end, "medium", {"jump_m": round(jump_m)}))
