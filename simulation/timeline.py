"""
A trip's actual movement over time, and the operations that change it.

A Timeline is a list of (lat, lon) points with the second each is reached.
Behaviours edit it: pause the car, cut the trip short, stretch time. Every
edit also moves the recorded Behavior times, so ground-truth labels always
point at the right moment of the final track.
"""

import bisect
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

from geodata.router import haversine_m

LatLon = Tuple[float, float]


@dataclass
class Behavior:
    """Something that happened during a trip, recorded as ground truth."""
    kind: str                      # e.g. "long_stop", "traffic_stop", "gps_spoofing"
    is_incident: bool              # True only for the 5 safety incident types
    start_s: float                 # seconds from trip start
    end_s: Optional[float]         # None when it lasts until the trip ends
    severity: str = "none"         # low / medium / high for incidents
    params: Dict = field(default_factory=dict)


def segment_at(coords: List[LatLon], times: List[float], t: float) -> Tuple[int, float]:
    """Index i of the segment (i -> i+1) containing time t, and the fraction along it."""
    t = min(max(t, times[0]), times[-1])
    i = min(bisect.bisect_right(times, t) - 1, len(coords) - 2)
    span = times[i + 1] - times[i]
    return i, ((t - times[i]) / span if span > 0 else 1.0)


def position_at(coords: List[LatLon], times: List[float], t: float) -> LatLon:
    i, f = segment_at(coords, times, t)
    a, b = coords[i], coords[i + 1]
    return a[0] + (b[0] - a[0]) * f, a[1] + (b[1] - a[1]) * f


class Timeline:

    def __init__(self, coords: List[LatLon], times: List[float]):
        self.coords = list(coords)
        self.times = list(times)
        self.behaviors: List[Behavior] = []

    @property
    def duration_s(self) -> float:
        return self.times[-1]

    @property
    def distance_m(self) -> float:
        return sum(haversine_m(a, b) for a, b in zip(self.coords, self.coords[1:]))

    def position_at(self, t: float) -> LatLon:
        return position_at(self.coords, self.times, t)

    def segment_speed_kmh(self, i: int) -> float:
        span = self.times[i + 1] - self.times[i]
        return haversine_m(self.coords[i], self.coords[i + 1]) / span * 3.6 if span > 0 else 0.0

    # ---------------- edits ----------------

    def scale(self, factor: float) -> None:
        """Stretch (or compress) all times, e.g. traffic or making room for stops."""
        self.times = [t * factor for t in self.times]
        for b in self.behaviors:
            b.start_s *= factor
            if b.end_s is not None:
                b.end_s *= factor

    def insert_pause(self, t: float, seconds: float) -> None:
        """The car stands still at time t for `seconds`; everything later shifts."""
        t = min(max(t, 0.0), self.duration_s)
        pos = self.position_at(t)
        i = bisect.bisect_right(self.times, t)
        self.coords = self.coords[:i] + [pos, pos] + self.coords[i:]
        self.times = self.times[:i] + [t, t + seconds] + [x + seconds for x in self.times[i:]]
        for b in self.behaviors:
            if b.start_s > t:
                b.start_s += seconds
            if b.end_s is not None and b.end_s > t:
                b.end_s += seconds

    def truncate(self, t: float) -> None:
        """The trip ends at time t, wherever the car is. Later behaviours never happened."""
        pos = self.position_at(t)
        i = bisect.bisect_left(self.times, t)
        self.coords = self.coords[:i] + [pos]
        self.times = self.times[:i] + [t]
        self.behaviors = [b for b in self.behaviors if b.start_s < t]
        for b in self.behaviors:
            if b.end_s is not None and b.end_s > t:
                b.end_s = t

    def extend_stationary(self, seconds: float) -> None:
        """The car stays where it is for `seconds` more before the trip ends."""
        self.coords.append(self.coords[-1])
        self.times.append(self.times[-1] + seconds)
