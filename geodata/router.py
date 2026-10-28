"""
Route rides along real NYC streets.

Given a pickup zone and a dropoff zone, the Router picks a real street
point inside each zone and returns the fastest path between them, in the
shape of the route_plan contract (docs/DATA_DESIGN.md, section 2.2).

The graph is loaded once and reused; routing uses travel_time as weight.
"""

import math
from dataclasses import dataclass, field
from random import Random
from typing import List, Optional, Tuple

import networkx as nx
import osmnx as ox
import pandas as pd
import polyline

from geodata.config import ROAD_GRAPH_FILE, ZONE_NODES_CSV

EARTH_RADIUS_M = 6_371_000


def haversine_m(a: Tuple[float, float], b: Tuple[float, float]) -> float:
    """Great-circle distance in meters between two (lat, lon) points."""
    lat1, lon1, lat2, lon2 = map(math.radians, (a[0], a[1], b[0], b[1]))
    h = math.sin((lat2 - lat1) / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin((lon2 - lon1) / 2) ** 2
    return 2 * EARTH_RADIUS_M * math.asin(math.sqrt(h))


@dataclass
class RoutePlan:
    pickup_zone: int
    dropoff_zone: int
    nodes: List[int]
    coords: List[Tuple[float, float]]        # (lat, lon) along the path
    edge_lengths_m: List[float] = field(repr=False)
    edge_times_s: List[float] = field(repr=False)
    point_times_s: List[float] = field(repr=False)   # free-flow seconds from start, one per coord
    node_point_index: List[int] = field(repr=False)  # position in coords of each node in `nodes`

    @property
    def distance_m(self) -> float:
        return sum(self.edge_lengths_m)

    @property
    def duration_s(self) -> float:
        return sum(self.edge_times_s)

    @property
    def encoded_polyline(self) -> str:
        return polyline.encode(self.coords, precision=5)


class Router:

    def __init__(self, graph=None, zone_nodes: Optional[pd.DataFrame] = None):
        self.graph = graph if graph is not None else ox.load_graphml(ROAD_GRAPH_FILE)
        zone_nodes = zone_nodes if zone_nodes is not None else pd.read_csv(ZONE_NODES_CSV)
        self.nodes_by_zone = zone_nodes.groupby("LocationID")["node_id"].apply(list).to_dict()

    @property
    def zones(self) -> List[int]:
        return sorted(self.nodes_by_zone)

    def random_node(self, zone_id: int, rng: Random) -> int:
        """A real street intersection inside the zone."""
        return rng.choice(self.nodes_by_zone[zone_id])

    def route_nodes(self, origin: int, destination: int) -> List[int]:
        return nx.shortest_path(self.graph, origin, destination, weight="travel_time")

    def node_latlon(self, node: int) -> Tuple[float, float]:
        return self.graph.nodes[node]["y"], self.graph.nodes[node]["x"]

    def path_plan(self, path: List[int], pickup_zone: int = 0, dropoff_zone: int = 0) -> RoutePlan:
        """Timed coordinates for an explicit node path (used for detours)."""
        return self._to_plan(pickup_zone, dropoff_zone, path)

    def random_node_near(self, center: int, min_m: float, max_m: float, rng: Random,
                         avoid: List[Tuple[float, float]] = (), avoid_m: float = 0.0) -> Optional[int]:
        """
        A node whose straight-line distance from `center` is between min_m and
        max_m, and at least avoid_m from every point in `avoid` (e.g. the
        planned route). Searches only nodes reachable within a road-distance
        cutoff, so it stays fast on the full NYC graph.
        """
        reachable = nx.single_source_dijkstra_path_length(self.graph, center, cutoff=max_m * 2, weight="length")
        origin = self.node_latlon(center)
        candidates = []
        for node in reachable:
            point = self.node_latlon(node)
            if not min_m <= haversine_m(origin, point) <= max_m:
                continue
            if avoid_m and any(haversine_m(point, a) < avoid_m for a in avoid):
                continue
            candidates.append(node)
        return rng.choice(sorted(candidates)) if candidates else None

    def plan(self, pickup_zone: int, dropoff_zone: int, rng: Random,
             min_distance_m: float = 300.0, attempts: int = 5) -> RoutePlan:
        """
        Route between random street points in two zones.

        Retries when both points coincide or the trip is unrealistically
        short (same-zone trips can otherwise produce a 0 m route).
        """
        plan = None
        for _ in range(attempts):
            origin = self.random_node(pickup_zone, rng)
            destination = self.random_node(dropoff_zone, rng)
            if origin == destination:
                continue
            path = self.route_nodes(origin, destination)
            plan = self._to_plan(pickup_zone, dropoff_zone, path)
            if plan.distance_m >= min_distance_m:
                return plan
        if plan is None:
            raise ValueError(f"No route between zones {pickup_zone} and {dropoff_zone}: "
                             "every attempt picked the same street point")
        return plan

    def _to_plan(self, pickup_zone: int, dropoff_zone: int, path: List[int]) -> RoutePlan:
        coords, lengths, times, point_times = [], [], [], []
        node_index = [0]
        elapsed = 0.0
        for u, v in zip(path[:-1], path[1:]):
            # Parallel edges are possible; take the fastest one, same as routing did
            edge = min(self.graph[u][v].values(), key=lambda e: e["travel_time"])
            lengths.append(float(edge["length"]))
            times.append(float(edge["travel_time"]))

            # Curved roads carry their full shape; straight ones are node to node
            if "geometry" in edge:
                segment = [(lat, lon) for lon, lat in edge["geometry"].coords]
            else:
                segment = [
                    (self.graph.nodes[u]["y"], self.graph.nodes[u]["x"]),
                    (self.graph.nodes[v]["y"], self.graph.nodes[v]["x"]),
                ]

            # Spread the edge's travel time over its shape points by distance,
            # so the car moves at the edge's speed along curves too
            hops = [haversine_m(a, b) for a, b in zip(segment[:-1], segment[1:])]
            shape_length = sum(hops) or 1.0
            seg_times = [elapsed]
            for hop in hops:
                seg_times.append(seg_times[-1] + edge["travel_time"] * hop / shape_length)

            if coords:
                segment, seg_times = segment[1:], seg_times[1:]
            coords.extend(segment)
            point_times.extend(seg_times)
            node_index.append(len(coords) - 1)
            elapsed += float(edge["travel_time"])

        coords = [(round(lat, 6), round(lon, 6)) for lat, lon in coords]
        return RoutePlan(pickup_zone, dropoff_zone, path, coords, lengths, times, point_times, node_index)
