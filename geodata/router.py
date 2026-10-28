"""
Route rides along real NYC streets.

Given a pickup zone and a dropoff zone, the Router picks a real street
point inside each zone and returns the fastest path between them, in the
shape of the route_plan contract (docs/DATA_DESIGN.md, section 2.2).

The graph is loaded once and reused; routing uses travel_time as weight.
"""

from dataclasses import dataclass, field
from random import Random
from typing import List, Optional, Tuple

import networkx as nx
import osmnx as ox
import pandas as pd
import polyline

from geodata.config import ROAD_GRAPH_FILE, ZONE_NODES_CSV


@dataclass
class RoutePlan:
    pickup_zone: int
    dropoff_zone: int
    nodes: List[int]
    coords: List[Tuple[float, float]]        # (lat, lon) along the path
    edge_lengths_m: List[float] = field(repr=False)
    edge_times_s: List[float] = field(repr=False)

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
        coords, lengths, times = [], [], []
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
            coords.extend(segment if not coords else segment[1:])

        coords = [(round(lat, 6), round(lon, 6)) for lat, lon in coords]
        return RoutePlan(pickup_zone, dropoff_zone, path, coords, lengths, times)
