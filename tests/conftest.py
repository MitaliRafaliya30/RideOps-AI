"""
Shared pytest fixtures.
"""

import networkx as nx
import pandas as pd
import pytest
from shapely.geometry import LineString

from geodata.router import Router
from simulation.trip_planner import Calibration

# Three intersections in a line (lon = x, lat = y)
A, B, C = 1, 2, 3
POS = {A: (-73.990, 40.750), B: (-73.980, 40.750), C: (-73.970, 40.760)}


@pytest.fixture
def router():
    """
    A -> B has two parallel roads: slow+short and fast+long.
    B -> C is curved (carries a geometry with a midpoint).
    A -> C is a direct road that is much slower.
    """
    g = nx.MultiDiGraph()
    for node, (x, y) in POS.items():
        g.add_node(node, x=x, y=y)
    g.add_edge(A, B, length=400.0, travel_time=50.0)
    g.add_edge(A, B, length=500.0, travel_time=30.0)
    g.add_edge(B, C, length=300.0, travel_time=20.0,
               geometry=LineString([POS[B], (-73.975, 40.758), POS[C]]))
    g.add_edge(A, C, length=600.0, travel_time=200.0)

    zone_nodes = pd.DataFrame({"node_id": [A, B, C], "LocationID": [10, 30, 20]})
    return Router(graph=g, zone_nodes=zone_nodes)


@pytest.fixture
def calibration():
    od = pd.DataFrame({"pickup_zone": [10, 30], "dropoff_zone": [20, 20], "share": [0.8, 0.2]})
    speeds = pd.DataFrame([
        {"day_type": day, "hour": h, "traffic_factor": 2.8 if h == 16 else 1.3}
        for day in ("weekday", "weekend") for h in range(24)
    ])
    speeds.loc[(speeds.day_type == "weekend") & (speeds.hour == 16), "traffic_factor"] = 2.0
    return Calibration(od=od, speeds=speeds)
