"""
Tests for geodata/router.py
Uses a tiny hand-built road graph, so no download or network is needed.
"""

from random import Random

import networkx as nx
import pandas as pd
import polyline
import pytest
from shapely.geometry import LineString

from geodata.router import Router

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


def test_route_uses_fastest_path(router):
    plan = router.plan(10, 20, Random(1))
    assert plan.nodes == [A, B, C]
    assert plan.duration_s == pytest.approx(50.0)     # 30 + 20, not the 200 s direct road


def test_parallel_edges_pick_the_fastest(router):
    plan = router.plan(10, 20, Random(1))
    assert plan.edge_lengths_m == [500.0, 300.0]       # 500 m road had travel_time 30
    assert plan.distance_m == pytest.approx(800.0)


def test_curved_road_keeps_its_shape_without_duplicate_points(router):
    plan = router.plan(10, 20, Random(1))
    assert plan.coords == [(40.75, -73.99), (40.75, -73.98), (40.758, -73.975), (40.76, -73.97)]


def test_polyline_round_trip(router):
    plan = router.plan(10, 20, Random(1))
    assert polyline.decode(plan.encoded_polyline, precision=5) == plan.coords


def test_same_seed_same_route(router):
    assert router.plan(10, 20, Random(7)).coords == router.plan(10, 20, Random(7)).coords


def test_zones_lists_routable_zones(router):
    assert router.zones == [10, 20, 30]


def test_same_single_node_zone_raises(router):
    with pytest.raises(ValueError, match="No route"):
        router.plan(10, 10, Random(1))
