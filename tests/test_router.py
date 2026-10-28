"""
Tests for geodata/router.py
Uses the tiny hand-built road graph from conftest.py, so no download or network is needed.
"""

from random import Random

import polyline
import pytest

from tests.conftest import A, B, C


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


def test_point_times_follow_edge_travel_times(router):
    plan = router.plan(10, 20, Random(1))
    times = plan.point_times_s
    assert len(times) == len(plan.coords)
    assert times[0] == 0.0 and times[1] == pytest.approx(30.0)    # end of the fast A->B road
    assert times[-1] == pytest.approx(plan.duration_s)
    assert all(b >= a for a, b in zip(times, times[1:]))


def test_same_seed_same_route(router):
    assert router.plan(10, 20, Random(7)).coords == router.plan(10, 20, Random(7)).coords


def test_zones_lists_routable_zones(router):
    assert router.zones == [10, 20, 30]


def test_same_single_node_zone_raises(router):
    with pytest.raises(ValueError, match="No route"):
        router.plan(10, 10, Random(1))
