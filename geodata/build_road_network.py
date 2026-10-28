"""
Download and prepare the NYC drivable road network from OpenStreetMap.

Steps:
  1. Download every drivable road inside New York City's administrative
     boundary (OSMnx / Overpass). The OSM city boundary includes the rivers,
     so bridges and tunnels survive; the taxi-zone union is land only and
     would cut Manhattan off from the other boroughs.
  2. Keep the largest strongly connected component, so a route always
     exists between any two nodes (no dead-end islands, one-way traps)
  3. Add edge speeds (from OSM speed limits, imputed where missing) and
     travel times, used as the routing weight
  4. Assign every node to the taxi zone it falls in, so the simulator can
     pick a real street point inside a pickup or dropoff zone

Outputs (data/reference/):
  nyc_drive.graphml   the road graph
  zone_nodes.csv      node_id, LocationID

Map data © OpenStreetMap contributors (ODbL).

Run:  python -m geodata.build_road_network   (after build_zones)
"""

import logging
import time

import geopandas as gpd
import osmnx as ox

from geodata.config import (
    EXCLUDED_ZONE_IDS, ROAD_GRAPH_FILE, WGS84, ZONE_NODES_CSV, ZONES_GEOJSON,
)

logger = logging.getLogger(__name__)

NYC_PLACE = "New York City, New York, United States"


def build_road_network():
    started = time.time()
    logger.info("Downloading NYC drive network from OpenStreetMap (several minutes)...")
    graph = ox.graph_from_place(NYC_PLACE, network_type="drive", simplify=True)
    logger.info("Downloaded in %.0fs: %d nodes, %d edges",
                time.time() - started, graph.number_of_nodes(), graph.number_of_edges())

    graph = ox.truncate.largest_component(graph, strongly=True)
    graph = ox.add_edge_speeds(graph)
    graph = ox.add_edge_travel_times(graph)
    logger.info("Largest strongly connected component: %d nodes, %d edges",
                graph.number_of_nodes(), graph.number_of_edges())

    ox.save_graphml(graph, ROAD_GRAPH_FILE)

    # Node -> zone lookup via a spatial join
    nodes = ox.graph_to_gdfs(graph, edges=False)[["geometry"]].reset_index()
    zones = gpd.read_file(ZONES_GEOJSON).to_crs(WGS84)
    zones = zones[~zones["LocationID"].isin(EXCLUDED_ZONE_IDS)]

    zone_nodes = gpd.sjoin(nodes, zones[["LocationID", "geometry"]], predicate="within", how="inner")
    zone_nodes = zone_nodes[["osmid", "LocationID"]].rename(columns={"osmid": "node_id"})
    zone_nodes.to_csv(ZONE_NODES_CSV, index=False)

    empty = sorted(set(zones["LocationID"]) - set(zone_nodes["LocationID"]))
    logger.info("Nodes mapped to zones: %d | zones without road nodes: %s",
                len(zone_nodes), empty or "none")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    build_road_network()
