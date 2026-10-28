"""
Paths and constants for geographic reference data.

All generated files live in data/reference/ (git-ignored). They are
rebuilt from public sources with build_zones.py and build_road_network.py.
"""

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
REFERENCE_DIR = REPO_ROOT / "data" / "reference"
RAW_DIR = REFERENCE_DIR / "raw"

# Official NYC TLC taxi zone shapefile
TAXI_ZONES_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zones.zip"

# Outputs of build_zones.py
ZONES_GEOJSON = REFERENCE_DIR / "taxi_zones.geojson"
ZONE_CENTROIDS_CSV = REFERENCE_DIR / "zone_centroids.csv"
NYC_BOUNDARY_GEOJSON = REFERENCE_DIR / "nyc_boundary.geojson"

# Outputs of build_road_network.py
ROAD_GRAPH_FILE = REFERENCE_DIR / "nyc_drive.graphml"
ZONE_NODES_CSV = REFERENCE_DIR / "zone_nodes.csv"

# CRS: the TLC shapefile uses NY State Plane (feet); everything we publish is WGS84
NY_STATE_PLANE = "EPSG:2263"
WGS84 = "EPSG:4326"

# Zone 1 is Newark Airport (New Jersey). It is outside the NYC road
# network we download, so rides never start or end there.
EXCLUDED_ZONE_IDS = {1}
