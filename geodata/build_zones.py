"""
Build NYC taxi zone reference data from the official TLC shapefile.

Outputs (data/reference/):
  taxi_zones.geojson   one polygon per zone, WGS84
  zone_centroids.csv   LocationID, borough, zone, lat, lon
  nyc_boundary.geojson union of all NYC zones, used to download roads

The shapefile has a few zones split into several polygons (e.g. islands),
so geometries are dissolved to exactly one row per LocationID.

Run:  python -m geodata.build_zones
"""

import io
import logging
import zipfile

import geopandas as gpd
import requests

from geodata.config import (
    EXCLUDED_ZONE_IDS, NY_STATE_PLANE, NYC_BOUNDARY_GEOJSON, RAW_DIR,
    REFERENCE_DIR, TAXI_ZONES_URL, WGS84, ZONE_CENTROIDS_CSV, ZONES_GEOJSON,
)

logger = logging.getLogger(__name__)


def download_shapefile() -> gpd.GeoDataFrame:
    """Download and unzip the TLC shapefile, return it in its native CRS."""
    target = RAW_DIR / "taxi_zones"
    if not any(target.glob("**/*.shp")):
        logger.info("Downloading %s", TAXI_ZONES_URL)
        response = requests.get(TAXI_ZONES_URL, timeout=120)
        response.raise_for_status()
        target.mkdir(parents=True, exist_ok=True)
        zipfile.ZipFile(io.BytesIO(response.content)).extractall(target)

    shp = next(target.glob("**/*.shp"))
    zones = gpd.read_file(shp)
    return zones.set_crs(NY_STATE_PLANE) if zones.crs is None else zones


def build_zones() -> gpd.GeoDataFrame:
    zones = download_shapefile()

    # One row per zone: merge split polygons, keep the descriptive columns
    zones = (
        zones[["LocationID", "borough", "zone", "geometry"]]
        .dissolve(by="LocationID", aggfunc="first")
        .reset_index()
        .to_crs(NY_STATE_PLANE)
    )
    zones["LocationID"] = zones["LocationID"].astype(int)

    # Centroids are computed in feet (projected), then converted to lat/lon.
    # representative_point() is used instead of centroid so the point is
    # always inside the zone, even for odd shapes.
    points = zones.geometry.representative_point().to_crs(WGS84)
    zones = zones.to_crs(WGS84)

    REFERENCE_DIR.mkdir(parents=True, exist_ok=True)
    zones.to_file(ZONES_GEOJSON, driver="GeoJSON")

    centroids = zones[["LocationID", "borough", "zone"]].copy()
    centroids["lat"] = points.y.round(6).values
    centroids["lon"] = points.x.round(6).values
    centroids.to_csv(ZONE_CENTROIDS_CSV, index=False)

    nyc = zones[~zones["LocationID"].isin(EXCLUDED_ZONE_IDS)]
    boundary = gpd.GeoDataFrame(geometry=[nyc.union_all()], crs=WGS84)
    boundary.to_file(NYC_BOUNDARY_GEOJSON, driver="GeoJSON")

    logger.info("Zones: %d | boroughs: %s", len(zones), sorted(zones["borough"].unique()))
    return zones


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    build_zones()
