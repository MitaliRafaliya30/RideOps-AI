"""
Draw one example of each safety incident on the real NYC road network.

Blue = planned route (route_plan message), dots = GPS pings, red dots =
pings during the incident. Saved to docs/images/safety_incidents.png.

Run:  python -m simulation.plot_incidents
"""

from datetime import datetime, timedelta
from random import Random

import geopandas as gpd
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

from geodata.config import REPO_ROOT, ZONES_GEOJSON
from geodata.router import Router
from simulation.behaviors import INCIDENT_TYPES, BehaviorInjector
from simulation.gps import GpsGenerator
from simulation.trip_planner import Calibration, TripPlanner

OUTPUT = REPO_ROOT / "docs" / "images" / "safety_incidents.png"
TITLES = {
    "route_deviation": "Route deviation",
    "long_stop": "Unexpected long stop",
    "wrong_dropoff": "Wrong / early drop-off",
    "possible_crash": "Possible crash",
    "gps_spoofing": "GPS spoofing",
}


def main(seed: int = 5):
    rng = Random(seed)
    router = Router()
    planner = TripPlanner(router, Calibration(), injector=BehaviorInjector(router))
    gps = GpsGenerator(rng)
    zones = gpd.read_file(ZONES_GEOJSON)
    start = datetime(2026, 5, 6, 14, 0)

    fig, axes = plt.subplots(1, len(INCIDENT_TYPES), figsize=(20, 5.2))
    for ax, kind in zip(axes, INCIDENT_TYPES):
        while True:
            trip = planner.build_trip(*planner.sample_od(rng), start, rng, incident=kind)
            if trip.incidents and trip.distance_m > 4000:
                break
        incident = trip.incidents[0]
        pings = gps.pings(trip, "example", 1)
        seconds = [(datetime.fromisoformat(p["ping_ts"][:-1]) - trip.start_ts).total_seconds() for p in pings]
        end_s = incident.end_s if incident.end_s is not None else trip.duration_s
        during = [incident.start_s <= s <= max(end_s, incident.start_s + 1) for s in seconds]

        lat, lon = zip(*trip.route.coords)
        ax.plot(lon, lat, color="#1d4e89", linewidth=2.2, label="Planned route", zorder=2)
        ax.scatter([p["lon"] for p, d in zip(pings, during) if not d], [p["lat"] for p, d in zip(pings, during) if not d],
                   s=7, color="#f2a541", zorder=3, label="GPS pings")
        ax.scatter([p["lon"] for p, d in zip(pings, during) if d], [p["lat"] for p, d in zip(pings, during) if d],
                   s=14, color="#d7263d", zorder=4, label="During incident")

        all_lon = list(lon) + [p["lon"] for p in pings]
        all_lat = list(lat) + [p["lat"] for p in pings]
        pad = 0.01
        ax.set_xlim(min(all_lon) - pad, max(all_lon) + pad)
        ax.set_ylim(min(all_lat) - pad, max(all_lat) + pad)
        zones.plot(ax=ax, color="#eef2f1", edgecolor="#c9d3cf", linewidth=0.4, zorder=1)
        ax.set_title(f"{TITLES[kind]}\n{incident.params}", fontsize=10)
        ax.set_axis_off()
        ax.set_aspect("equal")

    axes[0].legend(loc="lower left", fontsize=8)
    fig.suptitle("RideOps SafeRide: injected safety incidents on real NYC roads", fontsize=13)
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(OUTPUT, dpi=110, bbox_inches="tight")
    print(f"Saved {OUTPUT}")


if __name__ == "__main__":
    main()
