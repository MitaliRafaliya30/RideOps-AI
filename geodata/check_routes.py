"""
Sanity check for the road network and router.

  1. Routes three known trips, including bridge crossings, to prove the
     boroughs are connected
  2. Times 200 random zone-to-zone routes, to size the backfill run
  3. Compares free-flow route speed with the real NYC average speed
     (5.01 miles in 20.6 minutes, from the FHVHV profiling report), which
     tells the simulator how much traffic slowdown to apply

Run:  python -m geodata.check_routes
"""

import statistics
import time
from random import Random

from geodata.router import Router

KNOWN_TRIPS = [
    (161, 132, "Midtown Center (Manhattan) -> JFK Airport (Queens)"),
    (61, 230, "Crown Heights North (Brooklyn) -> Times Sq (Manhattan)"),
    (221, 61, "Stapleton (Staten Island) -> Crown Heights North (Brooklyn)"),
]
REAL_NYC_MPH = 5.01 / (20.6 / 60)


def mph(plan) -> float:
    return (plan.distance_m / 1609.34) / (plan.duration_s / 3600)


def main():
    started = time.time()
    router = Router()
    print(f"Graph loaded in {time.time() - started:.1f}s: "
          f"{router.graph.number_of_nodes():,} nodes, {len(router.zones)} routable zones\n")

    rng = Random(42)
    for pickup, dropoff, label in KNOWN_TRIPS:
        plan = router.plan(pickup, dropoff, rng)
        print(f"{label}\n  {plan.distance_m / 1000:.1f} km | {plan.duration_s / 60:.1f} min free-flow | "
              f"{len(plan.coords)} points | polyline {len(plan.encoded_polyline)} chars")

    zones = router.zones
    samples, timings = [], []
    for _ in range(200):
        t0 = time.perf_counter()
        plan = router.plan(rng.choice(zones), rng.choice(zones), rng)
        timings.append(time.perf_counter() - t0)
        samples.append(plan)

    free_flow = statistics.median(mph(p) for p in samples)
    print(f"\n200 random routes")
    print(f"  routing time: median {statistics.median(timings) * 1000:.0f} ms, "
          f"max {max(timings) * 1000:.0f} ms")
    print(f"  distance: median {statistics.median(p.distance_m for p in samples) / 1000:.1f} km")
    print(f"  free-flow speed: median {free_flow:.1f} mph vs real NYC {REAL_NYC_MPH:.1f} mph "
          f"-> traffic factor ~{free_flow / REAL_NYC_MPH:.2f}x slower")


if __name__ == "__main__":
    main()
