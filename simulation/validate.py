"""
Validate simulated trips against the real NYC trips.

Plans N trips on the real road network, with request times drawn from the
real demand profile and the fitted fare model applied, and compares median
distance, duration and base fare with the real Uber trips (HV0003).

Run:  python -m simulation.validate [n_trips]
"""

import json
import statistics
import sys
from random import Random

import pandas as pd

from database.utils.fare_calculator import FareCalculator
from geodata.router import Router
from simulation.calibrate import apply_fare_model, sample_request_times
from simulation.config import CALIBRATION_SUMMARY_JSON, DEMAND_BY_HOUR_CSV
from simulation.trip_planner import Calibration, TripPlanner


def main(n_trips: int = 500, seed: int = 7):
    rng = Random(seed)
    model = apply_fare_model()
    planner = TripPlanner(Router(), Calibration())
    demand = pd.read_csv(DEMAND_BY_HOUR_CSV)

    miles, minutes, fares = [], [], []
    for start in sample_request_times(demand, n_trips, rng):
        trip = planner.build_trip(*planner.sample_od(rng), start, rng)
        fare = FareCalculator.calculate_fare(trip.distance_miles, int(trip.duration_s), rng=rng)
        miles.append(trip.distance_miles)
        minutes.append(trip.duration_s / 60)
        fares.append(float(fare["base_passenger_fare"]))

    real = json.loads(CALIBRATION_SUMMARY_JSON.read_text())["real"]
    rows = [
        ("Median distance (miles)", real["median_miles"], statistics.median(miles)),
        ("Median duration (min)", real["median_minutes"], statistics.median(minutes)),
        ("Median base fare ($)", real["median_base_fare"], statistics.median(fares)),
    ]
    print(f"Fare model: ${model['base_fare']} + ${model['per_mile']}/mile + ${model['per_minute']}/min "
          f"(R2 {model['r2']} on {model['rows']:,} real trips)")
    print(f"{n_trips} simulated trips vs {real['trips']:,} real trips\n")
    print(f"{'Metric':<26}{'Real':>10}{'Simulated':>12}{'Diff':>8}")
    for name, real_value, sim_value in rows:
        print(f"{name:<26}{real_value:>10.2f}{sim_value:>12.2f}{(sim_value / real_value - 1):>8.0%}")


if __name__ == "__main__":
    main(int(sys.argv[1]) if len(sys.argv) > 1 else 500)
