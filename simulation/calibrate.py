"""
Calibrate the simulator against real NYC trips (Uber, HV0003).

Produces three tables the simulator samples from, plus a summary:

  od_matrix.csv       pickup zone x dropoff zone -> share of real trips
                      (where people actually travel; uniform random zones
                      gave a 17 km median trip vs 4.6 km in reality)
  speed_by_hour.csv   day type x hour -> real median speed and a traffic
                      factor = free-flow route speed / real speed
                      (OSM speed limits are ~2.5x faster than NYC traffic)
  demand_by_hour.csv  day of week x hour -> share of trip requests
  summary.json        headline numbers, the fitted fare model, and the
                      targets the simulator is validated against

Two corrections make the simulator match reality, not just look like it:
  - traffic factors get one global rescale so the simulated median trip
    duration equals the real one (a median of ratios underestimates delay)
  - the base fare formula (base + per mile + per minute) is fitted with
    least squares on a sample of real trips, replacing hand-picked rates

Only zones that have road nodes are kept, so every sampled trip is routable.

Run:  python -m simulation.calibrate
"""

import json
import logging
import statistics
from datetime import datetime, timedelta
from random import Random
from typing import Iterator

import duckdb
import numpy as np
import pandas as pd

from geodata.config import ZONE_NODES_CSV
from geodata.router import Router
from simulation.config import (
    CALIBRATION_DIR, CALIBRATION_SUMMARY_JSON, DEMAND_BY_HOUR_CSV, FHVHV_GLOB,
    MAX_PLAUSIBLE_MPH, MIN_TRIP_MILES, MIN_TRIP_SECONDS, OD_MATRIX_CSV,
    SPEED_BY_HOUR_CSV, UBER_LICENSE,
)
from simulation.trip_planner import Calibration, TripPlanner

logger = logging.getLogger(__name__)

FREE_FLOW_SAMPLE_SIZE = 1000
FARE_SAMPLE_ROWS = 500_000
METERS_PER_MILE = 1609.34
WEEK_START = datetime(2026, 5, 4)      # a Monday; only weekday and hour matter


def _connect(routable_zones) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute(f"""
        CREATE VIEW trips AS
        SELECT *
        FROM read_parquet('{FHVHV_GLOB}')
        WHERE hvfhs_license_num = '{UBER_LICENSE}'
    """)
    con.execute("CREATE TABLE routable AS SELECT unnest(?) AS zone_id", [sorted(routable_zones)])
    return con


def od_matrix(con) -> pd.DataFrame:
    od = con.execute("""
        SELECT PULocationID AS pickup_zone, DOLocationID AS dropoff_zone, count(*) AS trips
        FROM trips
        WHERE PULocationID IN (SELECT zone_id FROM routable)
          AND DOLocationID IN (SELECT zone_id FROM routable)
        GROUP BY 1, 2
    """).df()
    od["share"] = od["trips"] / od["trips"].sum()
    return od.sort_values("trips", ascending=False).reset_index(drop=True)


def real_speed_by_hour(con) -> pd.DataFrame:
    return con.execute(f"""
        WITH speeds AS (
            SELECT
                CASE WHEN isodow(pickup_datetime) >= 6 THEN 'weekend' ELSE 'weekday' END AS day_type,
                hour(pickup_datetime) AS hour,
                trip_miles / (trip_time / 3600.0) AS mph
            FROM trips
            WHERE trip_time >= {MIN_TRIP_SECONDS} AND trip_miles >= {MIN_TRIP_MILES}
        )
        SELECT day_type, hour, median(mph) AS real_mph, count(*) AS trips
        FROM speeds
        WHERE mph BETWEEN 1 AND {MAX_PLAUSIBLE_MPH}
        GROUP BY 1, 2
        ORDER BY 1, 2
    """).df()


def demand_by_hour(con) -> pd.DataFrame:
    demand = con.execute("""
        SELECT isodow(request_datetime) AS day_of_week, hour(request_datetime) AS hour, count(*) AS requests
        FROM trips
        GROUP BY 1, 2
        ORDER BY 1, 2
    """).df()
    demand["share"] = demand["requests"] / demand["requests"].sum()
    return demand


def real_trip_summary(con) -> dict:
    row = con.execute("""
        SELECT count(*), median(trip_miles), median(trip_time) / 60.0, median(base_passenger_fare)
        FROM trips
        WHERE trip_miles > 0
    """).fetchone()
    return {
        "trips": int(row[0]),
        "median_miles": round(row[1], 2),
        "median_minutes": round(row[2], 1),
        "median_base_fare": round(row[3], 2),
    }


def fit_fare_model(con, seed: int) -> dict:
    """Least squares: base_passenger_fare = base + per_mile * miles + per_minute * minutes."""
    sample = con.execute(f"""
        SELECT trip_miles, trip_time / 60.0 AS minutes, base_passenger_fare AS fare
        FROM trips
        WHERE trip_time >= {MIN_TRIP_SECONDS} AND trip_miles >= {MIN_TRIP_MILES}
          AND base_passenger_fare > 0
        USING SAMPLE reservoir({FARE_SAMPLE_ROWS} ROWS) REPEATABLE ({seed})
    """).df()
    X = np.column_stack([np.ones(len(sample)), sample["trip_miles"], sample["minutes"]])
    y = sample["fare"].to_numpy()
    coef, *_ = np.linalg.lstsq(X, y, rcond=None)
    r2 = 1 - ((y - X @ coef) ** 2).sum() / ((y - y.mean()) ** 2).sum()
    return {
        "base_fare": round(float(coef[0]), 2),
        "per_mile": round(float(coef[1]), 2),
        "per_minute": round(float(coef[2]), 2),
        "r2": round(float(r2), 3),
        "rows": len(sample),
    }


def sample_request_times(demand: pd.DataFrame, n: int, rng: Random) -> Iterator[datetime]:
    """Request timestamps whose weekday and hour follow the real demand profile."""
    slots = list(zip(demand["day_of_week"], demand["hour"]))
    for dow, hour in rng.choices(slots, weights=demand["share"].tolist(), k=n):
        yield WEEK_START + timedelta(days=int(dow) - 1, hours=int(hour), seconds=rng.randint(0, 3599))


def duration_correction(router: Router, od: pd.DataFrame, speeds: pd.DataFrame,
                        demand: pd.DataFrame, real_median_minutes: float, rng: Random) -> float:
    """Global multiplier that makes the simulated median duration equal the real one."""
    planner = TripPlanner(router, Calibration(od=od, speeds=speeds), traffic_noise=0)
    minutes = [
        planner.build_trip(*planner.sample_od(rng), start, rng).duration_s / 60
        for start in sample_request_times(demand, FREE_FLOW_SAMPLE_SIZE, rng)
    ]
    return real_median_minutes / statistics.median(minutes)


def free_flow_sample(router: Router, od: pd.DataFrame, rng: Random) -> dict:
    """Route OD pairs drawn from the real distribution, at OSM speed limits."""
    pairs = rng.choices(
        list(zip(od["pickup_zone"], od["dropoff_zone"])),
        weights=od["share"].tolist(),
        k=FREE_FLOW_SAMPLE_SIZE,
    )
    plans = [router.plan(int(p), int(d), rng) for p, d in pairs]
    miles = [p.distance_m / METERS_PER_MILE for p in plans]
    mph = [m / (p.duration_s / 3600) for m, p in zip(miles, plans)]
    return {"median_miles": round(statistics.median(miles), 2), "median_mph": round(statistics.median(mph), 2)}


def apply_fare_model() -> dict:
    """Load the fitted fare rates into FareCalculator (call once at simulator start)."""
    from database.utils.fare_calculator import FareCalculator

    model = json.loads(CALIBRATION_SUMMARY_JSON.read_text())["fare_model"]
    FareCalculator.set_rates(model["base_fare"], model["per_mile"], model["per_minute"])
    return model


def calibrate(seed: int = 42) -> dict:
    routable = set(pd.read_csv(ZONE_NODES_CSV)["LocationID"])
    con = _connect(routable)

    od = od_matrix(con)
    speeds = real_speed_by_hour(con)
    demand = demand_by_hour(con)
    real = real_trip_summary(con)

    router = Router()
    logger.info("Routing %d real-distribution trips for free-flow speed...", FREE_FLOW_SAMPLE_SIZE)
    free_flow = free_flow_sample(router, od, Random(seed))

    # Traffic factor: multiply free-flow route time by this to get real time
    speeds["traffic_factor"] = free_flow["median_mph"] / speeds["real_mph"]
    logger.info("Simulating %d trips to correct the median duration...", FREE_FLOW_SAMPLE_SIZE)
    correction = duration_correction(router, od, speeds, demand, real["median_minutes"], Random(seed))
    speeds["traffic_factor"] = (speeds["traffic_factor"] * correction).round(3)
    speeds["real_mph"] = speeds["real_mph"].round(2)

    logger.info("Fitting the fare model on %d real trips...", FARE_SAMPLE_ROWS)
    fare_model = fit_fare_model(con, seed)

    CALIBRATION_DIR.mkdir(parents=True, exist_ok=True)
    od.to_csv(OD_MATRIX_CSV, index=False)
    speeds.to_csv(SPEED_BY_HOUR_CSV, index=False)
    demand.to_csv(DEMAND_BY_HOUR_CSV, index=False)

    summary = {
        "real": real,
        "free_flow_routes": free_flow,
        "duration_correction": round(correction, 3),
        "fare_model": fare_model,
        "od_pairs": len(od),
        "od_trips_kept": int(od["trips"].sum()),
        "top10_pair_share": round(float(od["share"].head(10).sum()), 4),
        "traffic_factor_range": [float(speeds["traffic_factor"].min()), float(speeds["traffic_factor"].max())],
    }
    CALIBRATION_SUMMARY_JSON.write_text(json.dumps(summary, indent=2))
    return summary


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    print(json.dumps(calibrate(), indent=2))
