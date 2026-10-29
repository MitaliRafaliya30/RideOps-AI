"""
Build the local feature cache from a simulator backfill run.

Runs the shared feature module on every ride, exactly like the Silver
pipeline does with applyInPandas, and writes:

  data/features/<run>/ping_features.parquet     one row per GPS ping
  data/features/<run>/window_features.parquet   one row per ride per 60 s window

Rides are independent, so they are split into chunks and processed in
parallel (one process per CPU core).

  python -m ml.build_features
  python -m ml.build_features --run run_2026-05-04_7d_50000r_seed42 --workers 8
"""

import argparse
import logging
import os
import time
from multiprocessing import Pool

import numpy as np
import pandas as pd

from ml.config import BACKFILL_DIR, DEFAULT_RUN, FEATURES_DIR
from utilities.safety_features import ping_features, window_features

logger = logging.getLogger("ml.build_features")

PING_INPUT = ["message_id", "ride_id", "seq_no", "ping_ts", "lat", "lon", "speed_kmh", "accuracy_m"]
PLAN_INPUT = ["ride_id", "route_polyline", "planned_at", "planned_duration_s", "dropoff_lat", "dropoff_lon"]


def _features_for_chunk(args):
    pings, plans = args
    plan_by_ride = plans.set_index("ride_id").to_dict("index")
    ping_parts, window_parts = [], []
    for ride_id, ride_pings in pings.groupby("ride_id", sort=False):
        pf = ping_features(ride_pings, plan_by_ride.get(ride_id))
        ping_parts.append(pf)
        window_parts.append(window_features(pf, ride_ended=True))     # a backfill ride is complete
    return pd.concat(ping_parts, ignore_index=True), pd.concat(window_parts, ignore_index=True)


def build(run: str = DEFAULT_RUN, workers: int = os.cpu_count() or 1, chunks_per_worker: int = 4):
    run_dir = BACKFILL_DIR / run
    out_dir = FEATURES_DIR / run
    out_dir.mkdir(parents=True, exist_ok=True)

    started = time.time()
    pings = pd.read_parquet(run_dir / "gps_pings", columns=PING_INPUT).assign(source=run)
    plans = pd.read_parquet(run_dir / "route_plans", columns=PLAN_INPUT)
    logger.info("loaded %d pings, %d plans", len(pings), len(plans))

    # Split by ride: every chunk holds complete rides
    ride_ids = pings["ride_id"].unique()
    n_chunks = max(1, workers * chunks_per_worker)
    chunk_of_ride = pd.Series(np.arange(len(ride_ids)) % n_chunks, index=ride_ids)
    pings["_chunk"] = pings["ride_id"].map(chunk_of_ride)
    plans["_chunk"] = plans["ride_id"].map(chunk_of_ride)
    tasks = [(p.drop(columns="_chunk"), plans[plans["_chunk"] == c].drop(columns="_chunk"))
             for c, p in pings.groupby("_chunk")]
    del pings

    with Pool(workers) as pool:
        results = pool.map(_features_for_chunk, tasks)

    ping_feats = pd.concat([r[0] for r in results], ignore_index=True)
    window_feats = pd.concat([r[1] for r in results], ignore_index=True)
    ping_feats.to_parquet(out_dir / "ping_features.parquet", index=False)
    window_feats.to_parquet(out_dir / "window_features.parquet", index=False)
    logger.info("wrote %d ping rows, %d window rows to %s in %.0f s",
                len(ping_feats), len(window_feats), out_dir, time.time() - started)
    return ping_feats, window_feats


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--run", default=DEFAULT_RUN)
    parser.add_argument("--workers", type=int, default=os.cpu_count() or 1)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    build(args.run, args.workers)


if __name__ == "__main__":
    main()
