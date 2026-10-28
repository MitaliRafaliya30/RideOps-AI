"""
Run simulator v2.

  backfill  generate N rides as fast as possible into Parquet files
            (training data; time-compressed over a date range)
  live      generate rides now and send every message at its real time to
            Event Hubs (`ride-events`, `ride-gps`), or to a JSONL file with --dry-run

Examples
  python -m simulation.run backfill --rides 50000 --start 2026-05-04 --days 7
  python -m simulation.run live --rate 1 --duration 600 --dry-run --speed 20
  python -m simulation.run live --rate 1 --duration 1800

Live mode reads EVENT_HUBS_CONNECTION_STRING from .env. If the GPS hub uses
a different access policy, set GPS_EVENT_HUBS_CONNECTION_STRING too.
"""

import argparse
import heapq
import itertools
import json
import logging
import os
import time
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from random import Random
from typing import Callable, Dict, List, Optional

import pandas as pd

from geodata.config import REPO_ROOT
from simulation.config import DEMAND_BY_HOUR_CSV
from simulation.rides import RIDE_EVENTS_HUB, RIDE_GPS_HUB, RideFactory

logger = logging.getLogger("simulation.run")

BACKFILL_DIR = REPO_ROOT / "data" / "backfill"
LIVE_DRY_RUN_DIR = REPO_ROOT / "output" / "live"


# ---------------- setup ----------------

def build_factory(seed: int, incident_rate_scale: float = 1.0) -> RideFactory:
    """Wire the real pieces: Postgres master data, NYC roads, calibration, fares."""
    from database.generators.defect_injector import DefectInjector
    from database.generators.entity_pool import EntityPool
    from database.generators.ride_event_generator import RideEventGenerator
    from database.utils.config_loader import load_config
    from database.utils.database import PostgresConnection
    from geodata.router import Router
    from simulation.behaviors import BehaviorConfig, BehaviorInjector
    from simulation.calibrate import apply_fare_model
    from simulation.gps import GpsGenerator
    from simulation.trip_planner import Calibration, TripPlanner

    config = load_config(None, [])
    rng = Random(seed)
    behavior_config = BehaviorConfig.from_config(config.get("safety") or {})
    behavior_config.incident_rates = {k: v * incident_rate_scale for k, v in behavior_config.incident_rates.items()}
    gps_config = config.get("gps") or {}

    apply_fare_model()
    router = Router()
    planner = TripPlanner(router, Calibration(), injector=BehaviorInjector(router, behavior_config))
    generator = RideEventGenerator(EntityPool(PostgresConnection(), seed=seed), config, rng, trip_planner=planner)
    gps = GpsGenerator(rng,
                       interval_s=float(gps_config.get("ping_interval_s", 10)),
                       noise_m=float(gps_config.get("position_noise_m", 4)),
                       speed_noise=float(gps_config.get("speed_noise", 0.05)))
    return RideFactory(generator, DefectInjector(config, Random(seed + 1)), gps)


def request_time_sampler(demand: pd.DataFrame, start: datetime, days: int, rng: Random) -> Callable[[], datetime]:
    """Request times across [start, start + days) that follow the real weekday and hour profile."""
    by_dow = {dow: grp for dow, grp in demand.groupby("day_of_week")}
    dates = [start + timedelta(days=d) for d in range(days)]
    day_weights = [by_dow[d.isoweekday()]["share"].sum() for d in dates]

    def sample() -> datetime:
        day = rng.choices(dates, weights=day_weights)[0]
        hours = by_dow[day.isoweekday()]
        hour = rng.choices(hours["hour"].tolist(), weights=hours["share"].tolist())[0]
        return day + timedelta(hours=int(hour), seconds=rng.randint(0, 3599))

    return sample


# ---------------- backfill ----------------

class ParquetWriter:
    """Buffers rows per table and writes numbered Parquet part files."""

    def __init__(self, out_dir: Path):
        self.out_dir = out_dir
        self.buffers: Dict[str, List[Dict]] = defaultdict(list)
        self.parts: Counter = Counter()
        self.rows: Counter = Counter()

    def add(self, table: str, rows: List[Dict]) -> None:
        self.buffers[table].extend(rows)

    def flush(self) -> None:
        for table, rows in self.buffers.items():
            if not rows:
                continue
            folder = self.out_dir / table
            folder.mkdir(parents=True, exist_ok=True)
            pd.DataFrame(rows).to_parquet(folder / f"part-{self.parts[table]:05d}.parquet", index=False)
            self.parts[table] += 1
            self.rows[table] += len(rows)
        self.buffers.clear()


def backfill(factory: RideFactory, n_rides: int, start: datetime, days: int, out_dir: Path,
             seed: int, flush_every: int = 5000, demand: Optional[pd.DataFrame] = None) -> Dict:
    rng = Random(seed)
    demand = demand if demand is not None else pd.read_csv(DEMAND_BY_HOUR_CSV)
    sample_time = request_time_sampler(demand, start, days, rng)
    writer = ParquetWriter(out_dir)
    trips, incidents, behaviors = 0, Counter(), Counter()
    started = time.time()

    for i in range(1, n_rides + 1):
        ride = factory.make(sample_time())
        if ride is None:
            continue
        for m in ride.stream:
            emit_at = m.emit_at.isoformat(timespec="milliseconds") + "Z"
            if m.hub == RIDE_EVENTS_HUB:
                writer.add("ride_events", [{"ride_id": m.ride_id, "event_type": str(m.body.get("event_type")),
                                            "emit_at": emit_at, "raw_json": json.dumps(m.body)}])
            else:
                table = "route_plans" if m.body["message_type"] == "route_plan" else "gps_pings"
                writer.add(table, [m.body])
        writer.add("incident_labels", ride.incident_labels)
        writer.add("behaviors", ride.behaviors)

        if ride.bodies(RIDE_GPS_HUB, "route_plan"):
            trips += 1
        incidents.update(label["incident_type"] for label in ride.incident_labels)
        behaviors.update({b["kind"] for b in ride.behaviors})

        if i % flush_every == 0:
            writer.flush()
            logger.info("%d / %d rides (%.0fs)", i, n_rides, time.time() - started)
    writer.flush()

    manifest = {
        "mode": "backfill",
        "seed": seed,
        "rides": n_rides,
        "completed_trips_with_gps": trips,
        "window": [start.isoformat(), (start + timedelta(days=days)).isoformat()],
        "rows": dict(writer.rows),
        "incidents": dict(incidents),
        "measured_incident_rate": round(sum(incidents.values()) / trips, 4) if trips else 0.0,
        "trips_with_behavior": dict(behaviors.most_common()),
        "elapsed_s": round(time.time() - started, 1),
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "manifest.json").write_text(json.dumps(manifest, indent=2))
    return manifest


# ---------------- live ----------------

def live(factory: RideFactory, sink, rate: float, duration_s: float, seed: int, speed: float = 1.0,
         clock: Callable[[], float] = time.monotonic, sleep: Callable[[float], None] = time.sleep,
         now: Optional[datetime] = None, log_every_s: float = 10.0) -> Dict:
    """
    Rides arrive as a Poisson process (`rate` per second). Every message is
    sent when its timestamp is reached, so a 20-minute trip takes 20 minutes
    (or 20 / speed minutes with --speed). New rides stop after `duration_s`;
    rides already on the road finish and send their remaining pings.
    """
    rng = Random(seed)
    sim_start = now or datetime.now(timezone.utc).replace(tzinfo=None)
    wall_start = clock()
    sim_now = lambda: sim_start + timedelta(seconds=(clock() - wall_start) * speed)

    queue, tie = [], itertools.count()
    next_ride_at, stop_new_at = sim_start, sim_start + timedelta(seconds=duration_s)
    rides, last_log = 0, clock()

    try:
        while True:
            current = sim_now()
            while next_ride_at <= current and next_ride_at < stop_new_at:
                ride = factory.make(next_ride_at)
                if ride:
                    rides += 1
                    for m in ride.stream:
                        heapq.heappush(queue, (m.emit_at, next(tie), m))
                next_ride_at += timedelta(seconds=rng.expovariate(rate))

            due = defaultdict(list)
            while queue and queue[0][0] <= current:
                m = heapq.heappop(queue)[2]
                due[(m.hub, m.ride_id)].append(m.body)
            for (hub, ride_id), bodies in due.items():
                sink.send(hub, bodies, ride_id)

            if not queue and next_ride_at >= stop_new_at:
                break
            if clock() - last_log >= log_every_s:
                logger.info("rides started %d | queued %d | sent %s | failed %s",
                            rides, len(queue), dict(sink.sent), dict(sink.failed))
                last_log = clock()

            next_due = min(queue[0][0] if queue else stop_new_at,
                           next_ride_at if next_ride_at < stop_new_at else stop_new_at)
            sleep(min(max((next_due - sim_now()).total_seconds() / speed, 0.0), 1.0))
    except KeyboardInterrupt:
        logger.info("Stopped by user; %d messages were still queued", len(queue))
    finally:
        sink.close()

    return {"mode": "live", "rides": rides, "sent": dict(sink.sent), "failed": dict(sink.failed)}


# ---------------- CLI ----------------

def main(argv: Optional[List[str]] = None) -> None:
    parser = argparse.ArgumentParser(description="RideOps simulator v2")
    sub = parser.add_subparsers(dest="mode", required=True)

    b = sub.add_parser("backfill", help="generate training data into Parquet")
    b.add_argument("--rides", type=int, default=50_000)
    b.add_argument("--start", default="2026-05-04", help="first day (YYYY-MM-DD)")
    b.add_argument("--days", type=int, default=7)
    b.add_argument("--seed", type=int, default=42)
    b.add_argument("--incident-scale", type=float, default=1.0,
                   help="multiply incident rates (e.g. 5 for more training positives)")
    b.add_argument("--out", type=Path, default=None)

    lv = sub.add_parser("live", help="stream rides in real time")
    lv.add_argument("--rate", type=float, default=1.0, help="new rides per second")
    lv.add_argument("--duration", type=float, default=600, help="seconds of new rides")
    lv.add_argument("--speed", type=float, default=1.0, help="time acceleration (dry runs)")
    lv.add_argument("--seed", type=int, default=7)
    lv.add_argument("--incident-scale", type=float, default=1.0)
    lv.add_argument("--dry-run", action="store_true", help="write to output/live/*.jsonl instead of Azure")

    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    logging.getLogger("database").setLevel(logging.WARNING)

    factory = build_factory(args.seed, args.incident_scale)

    if args.mode == "backfill":
        start = datetime.fromisoformat(args.start)
        out = args.out or BACKFILL_DIR / f"run_{args.start}_{args.days}d_{args.rides}r_seed{args.seed}"
        summary = backfill(factory, args.rides, start, args.days, out, args.seed)
        logger.info("Backfill written to %s", out)
    else:
        if args.dry_run:
            from simulation.sinks import JsonlSink
            stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            sink = JsonlSink(LIVE_DRY_RUN_DIR / f"live_{stamp}.jsonl")
        else:
            from dotenv import load_dotenv
            from simulation.sinks import EventHubSink
            load_dotenv()
            main_conn = os.environ["EVENT_HUBS_CONNECTION_STRING"]
            sink = EventHubSink({
                RIDE_EVENTS_HUB: main_conn,
                RIDE_GPS_HUB: os.environ.get("GPS_EVENT_HUBS_CONNECTION_STRING", main_conn),
            })
        summary = live(factory, sink, args.rate, args.duration, args.seed, speed=args.speed)

    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
