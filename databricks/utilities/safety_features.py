"""
Rider-safety features from GPS pings.

ONE implementation used everywhere, so training and live scoring can never
compute a feature differently (training-serving skew):
  - locally, on the simulator backfill, for EDA and model training
  - in the Databricks Silver pipeline (applyInPandas), for backfill and live GPS

Plain pandas + numpy only, no Spark and no extra packages: the file runs the
same on a laptop and on Databricks serverless.

Two levels:
  ping_features()     one row per GPS ping, needs the ride's route plan
  window_features()   one row per ride per 60-second window: the model input

Distances use a local flat projection around each route (equirectangular).
Over a city-sized area the error is well under 0.5 %.
"""

import math
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

EARTH_RADIUS_M = 6_371_000.0

STATIONARY_KMH = 2.0      # slower than this counts as standing still
GAP_S = 30.0              # no ping for longer than this = a gap (pings come every 10 s)
JUMP_KMH = 250.0          # implied speed above this is physically impossible in a city
MIN_DT_S = 2.0            # pings closer than this give no speed: 4 m GPS noise over 0.05 s reads as 600 km/h
OFF_ROUTE_M = 100.0       # further than this from the route counts as off route (GPS noise stays under 20 m)
WINDOW_S = 60             # model window size
ENDED_AFTER_S = 60.0      # a ride whose last ping is this much older than the newest data has ended
NIGHT_HOURS = {22, 23, 0, 1, 2, 3, 4, 5}

# NYC bounding box, a little wider than the five boroughs
NYC_LAT = (40.47, 40.93)
NYC_LON = (-74.27, -73.68)

# ---------------------------------------------------------------------------
# Output schemas (Spark DDL strings, used by applyInPandas in the pipeline)
# ---------------------------------------------------------------------------
PING_FEATURES_DDL = (
    "ride_id STRING, source STRING, message_id STRING, seq_no INT, ping_ts TIMESTAMP, "
    "lat DOUBLE, lon DOUBLE, speed_kmh DOUBLE, accuracy_m DOUBLE, "
    "planned_at TIMESTAMP, has_route_plan BOOLEAN, elapsed_s DOUBLE, dt_prev_s DOUBLE, "
    "step_m DOUBLE, implied_speed_kmh DOUBLE, decel_kmh_per_s DOUBLE, stop_run_s DOUBLE, "
    "dist_off_route_m DOUBLE, off_route_run_s DOUBLE, progress_pct DOUBLE, dist_to_dropoff_m DOUBLE, "
    "away_from_dropoff_m DOUBLE, time_vs_plan DOUBLE, is_stationary BOOLEAN, is_gap BOOLEAN, is_jump BOOLEAN, is_outside_nyc BOOLEAN"
)

WINDOW_FEATURES_DDL = (
    "ride_id STRING, source STRING, window_index INT, window_start TIMESTAMP, window_end TIMESTAMP, "
    "n_pings INT, dist_off_route_max_m DOUBLE, dist_off_route_mean_m DOUBLE, off_route_run_max_s DOUBLE, "
    "stop_run_max_s DOUBLE, stationary_share DOUBLE, speed_mean_kmh DOUBLE, speed_max_kmh DOUBLE, "
    "decel_max_kmh_per_s DOUBLE, implied_speed_max_kmh DOUBLE, gap_max_s DOUBLE, n_jumps INT, "
    "outside_nyc_share DOUBLE, progress_pct_last DOUBLE, dist_to_dropoff_last_m DOUBLE, away_from_dropoff_max_m DOUBLE, "
    "time_vs_plan_last DOUBLE, speed_before_stop_kmh DOUBLE, hour_of_day INT, is_night BOOLEAN, "
    "is_final_window BOOLEAN, has_route_plan BOOLEAN"
)

PING_COLUMNS = [c.split()[0] for c in PING_FEATURES_DDL.split(", ")]
WINDOW_COLUMNS = [c.split()[0] for c in WINDOW_FEATURES_DDL.split(", ")]


# ---------------------------------------------------------------------------
# Geometry
# ---------------------------------------------------------------------------
def decode_polyline(encoded: str, precision: int = 5) -> List[Tuple[float, float]]:
    """Decode a Google encoded polyline into (lat, lon) pairs."""
    coords, index, lat, lon = [], 0, 0, 0
    factor = 10 ** precision
    while index < len(encoded):
        for axis in (0, 1):
            shift = result = 0
            while True:
                b = ord(encoded[index]) - 63
                index += 1
                result |= (b & 0x1F) << shift
                shift += 5
                if b < 0x20:
                    break
            delta = ~(result >> 1) if result & 1 else result >> 1
            if axis == 0:
                lat += delta
            else:
                lon += delta
        coords.append((lat / factor, lon / factor))
    return coords


def haversine_m(lat1, lon1, lat2, lon2):
    """Vectorised great-circle distance in meters."""
    lat1, lon1, lat2, lon2 = map(np.radians, (lat1, lon1, lat2, lon2))
    h = np.sin((lat2 - lat1) / 2) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin((lon2 - lon1) / 2) ** 2
    return 2 * EARTH_RADIUS_M * np.arcsin(np.sqrt(h))


class Route:
    """A planned route in a local flat frame, for distance-to-route and progress."""

    def __init__(self, coords: List[Tuple[float, float]]):
        pts = np.asarray(coords, dtype=float)
        self.lat0, self.lon0 = pts[0]
        self.ky = math.radians(1) * EARTH_RADIUS_M
        self.kx = self.ky * math.cos(math.radians(pts[:, 0].mean()))
        xy = self._to_xy(pts[:, 0], pts[:, 1])
        if len(xy) == 1:                       # degenerate route: a single point
            xy = np.vstack([xy, xy])
        self.a = xy[:-1]
        self.d = xy[1:] - xy[:-1]
        self.seg_len = np.hypot(self.d[:, 0], self.d[:, 1])
        self.cum_len = np.concatenate([[0.0], np.cumsum(self.seg_len)[:-1]])
        self.length = float(self.seg_len.sum())

    def _to_xy(self, lat, lon):
        return np.column_stack([(np.asarray(lon) - self.lon0) * self.kx, (np.asarray(lat) - self.lat0) * self.ky])

    def project(self, lat, lon, chunk: int = 256) -> Tuple[np.ndarray, np.ndarray]:
        """For each point: distance to the nearest route segment, and distance travelled along the route."""
        p = self._to_xy(lat, lon)
        seg_sq = np.maximum(self.seg_len ** 2, 1e-9)
        off, along = np.empty(len(p)), np.empty(len(p))
        for start in range(0, len(p), chunk):            # bounded memory on very long routes
            q = p[start:start + chunk, None, :]                       # (k, 1, 2)
            t = np.clip(((q - self.a) * self.d).sum(-1) / seg_sq, 0.0, 1.0)   # (k, m)
            closest = self.a + t[..., None] * self.d
            dist = np.hypot(*(q - closest).transpose(2, 0, 1))
            best = dist.argmin(axis=1)
            rows = np.arange(len(best))
            off[start:start + chunk] = dist[rows, best]
            along[start:start + chunk] = self.cum_len[best] + t[rows, best] * self.seg_len[best]
        return off, along


# ---------------------------------------------------------------------------
# Per-ping features
# ---------------------------------------------------------------------------
def _naive_utc(values):
    """
    Timestamps arrive as ISO text with Z (raw messages) or as naive UTC
    datetimes (from Spark). Make both naive UTC so they can be compared.
    """
    if isinstance(values, pd.Series):
        ts = pd.to_datetime(values, utc=True)
        return ts.dt.tz_localize(None)
    ts = pd.Timestamp(values)
    return ts.tz_convert("UTC").tz_localize(None) if ts.tzinfo is not None else ts


def _runs(state: np.ndarray, dt: np.ndarray) -> np.ndarray:
    """Seconds a condition has held without a break at each ping (0 when it does not hold)."""
    run = np.zeros(len(state))
    for i in range(1, len(state)):
        if state[i] and state[i - 1]:
            run[i] = run[i - 1] + dt[i]
    return run


def ping_features(pings: pd.DataFrame, plan: Optional[Dict]) -> pd.DataFrame:
    """
    pings: all pings of ONE ride, columns ride_id, source, message_id, seq_no,
           ping_ts (datetime), lat, lon, speed_kmh, accuracy_m
    plan:  the ride's route plan (route_polyline, planned_at, planned_duration_s,
           dropoff_lat, dropoff_lon), or None if it has not arrived
    """
    if pings.empty:
        return pd.DataFrame(columns=PING_COLUMNS)

    df = (
        pings.drop_duplicates("message_id")          # at-least-once delivery
        .sort_values(["ping_ts", "seq_no"])            # event time, not arrival order
        .reset_index(drop=True)
    )
    ts = _naive_utc(df["ping_ts"])
    lat, lon = df["lat"].to_numpy(float), df["lon"].to_numpy(float)
    speed = df["speed_kmh"].to_numpy(float)

    dt = ts.diff().dt.total_seconds().to_numpy()
    step = np.full(len(df), np.nan)
    step[1:] = haversine_m(lat[:-1], lon[:-1], lat[1:], lon[1:])
    with np.errstate(divide="ignore", invalid="ignore"):
        implied = np.where(dt >= MIN_DT_S, step / dt * 3.6, np.nan)
        decel = np.where(dt >= MIN_DT_S, (np.roll(speed, 1) - speed) / dt, np.nan)
    decel[0] = np.nan

    stationary = speed < STATIONARY_KMH
    out = pd.DataFrame({
        "ride_id": df["ride_id"],
        "source": df["source"],
        "message_id": df["message_id"],
        "seq_no": df["seq_no"].astype("int32"),
        "ping_ts": ts,
        "lat": lat, "lon": lon, "speed_kmh": speed,
        "accuracy_m": df["accuracy_m"].to_numpy(float),
        "dt_prev_s": dt,
        "step_m": step,
        "implied_speed_kmh": implied,
        "decel_kmh_per_s": decel,
        "stop_run_s": _runs(stationary, np.nan_to_num(dt)),
        "is_stationary": stationary,
        "is_gap": np.nan_to_num(dt) > GAP_S,
        "is_jump": np.nan_to_num(implied) > JUMP_KMH,
        "is_outside_nyc": ~((lat >= NYC_LAT[0]) & (lat <= NYC_LAT[1]) & (lon >= NYC_LON[0]) & (lon <= NYC_LON[1])),
    })

    if plan is not None and plan.get("route_polyline"):
        route = Route(decode_polyline(plan["route_polyline"]))
        off, along = route.project(lat, lon)
        planned_at = _naive_utc(plan["planned_at"])
        elapsed = (ts - planned_at).dt.total_seconds().to_numpy()
        out["planned_at"] = planned_at
        out["has_route_plan"] = True
        out["elapsed_s"] = elapsed
        out["dist_off_route_m"] = off
        # How long the car has stayed off route: an innocent detour rejoins within
        # minutes, a deviation keeps going (distance alone overlaps between the two)
        out["off_route_run_s"] = _runs(off > OFF_ROUTE_M, np.nan_to_num(dt))
        out["progress_pct"] = np.clip(along / max(route.length, 1.0), 0.0, 1.0)
        to_dropoff = haversine_m(lat, lon, plan["dropoff_lat"], plan["dropoff_lon"])
        out["dist_to_dropoff_m"] = to_dropoff
        # How much further from the destination than the closest the car has been
        # so far: a driver on a different but sensible road keeps closing in,
        # a deviation drives away (uses only past pings, so it works live)
        out["away_from_dropoff_m"] = to_dropoff - np.minimum.accumulate(to_dropoff)
        out["time_vs_plan"] = elapsed / max(float(plan["planned_duration_s"]), 1.0)
    else:
        # Plan not (yet) received: keep the ping, route features unknown
        first = ts.iloc[0]
        out["planned_at"] = pd.NaT
        out["has_route_plan"] = False
        out["elapsed_s"] = (ts - first).dt.total_seconds().to_numpy()
        for column in ("dist_off_route_m", "off_route_run_s", "progress_pct", "dist_to_dropoff_m",
                       "away_from_dropoff_m", "time_vs_plan"):
            out[column] = np.nan

    return out[PING_COLUMNS]


# ---------------------------------------------------------------------------
# Window features (model input)
# ---------------------------------------------------------------------------
def window_features(pf: pd.DataFrame, ride_ended: bool) -> pd.DataFrame:
    """
    pf: ping_features() output for ONE ride.
    ride_ended: True when the ride has finished (no newer pings will come); only
    then can the last window say "the trip ended here" (wrong drop-off, crash).
    """
    if pf.empty:
        return pd.DataFrame(columns=WINDOW_COLUMNS)

    pf = pf.sort_values("ping_ts").reset_index(drop=True)
    elapsed = pf["elapsed_s"].clip(lower=0)
    pf = pf.assign(window_index=(elapsed // WINDOW_S).astype(int))

    # Speed just before the current stop started, carried through the stop:
    # a stop that began from 40 km/h looks different from a slow crawl to a halt
    moving_speed = pf["speed_kmh"].where(~pf["is_stationary"])
    pf = pf.assign(speed_before_stop_kmh=moving_speed.ffill().where(pf["is_stationary"]))

    start = pf["planned_at"].iloc[0] if pf["has_route_plan"].iloc[0] else pf["ping_ts"].iloc[0]
    pf = pf.assign(
        is_stationary=pf["is_stationary"].astype(float),
        is_outside_nyc=pf["is_outside_nyc"].astype(float),
        is_jump=pf["is_jump"].astype(int),
    )

    # One vectorised pass over all windows of the ride (no Python per window)
    grouped = pf.groupby("window_index", sort=True)
    out = grouped.agg(
        n_pings=("ping_ts", "size"),
        dist_off_route_max_m=("dist_off_route_m", "max"),
        dist_off_route_mean_m=("dist_off_route_m", "mean"),
        off_route_run_max_s=("off_route_run_s", "max"),
        away_from_dropoff_max_m=("away_from_dropoff_m", "max"),
        stop_run_max_s=("stop_run_s", "max"),
        stationary_share=("is_stationary", "mean"),
        speed_mean_kmh=("speed_kmh", "mean"),
        speed_max_kmh=("speed_kmh", "max"),
        decel_max_kmh_per_s=("decel_kmh_per_s", "max"),
        implied_speed_max_kmh=("implied_speed_kmh", "max"),
        gap_max_s=("dt_prev_s", "max"),
        n_jumps=("is_jump", "sum"),
        outside_nyc_share=("is_outside_nyc", "mean"),
        speed_before_stop_kmh=("speed_before_stop_kmh", "max"),
    )
    # Values at each window's last ping. tail(1) keeps NaN as NaN, unlike
    # groupby "last", which would skip it and report an older ping's value.
    last = grouped.tail(1).set_index("window_index")[
        ["progress_pct", "dist_to_dropoff_m", "time_vs_plan", "has_route_plan"]
    ].rename(columns={
        "progress_pct": "progress_pct_last",
        "dist_to_dropoff_m": "dist_to_dropoff_last_m",
        "time_vs_plan": "time_vs_plan_last",
    })
    out = out.join(last).reset_index()
    out["has_route_plan"] = out["has_route_plan"].astype(bool)

    out["window_start"] = start + pd.to_timedelta(out["window_index"] * WINDOW_S, unit="s")
    out["window_end"] = out["window_start"] + pd.Timedelta(seconds=WINDOW_S)
    out["ride_id"] = pf["ride_id"].iloc[0]
    out["source"] = pf["source"].iloc[0]
    out["hour_of_day"] = out["window_start"].dt.hour.astype("int32")
    out["is_night"] = out["hour_of_day"].isin(NIGHT_HOURS)
    out["is_final_window"] = bool(ride_ended) & (out["window_index"] == out["window_index"].max())
    for column in ("n_pings", "window_index", "n_jumps"):
        out[column] = out[column].astype("int32")
    return out[WINDOW_COLUMNS]


# ---------------------------------------------------------------------------
# Ride history (model input on top of the current window)
# ---------------------------------------------------------------------------
HISTORY_DDL = (
    "dist_off_route_max_m_prev DOUBLE, away_from_dropoff_max_m_prev DOUBLE, stop_run_max_s_prev DOUBLE, "
    "time_vs_plan_last_prev DOUBLE, off_route_max_so_far_m DOUBLE, away_max_so_far_m DOUBLE, "
    "off_route_growth_m DOUBLE, stationary_share_3w DOUBLE, minutes_into_ride INT"
)
HISTORY_COLUMNS = [c.split()[0] for c in HISTORY_DDL.split(", ")]


def history_features(wf: pd.DataFrame) -> pd.DataFrame:
    """
    wf: window_features() rows of one or many rides.
    Adds what the ride did BEFORE this window: the previous window, the worst
    so far, and the trend. Only past windows are used, so a live window gets
    the same values it will have in training. One window alone cannot tell a
    deviation that keeps growing from a detour that is already turning back.
    """
    if wf.empty:
        return wf.assign(**{c: pd.Series(dtype=float) for c in HISTORY_COLUMNS})
    out = wf.sort_values(["ride_id", "window_index"]).copy()
    by_ride = out.groupby("ride_id", sort=False)
    for column in ("dist_off_route_max_m", "away_from_dropoff_max_m", "stop_run_max_s", "time_vs_plan_last"):
        out[f"{column}_prev"] = by_ride[column].shift(1)
    out["off_route_max_so_far_m"] = by_ride["dist_off_route_max_m"].cummax()
    out["away_max_so_far_m"] = by_ride["away_from_dropoff_max_m"].cummax()
    out["off_route_growth_m"] = out["dist_off_route_max_m"] - out["dist_off_route_max_m_prev"]
    out["stationary_share_3w"] = (by_ride["stationary_share"].rolling(3, min_periods=1).mean()
                                  .reset_index(level=0, drop=True))
    out["minutes_into_ride"] = out["window_index"].astype("int32")
    return out
