"""
Turn the simulator ground truth into training targets.

The model scores 60-second windows, so labels must be per window too:

  incident_type   the incident running during the window, or "normal"
  is_incident     incident_type != "normal"
  after_incident  a normal-labelled window after the ride's incident ended
                  (the car may still look abnormal: excluded, see below)
  context         harmless behaviour running during the window that LOOKS
                  suspicious (innocent long stop, innocent detour, GPS dropout),
                  or "none". Never a target: used to explain false alarms.

Window <-> incident matching:
  - a window is an incident window when it overlaps [start_ts, end_ts]
  - possible_crash has no end (the car never moves again): open until the ride ends
  - wrong_dropoff is a single moment (the trip ends early): only the ride's
    final window carries it, since only then is the ride known to be over

Labels never reach the stream (ADR-010): this module is used for training
and evaluation only.
"""

import pandas as pd

from ml.config import BACKFILL_DIR, DEFAULT_RUN, TRAIN_END, VALID_END

INCIDENT_TYPES = ["route_deviation", "long_stop", "wrong_dropoff", "possible_crash", "gps_spoofing"]
HARD_NEGATIVES = ["innocent_long_stop", "innocent_detour", "gps_dropout"]   # priority order


def _naive_utc(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, utc=True).dt.tz_localize(None)


def load_labels(run: str = DEFAULT_RUN) -> pd.DataFrame:
    labels = pd.read_parquet(BACKFILL_DIR / run / "incident_labels")
    return labels.assign(start_ts=_naive_utc(labels["start_ts"]), end_ts=_naive_utc(labels["end_ts"]))


def load_behaviors(run: str = DEFAULT_RUN) -> pd.DataFrame:
    behaviors = pd.read_parquet(BACKFILL_DIR / run / "behaviors")
    return behaviors.assign(start_ts=_naive_utc(behaviors["start_ts"]), end_ts=_naive_utc(behaviors["end_ts"]))


def _overlapping(windows: pd.DataFrame, spans: pd.DataFrame) -> pd.DataFrame:
    """(window row index, span columns) for every window overlapping a span of the same ride."""
    w = windows[["ride_id", "window_start", "window_end"]].reset_index(names="_row")
    m = w.merge(spans, on="ride_id")
    end = m["end_ts"].fillna(pd.Timestamp.max)                      # open span: until the ride ends
    return m[(m["window_start"] <= end) & (m["window_end"] > m["start_ts"])]


def label_windows(windows: pd.DataFrame, labels: pd.DataFrame, behaviors: pd.DataFrame = None) -> pd.DataFrame:
    out = windows.copy()
    out["incident_type"] = "normal"
    out["incident_id"] = None

    spans = labels[labels["incident_type"] != "wrong_dropoff"][["ride_id", "incident_id", "incident_type", "start_ts", "end_ts"]]
    hits = _overlapping(out, spans)
    out.loc[hits["_row"], "incident_type"] = hits["incident_type"].to_numpy()
    out.loc[hits["_row"], "incident_id"] = hits["incident_id"].to_numpy()

    dropoffs = labels[labels["incident_type"] == "wrong_dropoff"].set_index("ride_id")
    final = out["is_final_window"] & out["ride_id"].isin(dropoffs.index)
    out.loc[final, "incident_type"] = "wrong_dropoff"
    out.loc[final, "incident_id"] = out.loc[final, "ride_id"].map(dropoffs["incident_id"])

    out["is_incident"] = out["incident_type"] != "normal"

    # After an incident ends the car can still look abnormal (a deviated car
    # needs minutes to drive back to its route). Those windows are neither a
    # clean negative nor the incident: excluded from training and false alarms.
    ends = labels.dropna(subset=["end_ts"]).set_index("ride_id")["end_ts"]
    out["after_incident"] = ~out["is_incident"] & (out["window_start"] > out["ride_id"].map(ends))

    out["context"] = "none"
    if behaviors is not None:
        for kind in reversed(HARD_NEGATIVES):                         # highest priority written last
            spans = behaviors[behaviors["kind"] == kind][["ride_id", "start_ts", "end_ts"]]
            out.loc[_overlapping(out, spans)["_row"].unique(), "context"] = kind
    return out


def ride_table(labels: pd.DataFrame, windows: pd.DataFrame, behaviors: pd.DataFrame = None) -> pd.DataFrame:
    """One row per ride: start time, split, incident type, hard negatives it contains."""
    rides = windows.groupby("ride_id").agg(ride_start=("window_start", "min"), n_windows=("window_index", "size"))
    rides["incident_type"] = rides.index.map(labels.set_index("ride_id")["incident_type"]).fillna("normal")
    rides["is_incident"] = rides["incident_type"] != "normal"
    rides["split"] = split_of(rides["ride_start"])
    if behaviors is not None:
        for kind in HARD_NEGATIVES:
            rides[f"has_{kind}"] = rides.index.isin(behaviors.loc[behaviors["kind"] == kind, "ride_id"])
    return rides.reset_index()


def split_of(ts: pd.Series) -> pd.Series:
    """Time-based split: train on the past, validate and test on later days."""
    return pd.Series(
        pd.cut(ts, [pd.Timestamp.min, pd.Timestamp(TRAIN_END), pd.Timestamp(VALID_END), pd.Timestamp.max],
               labels=["train", "valid", "test"], right=False).astype(str),
        index=ts.index,
    )
