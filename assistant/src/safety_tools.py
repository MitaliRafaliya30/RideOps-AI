"""
Domain tools for rider-safety questions.

Free-form text-to-SQL is flexible but can be subtly wrong. The two questions
a safety team asks all day get fixed, reviewed SQL instead:

  explain_ride(ride_id)    why was this ride flagged? minute-by-minute story
  safety_briefing(date)    what happened on this day? (alerts, zones, model, data quality)

The agent picks a tool when the question fits and falls back to SQL otherwise.
Inputs are validated against strict patterns before they reach SQL, so a
tool argument can never inject SQL. The functions take a `query` callable
(sql, params) -> list of dicts, which keeps them testable without Databricks.
"""

import re
from datetime import date as Date
from typing import Callable, Dict, List

Query = Callable[[str, dict], List[Dict]]

RIDE_ID = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")
ISO_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


class ToolInputError(ValueError):
    pass


def _ride_id(value: str) -> str:
    value = value.strip().lower()
    if not RIDE_ID.match(value):
        raise ToolInputError(f"'{value}' is not a ride id (expected a UUID like 1b0e...-...).")
    return value


def _day(value: str) -> str:
    value = value.strip()
    if value.lower() == "latest":
        return value.lower()
    if not ISO_DATE.match(value):
        raise ToolInputError(f"'{value}' is not a date (expected YYYY-MM-DD or 'latest').")
    try:
        Date.fromisoformat(value)                   # rejects 2026-13-45
    except ValueError as e:
        raise ToolInputError(f"'{value}' is not a real date: {e}.") from None
    return value


def _num(value, digits=1):
    return "n/a" if value is None else f"{float(value):.{digits}f}"


def explain_ride(ride_id: str, query: Query) -> str:
    """Why was this ride flagged (or not)? Ride summary, alerts, and the risk minute by minute."""
    rid = _ride_id(ride_id)
    rides = query("""
        SELECT r.ride_id, r.source, r.data_role, r.trip_start, r.minutes_tracked, r.max_risk_score, r.alert_count,
               r.first_alert_type, r.minutes_to_first_alert, r.true_incident_type, r.outcome, r.detection_delay_s,
               z.zone_name, z.borough
        FROM fact_ride_safety r LEFT JOIN dim_zone z ON r.pickup_zone_key = z.zone_key
        WHERE r.ride_id = :rid""", {"rid": rid})
    if not rides:
        return f"No tracked ride with id {rid}. (Only rides with GPS are in fact_ride_safety.)"
    r = rides[0]
    lines = [f"Ride {rid} ({r['data_role']}), picked up in {r['zone_name']}, {r['borough']} at {r['trip_start']}, "
             f"tracked for {r['minutes_tracked']} min. Highest risk: {_num(r['max_risk_score'], 3)}."]

    alerts = query("""
        SELECT a.alert_type, t.alert_name, t.severity, a.alert_source, a.detected_at, a.detected_window_index,
               a.risk_score, a.max_risk_score, a.alert_windows, a.reasons, t.recommended_action
        FROM fact_safety_alert a JOIN dim_incident_type t ON a.alert_type = t.alert_type
        WHERE a.ride_id = :rid ORDER BY a.detected_at""", {"rid": rid})
    if not alerts:
        lines.append("No alert fired on this ride.")
    for a in alerts:
        lines.append(f"- {a['alert_name']} ({a['severity']} severity, by {a['alert_source']}) at minute "
                     f"{a['detected_window_index']} ({a['detected_at']}), risk {_num(a['risk_score'], 3)}, active "
                     f"{a['alert_windows']} min. Why: {a['reasons']}. Action: {a['recommended_action']}.")

    timeline = query("""
        SELECT window_index, risk_score, alert, alert_type, reasons
        FROM safety_window_scores WHERE ride_id = :rid ORDER BY window_index""", {"rid": rid})
    if timeline:
        risks = ", ".join(f"m{t['window_index']}:{_num(t['risk_score'], 2)}" for t in timeline[:40])
        more = f" ... ({len(timeline)} minutes)" if len(timeline) > 40 else ""
        lines.append(f"Risk by minute: {risks}{more}")

    if r["true_incident_type"] or r["outcome"]:
        truth = r["true_incident_type"] or "none"
        delay = f", detected {_num(r['detection_delay_s'], 0)} s after it started" if r["outcome"] == "caught" else ""
        lines.append(f"Ground truth (simulation): incident = {truth}; outcome = {r['outcome']}{delay}.")
    else:
        lines.append("Live ride: no ground truth, so it is unknown whether the alert was right.")
    return "\n".join(lines)


def safety_briefing(day: str, query: Query) -> str:
    """One-day safety summary: volume, alerts by type, worst zones, model quality, data quality."""
    d = _day(day)
    if d == "latest":
        latest = query("SELECT max(date_key) AS d FROM fact_ride_safety", {})
        if not latest or latest[0]["d"] is None:
            return "No tracked rides yet."
        key = int(latest[0]["d"])
    else:
        key = int(d.replace("-", ""))
    label = f"{str(key)[:4]}-{str(key)[4:6]}-{str(key)[6:]}"

    kpi = query("""
        SELECT count(*) AS rides, sum(CASE WHEN has_alert THEN 1 ELSE 0 END) AS alerted,
               avg(minutes_to_first_alert) AS min_to_alert,
               sum(CASE WHEN outcome = 'caught' THEN 1 ELSE 0 END) AS caught,
               sum(CASE WHEN has_ground_truth AND true_incident_type IS NOT NULL THEN 1 ELSE 0 END) AS incidents,
               sum(CASE WHEN false_alarm THEN 1 ELSE 0 END) AS false_alarms,
               sum(CASE WHEN has_ground_truth THEN 1 ELSE 0 END) AS truth_rides
        FROM fact_ride_safety WHERE date_key = :k""", {"k": key})[0]
    if not kpi["rides"]:
        return f"No tracked rides on {label}."
    head = f"Safety briefing for {label}: {kpi['rides']:,} rides tracked"
    if kpi["alerted"]:
        head += (f", {kpi['alerted']:,} alerted ({1000 * kpi['alerted'] / kpi['rides']:.1f} per 1,000), "
                 f"first alert after {_num(kpi['min_to_alert'])} min on average.")
    else:
        head += ", no alerts."
    lines = [head]

    by_type = query("""
        SELECT t.alert_name, t.severity, count(*) AS n
        FROM fact_safety_alert a JOIN dim_incident_type t ON a.alert_type = t.alert_type
        JOIN fact_ride_safety r ON a.ride_id = r.ride_id
        WHERE r.date_key = :k GROUP BY t.alert_name, t.severity ORDER BY n DESC""", {"k": key})
    if by_type:
        lines.append("Alerts by type: " + "; ".join(f"{t['alert_name']} {t['n']} ({t['severity']})" for t in by_type) + ".")

    zones = query("""
        SELECT z.zone_name, z.borough, count(*) AS n
        FROM fact_ride_safety r JOIN dim_zone z ON r.pickup_zone_key = z.zone_key
        WHERE r.date_key = :k AND r.has_alert GROUP BY z.zone_name, z.borough ORDER BY n DESC LIMIT 3""", {"k": key})
    if zones:
        lines.append("Most alerted pickup zones: " + "; ".join(f"{z['zone_name']} ({z['borough']}) {z['n']}" for z in zones) + ".")

    if kpi["truth_rides"]:
        recall = kpi["caught"] / kpi["incidents"] if kpi["incidents"] else None
        lines.append(f"Model vs ground truth (simulated rides): {kpi['caught']} of {kpi['incidents']} incidents caught"
                     f" (recall {_num(100 * recall if recall is not None else None)}%), {kpi['false_alarms']} false-alarm rides.")

    dq = query("""
        SELECT status, count(*) AS n FROM dq_check_results
        WHERE checked_at = (SELECT max(checked_at) FROM dq_check_results) GROUP BY status""", {})
    if dq:
        counts = {row["status"]: row["n"] for row in dq}
        lines.append("Latest data quality run: " + ", ".join(f"{counts.get(s, 0)} {s}" for s in ("pass", "warn", "fail", "skipped")) + ".")
    return "\n".join(lines)
