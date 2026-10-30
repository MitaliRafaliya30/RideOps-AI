"""
Tests for the AI assistant that need no LLM and no Databricks:
the SQL guardrails and the rider-safety domain tools (with a fake database).
"""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "assistant"))

from src.guardrails import GuardrailViolation, validate_query  # noqa: E402
from src import safety_tools  # noqa: E402

RIDE = "01bd0097-94a7-4c34-ae3e-8d22ad4bdb52"


# ---------------- guardrails ----------------

@pytest.mark.parametrize("sql", [
    "DELETE FROM fact_ride",
    "SELECT 1; DROP TABLE fact_ride",
    "WITH x AS (SELECT 1) INSERT INTO fact_ride SELECT * FROM x",
    "SELECT * FROM rideops_ai.bronze.gps_messages_raw",
    "SELECT * FROM silver.sv_safety_labels",
    "SELECT * FROM rideops_ai.ml.something_else",
])
def test_unsafe_queries_are_rejected(sql):
    with pytest.raises(GuardrailViolation):
        validate_query(sql)


def test_safety_tables_are_allowed_and_a_limit_is_added():
    sql = validate_query("SELECT alert_type, count(*) FROM rideops_ai.gold.fact_safety_alert GROUP BY alert_type")
    assert sql.endswith("LIMIT 1000")
    assert validate_query("SELECT * FROM fact_ride_safety LIMIT 5").endswith("LIMIT 5")


def test_comments_cannot_hide_a_write():
    with pytest.raises(GuardrailViolation):
        validate_query("SELECT 1 /* harmless */; -- \n UPDATE fact_ride SET total_fare = 0")


# ---------------- domain tools ----------------

class FakeDb:
    """Answers the tools' queries from canned rows, and records the parameters they bound."""

    def __init__(self, tables):
        self.tables, self.calls = tables, []

    def __call__(self, sql, params):
        self.calls.append((sql, params))
        for marker, rows in self.tables.items():
            if marker in sql:
                return rows
        return []


@pytest.mark.parametrize("bad", ["x' OR 1=1 --", "12345", "", "01bd0097; DROP TABLE x"])
def test_explain_ride_rejects_anything_but_a_ride_id(bad):
    with pytest.raises(safety_tools.ToolInputError):
        safety_tools.explain_ride(bad, FakeDb({}))


@pytest.mark.parametrize("bad", ["yesterday'; --", "2026-13-45", "10/05/2026"])
def test_briefing_rejects_bad_dates(bad):
    with pytest.raises(safety_tools.ToolInputError):
        safety_tools.safety_briefing(bad, FakeDb({}))


def test_explain_ride_tells_the_story_with_bound_parameters():
    db = FakeDb({
        "FROM fact_ride_safety r LEFT JOIN": [dict(
            ride_id=RIDE, source="run_x", data_role="training data (optimistic)", trip_start="2026-05-10 01:23",
            minutes_tracked=22, max_risk_score=1.0, alert_count=1, first_alert_type="unexpected_stop",
            minutes_to_first_alert=13.0, true_incident_type="possible_crash", outcome="caught",
            detection_delay_s=327.0, zone_name="DUMBO", borough="Brooklyn")],
        "FROM fact_safety_alert a": [dict(
            alert_type="unexpected_stop", alert_name="Unexpected stop", severity="high", alert_source="model",
            detected_at="2026-05-10 01:36", detected_window_index=12, risk_score=0.998, max_risk_score=1.0,
            alert_windows=9, reasons="stopped for 310 s", recommended_action="Call the rider")],
        "FROM safety_window_scores": [dict(window_index=i, risk_score=r, alert=r > 0.9, alert_type=None, reasons=None)
                                      for i, r in enumerate([0.01, 0.4, 0.99])],
    })
    text = safety_tools.explain_ride(RIDE.upper(), db)
    assert "Unexpected stop (high severity, by model) at minute 12" in text
    assert "Why: stopped for 310 s" in text and "Action: Call the rider" in text
    assert "m2:0.99" in text and "incident = possible_crash; outcome = caught, detected 327 s" in text
    assert all(params == {"rid": RIDE} for _, params in db.calls)          # lower-cased, bound, never pasted
    assert all(RIDE not in sql for sql, _ in db.calls)


def test_explain_ride_live_ride_has_no_ground_truth_and_unknown_ride():
    live = FakeDb({"FROM fact_ride_safety r LEFT JOIN": [dict(
        ride_id=RIDE, source="live", data_role="live", trip_start="t", minutes_tracked=3, max_risk_score=0.02,
        alert_count=0, first_alert_type=None, minutes_to_first_alert=None, true_incident_type=None, outcome=None,
        detection_delay_s=None, zone_name="SoHo", borough="Manhattan")]})
    text = safety_tools.explain_ride(RIDE, live)
    assert "No alert fired" in text and "unknown whether the alert was right" in text
    assert "No tracked ride" in safety_tools.explain_ride(RIDE, FakeDb({}))


def test_briefing_reports_rates_quality_and_resolves_latest():
    db = FakeDb({
        "max(date_key)": [{"d": 20260508}],
        "count(*) AS rides": [dict(rides=7349, alerted=139, min_to_alert=14.2, caught=137, incidents=146,
                                   false_alarms=2, truth_rides=7349)],
        "GROUP BY t.alert_name": [dict(alert_name="Route deviation", severity="medium", n=48)],
        "GROUP BY z.zone_name": [dict(zone_name="Upper East Side North", borough="Manhattan", n=3)],
        "FROM dq_check_results": [dict(status="pass", n=12), dict(status="warn", n=1), dict(status="skipped", n=5)],
    })
    text = safety_tools.safety_briefing("latest", db)
    assert text.startswith("Safety briefing for 2026-05-08: 7,349 rides tracked, 139 alerted (18.9 per 1,000)")
    assert "137 of 146 incidents caught (recall 93.8%), 2 false-alarm rides" in text
    assert "12 pass, 1 warn, 0 fail, 5 skipped" in text
    assert any(params == {"k": 20260508} for _, params in db.calls)


def test_briefing_on_a_quiet_live_day():
    db = FakeDb({"count(*) AS rides": [dict(rides=20, alerted=0, min_to_alert=None, caught=0, incidents=0,
                                             false_alarms=0, truth_rides=0)]})
    text = safety_tools.safety_briefing("2026-10-28", db)
    assert text == "Safety briefing for 2026-10-28: 20 rides tracked, no alerts."
