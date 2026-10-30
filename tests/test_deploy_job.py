"""
Tests for the job definition (databricks/jobs/rideops_safety_job.json) and
its deploy script, without touching Databricks.
"""

import importlib.util
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("deploy_job", ROOT / "databricks" / "deploy_job.py")
deploy_job = importlib.util.module_from_spec(spec)
spec.loader.exec_module(deploy_job)

VALUES = {"USER": "someone@example.com", "PIPELINE_ID": "pipe-123", "JOBS_DIR": "/Users/someone@example.com/jobs"}


@pytest.fixture(scope="module")
def job():
    return deploy_job.render(deploy_job.DEFINITION.read_text(encoding="utf-8"), VALUES)


def test_tasks_run_as_one_chain_in_the_right_order(job):
    keys = [t["task_key"] for t in job["tasks"]]
    assert keys == ["refresh_pipeline", "score_safety", "build_safety_gold", "run_dq_checks"]
    for previous, task in zip(job["tasks"], job["tasks"][1:]):
        assert task["depends_on"] == [{"task_key": previous["task_key"]}]


def test_pipeline_is_never_fully_refreshed(job):
    # A full refresh would wipe the Bronze streaming tables (their source data has expired)
    assert job["tasks"][0]["pipeline_task"] == {"pipeline_id": "pipe-123", "full_refresh": False}


def test_quality_checks_run_even_after_a_failure_and_failures_email(job):
    assert job["tasks"][-1]["run_if"] == "ALL_DONE"
    assert job["email_notifications"]["on_failure"] == ["someone@example.com"]
    assert all(t.get("max_retries", 0) >= 1 for t in job["tasks"])
    assert job["max_concurrent_runs"] == 1


def test_schedule_is_paused_until_turned_on(job):
    assert job["schedule"]["pause_status"] == "PAUSED"


def test_every_notebook_task_exists_in_the_repo(job):
    for task in job["tasks"]:
        if "notebook_task" in task:
            name = task["notebook_task"]["notebook_path"].rsplit("/", 1)[1]
            assert (ROOT / "databricks" / "jobs" / f"{name}.py").exists(), name


def test_definition_is_accepted_by_the_databricks_sdk(job):
    from databricks.sdk.service.jobs import JobSettings
    settings = JobSettings.from_dict(job)
    assert settings.as_dict()["tasks"][3]["run_if"] == "ALL_DONE"


def test_unfilled_placeholder_is_an_error():
    with pytest.raises(ValueError, match="PIPELINE_ID"):
        deploy_job.render('{"id": "${PIPELINE_ID}"}', {})
