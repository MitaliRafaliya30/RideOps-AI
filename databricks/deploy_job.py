"""
Deploy the RideOps safety job from its definition in the repo (job as code).

  python databricks/deploy_job.py            create the job, or update it if it exists (by name)
  python databricks/deploy_job.py --run      ... and start a run now, wait, print each task's result

The definition (databricks/jobs/rideops_safety_job.json) carries placeholders
filled in here, so the repo holds no workspace-specific ids:
  ${USER}         the Databricks user (failure e-mails, notebook folder)
  ${PIPELINE_ID}  looked up by pipeline name
  ${JOBS_DIR}     /Users/<user>/jobs, where the job notebooks are imported

Needs DATABRICKS_HOST and DATABRICKS_TOKEN in .env (same as ml/serving.py).
Re-running is safe: the same job is updated in place, never duplicated.
"""

import argparse
import json
import time
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
DEFINITION = ROOT / "databricks" / "jobs" / "rideops_safety_job.json"
PIPELINE_NAME = "RideOps Pipeline"


def render(template: str, values: dict) -> dict:
    """Fill ${NAME} placeholders; fail if any is left unfilled."""
    for key, value in values.items():
        template = template.replace("${" + key + "}", value)
    if "${" in template:
        missing = sorted({part.split("}")[0] for part in template.split("${")[1:]})
        raise ValueError(f"unfilled placeholders: {missing}")
    return json.loads(template)


def deploy(run_now: bool = False):
    load_dotenv(ROOT / ".env")
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.jobs import JobSettings

    w = WorkspaceClient()
    user = w.current_user.me().user_name
    pipelines = [p for p in w.pipelines.list_pipelines() if p.name == PIPELINE_NAME]
    if len(pipelines) != 1:
        raise SystemExit(f"expected one pipeline named {PIPELINE_NAME!r}, found {len(pipelines)}")

    spec = render(DEFINITION.read_text(encoding="utf-8"),
                  {"USER": user, "PIPELINE_ID": pipelines[0].pipeline_id, "JOBS_DIR": f"/Users/{user}/jobs"})
    settings = JobSettings.from_dict(spec)

    existing = [j for j in w.jobs.list(name=spec["name"])]
    if existing:
        job_id = existing[0].job_id
        w.jobs.reset(job_id=job_id, new_settings=settings)
        print(f"updated job {job_id}: {spec['name']}")
    else:
        job_id = w.jobs.create(**{k: getattr(settings, k) for k in settings.as_dict() if getattr(settings, k, None) is not None}).job_id
        print(f"created job {job_id}: {spec['name']}")
    print(f"{w.config.host}/jobs/{job_id}")

    if run_now:
        run = w.jobs.run_now(job_id=job_id).response
        print(f"run started: {w.config.host}/jobs/{job_id}/runs/{run.run_id}")
        while True:
            state = w.jobs.get_run(run.run_id)
            if state.state.life_cycle_state.value in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR"):
                break
            time.sleep(30)
        for task in state.tasks:
            result = task.state.result_state.value if task.state.result_state else task.state.life_cycle_state.value
            seconds = ((task.end_time or 0) - (task.start_time or 0)) / 1000
            print(f"  {task.task_key:20s} {result:10s} {seconds:6.0f} s")
        print("run result:", state.state.result_state.value if state.state.result_state else state.state.life_cycle_state.value)
    return job_id


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--run", action="store_true", help="start a run after deploying and wait for it")
    deploy(parser.parse_args().run)


if __name__ == "__main__":
    main()
