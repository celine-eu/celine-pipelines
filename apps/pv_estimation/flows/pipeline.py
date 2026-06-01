import logging
import os
import sys
from pathlib import Path
from typing import Any, Dict

from prefect import flow, task

from celine.utils.pipelines.pipeline import (
    DEV_MODE,
    PipelineConfig,
    PipelineTaskResult,
    PipelineStatus,
    dbt_run,
)

_flows_dir = str(Path(__file__).parent)
if _flows_dir not in sys.path:
    sys.path.insert(0, _flows_dir)

from db import get_engine_from_config, ensure_table
from roi_estimator import load_config, run_parallel

logger = logging.getLogger(__name__)

os.environ.setdefault("APP_NAME", "pv_estimation")

script_dir = Path(__file__).parent
app_dir = script_dir.parent
dbt_dir = str(app_dir / "dbt")

os.environ.setdefault("DBT_PROJECT_DIR", dbt_dir)
os.environ.setdefault("DBT_PROFILES_DIR", dbt_dir)


@task(name="Ensure Raw Tables")
def ensure_raw_tables_task(cfg: PipelineConfig):
    est_cfg = load_config()
    engine = get_engine_from_config(cfg)
    pred_cfg = est_cfg["predictions"]
    ensure_table(engine, pred_cfg["schema"], pred_cfg["table"])


@task(name="Estimate ROI", retries=1, retry_delay_seconds=60)
def estimate_roi_task(cfg: PipelineConfig) -> PipelineTaskResult:
    engine = get_engine_from_config(cfg)
    written = run_parallel(engine)

    return PipelineTaskResult(
        status=PipelineStatus.COMPLETED,
        command="estimate_roi",
        details=f"{written} buildings estimated",
    )


@task(name="Transform Staging Layer")
def transform_staging_task(cfg: PipelineConfig) -> PipelineTaskResult:
    return dbt_run("staging", cfg)


@task(name="Transform Silver Layer")
def transform_silver_task(cfg: PipelineConfig) -> PipelineTaskResult:
    return dbt_run("silver", cfg)


@task(name="Transform Gold Layer")
def transform_gold_task(cfg: PipelineConfig) -> PipelineTaskResult:
    return dbt_run("gold", cfg)


@task(name="Run dbt Tests")
def run_dbt_tests_task(cfg: PipelineConfig) -> PipelineTaskResult:
    return dbt_run("test", cfg)


@flow(name="pv-estimation-flow")
async def pv_estimation_flow(config: Dict[str, Any] | None = None):
    cfg = PipelineConfig.model_validate(config or {})

    results = {}
    results["ensure_tables"] = ensure_raw_tables_task(cfg)
    results["estimate"] = estimate_roi_task(cfg)
    results["staging"] = transform_staging_task(cfg)
    results["silver"] = transform_silver_task(cfg)
    results["gold"] = transform_gold_task(cfg)
    results["tests"] = run_dbt_tests_task(cfg)

    return results


if __name__ == "__main__":
    est_cfg = load_config()
    schedule = est_cfg.get("schedule", {})
    if DEV_MODE:
        pv_estimation_flow.serve(
            name=schedule.get("name", "pv-estimation-weekly"),
            cron=schedule.get("cron", "0 4 * * 1"),
        )
