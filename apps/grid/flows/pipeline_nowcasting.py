"""Grid nowcasting pipeline: dbt transforms for observation-based risk overlays.

Schedule: every 15 minutes (starting at minute 20).
"""

import os
from pathlib import Path
from typing import Any, Dict

from prefect import flow, task
from celine.utils.pipelines.pipeline import (
    DEV_MODE,
    PipelineConfig,
    dbt_run,
)

os.environ.setdefault("APP_NAME", "grid")

script_dir = Path(__file__).parent
app_dir = script_dir.parent  # apps/grid/
dbt_dir = str(app_dir / "dbt")

os.environ.setdefault("DBT_PROJECT_DIR", dbt_dir)
os.environ.setdefault("DBT_PROFILES_DIR", dbt_dir)


@task(name="Transform Nowcast Layer")
def transform_nowcast_task(cfg: PipelineConfig):
    return dbt_run("tag:nowcast", cfg)


@task(name="Test nowcast models")
def test_nowcast_task(cfg: PipelineConfig):
    return dbt_run("test -s tag:nowcast", cfg)


@flow(name="grid-nowcasting-flow")
def grid_nowcasting_flow(config: Dict[str, Any] | None = None):
    cfg = PipelineConfig.model_validate(config or {})
    return {
        "nowcast": transform_nowcast_task(cfg),
        "tests": test_nowcast_task(cfg),
    }


if __name__ == "__main__":
    import yaml

    config_path = script_dir / "config_nowcasting.yaml"
    with open(config_path) as fh:
        pipeline_cfg = yaml.safe_load(fh)
    schedule = pipeline_cfg["schedule"]

    if DEV_MODE:
        grid_nowcasting_flow.serve(name=schedule["name"], cron=schedule["cron"])
