"""Grid resilience pipeline: dbt transforms for wind/heat risk overlays.

Reads from CIM-normalized silver tables (silver_grid_ac_line_segment,
silver_grid_substation) produced by the grid topology ingestion pipeline and from
Open-Meteo gold tables produced by the om pipeline.

No extraction step — all upstream data is already in the warehouse.

Schedule: daily (once per day, after om wind/heat/soil pipelines complete).
"""

import os
from pathlib import Path
from typing import Any, Dict

from prefect import flow, task
from prefect.logging import get_run_logger

from celine.utils.pipelines.pipeline import (
    DEV_MODE,
    PipelineConfig,
    PipelineTaskResult,
    dbt_run,
    dbt_run_tests,
    flow_hooks,
)

os.environ.setdefault("APP_NAME", "grid")

script_dir = Path(__file__).parent
app_dir = script_dir.parent  # apps/grid/
dbt_dir = str(app_dir / "dbt")

os.environ.setdefault("DBT_PROJECT_DIR", dbt_dir)
os.environ.setdefault("DBT_PROFILES_DIR", dbt_dir)

# MQTT run events on celine/pipelines/runs/<namespace> — celine-grid listens for the
# completed event of this flow to evaluate the DSO alert rules (see celine-grid
# docs/specifications/alert-dispatch.md).
_cfg = PipelineConfig()
_on_running, _on_completion, _on_failure = flow_hooks(_cfg)


# ---------------------------------------------------------------------------
# Prefect tasks
# ---------------------------------------------------------------------------


@task(name="Transform Gold Layer")
def transform_gold_task(cfg: PipelineConfig) -> PipelineTaskResult:
    """Run all grid gold models."""
    return dbt_run("tag:daily", cfg)


@task(name="Test grid models")
def test_grid_task(cfg: PipelineConfig) -> PipelineTaskResult:
    """Run dbt tests for all grid models."""
    return dbt_run_tests(cfg)


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------


@flow(
    name="grid-resilience-flow",
    on_running=[_on_running],
    on_completion=[_on_completion],
    on_failure=[_on_failure],
)
def grid_resilience_flow(config: Dict[str, Any] | None = None) -> dict:
    """Grid resilience pipeline: compute wind/heat risk overlays.

    Reads CIM-normalized silver tables (produced by the grid topology ingestion pipeline) and
    Open-Meteo weather gold tables (produced by om pipeline), then writes:
      - grid_wind_risks         (overhead segments + wind metrics, today + 2 days — intermediary)
      - grid_heat_status        (soil x air heat status per weather point: intermediary)
      - grid_heat_risks         (underground cables + heat metrics, today + 2 days — intermediary)
      - grid_joint_heat_risks   (cable joints + heat metrics, today + 2 days: intermediary)
      - grid_shapes             (unified CIM asset registry, geometry only: monthly, run by hand)
      - grid_risks              (collapsed WARNING/ALERT risks by asset: incremental)
      - grid_wind_risks_8h / grid_risks_8h (same on 8-hour windows — incremental)
      - grid_risks_trendline    (daily risk ratio per vector — incremental)
      - grid_risk_km            (length-weighted exposure per tratta / unit — incremental)
      - grid_tile_grid / grid_tree_strike_spans / grid_tree_strike_tiles (tree-strike overlay — monthly)
      - v_superset_grid         (Superset view: risks joined with shapes, today + future)
      - grid_substations        (MV/LV substations with geojson — legacy)
      - grid_network_topology   (topology filter values — static)
    """
    cfg = PipelineConfig.model_validate(config or {})

    result: dict = {"status": "success"}

    result["gold"] = transform_gold_task(cfg)
    result["tests"] = test_grid_task(cfg)

    return result


if __name__ == "__main__":
    import yaml

    config_path = script_dir / "config.yaml"
    with open(config_path) as fh:
        pipeline_cfg = yaml.safe_load(fh)
    schedule = pipeline_cfg["schedule"]

    if DEV_MODE:
        grid_resilience_flow.serve(name=schedule["name"], cron=schedule["cron"])
