"""Unified weather pipeline: dbt transforms for forecast/alert/current aggregation.

Reads contract-compliant intermediary tables produced by provider pipelines
(mt, owm, om) and writes unified gold tables consumed by the digital-twin
and celine-webapp.

No extraction step — all upstream data is already in the warehouse.

Schedule: hourly (after provider pipelines complete).
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
    dbt_run_operation,
    dbt_seed,
)

os.environ.setdefault("APP_NAME", "weather")

script_dir = Path(__file__).parent
app_dir = script_dir.parent
dbt_dir = str(app_dir / "dbt")

os.environ.setdefault("DBT_PROJECT_DIR", dbt_dir)
os.environ.setdefault("DBT_PROFILES_DIR", dbt_dir)


@task(name="Seed locations")
def seed_task(cfg: PipelineConfig) -> PipelineTaskResult:
    """Refresh the weather_locations seed table."""
    return dbt_seed(cfg)


@task(name="Transform Staging Layer")
def transform_staging_task(cfg: PipelineConfig) -> PipelineTaskResult:
    """Create staging views that UNION provider contract tables."""
    return dbt_run("staging", cfg)


@task(name="Transform Gold Layer")
def transform_gold_task(cfg: PipelineConfig) -> PipelineTaskResult:
    """Run all weather gold models."""
    return dbt_run("gold", cfg)


@task(name="Cleanup old forecasts")
def cleanup_task(cfg: PipelineConfig):
    """Delete forecasts older than 30 days."""
    return dbt_run_operation("cleanup_weather_forecasts", {}, cfg)


@flow(name="weather-flow")
def weather_flow(config: Dict[str, Any] | None = None) -> dict:
    """Unified weather pipeline: aggregate provider forecasts into stable gold tables.

    Reads from provider contract tables and writes:
      - weather_forecast_hourly  (sub-daily forecasts, all locations)
      - weather_forecast_daily   (daily summaries, all locations)
      - weather_alerts_active    (active alerts, all locations)
      - weather_current          (current obs, nearest station per location)
    """
    cfg = PipelineConfig.model_validate(config or {})

    result: dict = {"status": "success"}

    result["seed"] = seed_task(cfg)
    result["staging"] = transform_staging_task(cfg)
    result["gold"] = transform_gold_task(cfg)
    result["cleanup"] = cleanup_task(cfg)

    return result


if __name__ == "__main__":
    import yaml

    config_path = script_dir / "config.yaml"
    with open(config_path) as fh:
        pipeline_cfg = yaml.safe_load(fh)
    schedule = pipeline_cfg["schedule"]

    if DEV_MODE:
        weather_flow.serve(name=schedule["name"], cron=schedule["cron"])
