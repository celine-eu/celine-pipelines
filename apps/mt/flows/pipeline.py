"""
MeteoTrentino pipeline: Meltano extraction + dbt transforms.

Extracts meteorological data for Trentino via tap-meteotrentino (Meltano),
loads into Postgres raw schema, then runs dbt staging -> silver -> gold.

Streams ingested:
  - sky_conditions   (static reference)
  - alerts           (full refresh)
  - meteo_stations   (full refresh, parent)
  - station_observations (incremental, 15-min obs)
  - forecast_locations   (full refresh, parent)
  - forecasts_hourly (full refresh, 3-hour intervals)
  - forecasts_daily  (full refresh, daily summaries)

Schedule: hourly at :05 (new observations available continuously).
"""

import logging
import os
from pathlib import Path
from typing import Any, Dict

import yaml
from prefect import flow, task

from celine.utils.pipelines.pipeline import (
    DEV_MODE,
    PipelineConfig,
    PipelineTaskResult,
    dbt_run,
    dbt_run_operation,
    meltano_run_import,
)

logger = logging.getLogger(__name__)

os.environ.setdefault("APP_NAME", "mt")

script_dir = Path(__file__).parent
app_dir = script_dir.parent  # apps/mt/
dbt_dir = str(app_dir / "dbt")
meltano_dir = str(app_dir / "meltano")

os.environ.setdefault("DBT_PROJECT_DIR", dbt_dir)
os.environ.setdefault("DBT_PROFILES_DIR", dbt_dir)
os.environ.setdefault("MELTANO_PROJECT_ROOT", meltano_dir)


# ---------------------------------------------------------------------------
# Config loader
# ---------------------------------------------------------------------------

def _load_config() -> Dict[str, Any]:
    config_path = script_dir / "config.yaml"
    with open(config_path) as fh:
        return yaml.safe_load(fh)


# ---------------------------------------------------------------------------
# Prefect tasks
# ---------------------------------------------------------------------------

@task(name="Import raw data", retries=3, retry_delay_seconds=60)
def import_raw_data(cfg: PipelineConfig) -> PipelineTaskResult:
    """Extract MeteoTrentino data via tap-meteotrentino and load into Postgres raw."""
    return meltano_run_import(cfg)


@task(name="Cleanup old observations", retries=3, retry_delay_seconds=60)
def cleanup_old_data(cfg: PipelineConfig) -> PipelineTaskResult:
    """Delete station_observations rows beyond retention period."""
    return dbt_run_operation("cleanup_mt_observations", {}, cfg)


@task(name="Transform Staging Layer")
def transform_staging_task(cfg: PipelineConfig) -> PipelineTaskResult:
    """Run dbt staging for MeteoTrentino models."""
    return dbt_run("staging", cfg)


@task(name="Transform Silver Layer")
def transform_silver_task(cfg: PipelineConfig) -> PipelineTaskResult:
    """Run dbt silver for MeteoTrentino models."""
    return dbt_run("silver", cfg)


@task(name="Transform Gold Layer")
def transform_gold_task(cfg: PipelineConfig) -> PipelineTaskResult:
    """Run dbt gold for MeteoTrentino models."""
    return dbt_run("gold", cfg)


@task(name="Run dbt Tests")
def run_dbt_tests_task(cfg: PipelineConfig) -> PipelineTaskResult:
    """Run dbt tests for MeteoTrentino models."""
    return dbt_run("test", cfg)


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------

@flow(name="mt-flow")
def mt_flow(config: Dict[str, Any] | None = None) -> dict:
    """Full MeteoTrentino pipeline: import -> cleanup -> dbt transforms -> tests."""
    cfg = PipelineConfig.model_validate(config or {})

    result: dict = {"status": "success"}

    result["import"]   = import_raw_data(cfg)
    result["cleanup"]  = cleanup_old_data(cfg)
    result["staging"]  = transform_staging_task(cfg)
    result["silver"]   = transform_silver_task(cfg)
    result["gold"]     = transform_gold_task(cfg)
    result["tests"]    = run_dbt_tests_task(cfg)

    return result


if __name__ == "__main__":
    mt_cfg = _load_config()
    schedule = mt_cfg["schedule"]

    if DEV_MODE:
        mt_flow.serve(name=schedule["name"], cron=schedule["cron"])
