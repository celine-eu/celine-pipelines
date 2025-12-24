from prefect import task, flow
from typing import Dict, Any
import os
import datetime

from copernicus_downloader import download_datasets
from celine.utils.pipelines.pipeline import (
    PipelineConfig,
    PipelineTaskResult,
    meltano_run_import,
    dbt_run,
    dbt_run_operation,
    DEV_MODE,
)


script_dir = os.path.dirname(__file__)


# Prefect tasks wrapping functional helpers
@task(name="Download data", retries=3, retry_delay_seconds=60)
def download_data(cfg: PipelineConfig):
    # TODO add openlinege here
    download_datasets(f"{script_dir}/cds_config.yaml")
    return PipelineTaskResult(
        status="success",
        command="download_datasets",
    )


# Prefect tasks wrapping functional helpers
@task(name="Extract Data", retries=3, retry_delay_seconds=60)
def import_raw_data(cfg: PipelineConfig):
    return meltano_run_import(cfg)


# Prefect tasks wrapping functional helpers
@task(name="Cleanup old forecasts", retries=3, retry_delay_seconds=60)
def cleanup_old_forecast(cfg: PipelineConfig):
    return dbt_run_operation("cleanup_raw_era5_fc", {}, cfg)


# @task(name="Validate Raw Data")
# def validate_raw_data_task(cfg: PipelineConfig):
#     return pt.validate_raw_data(cfg, ["current_weather_stream", "forecast_stream"])


@task(name="Transform Staging Layer")
def transform_staging_layer_task(cfg: PipelineConfig):
    return dbt_run("run staging", cfg)


@task(name="Transform Silver Layer")
def transform_silver_layer_task(cfg: PipelineConfig):
    return dbt_run("run silver", cfg)


@task(name="Transform Gold Layer")
def transform_gold_layer_task(cfg: PipelineConfig):
    return dbt_run("run gold")


@task(name="Run dbt Tests")
def run_dbt_tests_task(cfg: PipelineConfig):
    return dbt_run("test", cfg)


@flow(name="copernicus-flow")
def copernicus_flow(config: Dict[str, Any] | None = None):
    cfg = PipelineConfig.model_validate(config or {})

    downloader = download_data(cfg)
    importer = import_raw_data(cfg)
    _cleanup_old_forecast = cleanup_old_forecast(cfg)
    staging = transform_staging_layer_task(cfg)
    silver = transform_silver_layer_task(cfg)
    gold = transform_gold_layer_task(cfg)
    tests = run_dbt_tests_task(cfg)

    return {
        "status": "success",
        "downloader": downloader,
        "importer": importer,
        "cleanup_old_forecast": _cleanup_old_forecast,
        "staging": staging,
        "silver": silver,
        "gold": gold,
        "tests": tests,
    }


if __name__ == "__main__":
    if DEV_MODE:
        copernicus_flow.serve(
            name="copernicus-daily",
            cron="0 0,6,12,18 * * *",
        )
