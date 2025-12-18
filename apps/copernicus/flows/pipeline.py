from prefect import task, flow
from typing import Dict, Any
import os
import datetime

from copernicus_downloader import download_datasets
from celine.utils.pipelines.pipeline import PipelineConfig, dbt_run, DEV_MODE


script_dir = os.path.dirname(__file__)


# Prefect tasks wrapping functional helpers
@task(name="Download data", retries=3, retry_delay_seconds=60)
def download_data(cfg: PipelineConfig) -> Dict[str, Any]:
    # TODO add openlinege here
    download_datasets(f"{script_dir}/cds_config.yaml")
    return {
        "status": "success",
        "command": "download_datasets",
        "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
    }


# Prefect tasks wrapping functional helpers
@task(name="Extract Data", retries=3, retry_delay_seconds=60)
def import_raw_data(cfg: PipelineConfig) -> Dict[str, Any]:
    return dbt_run("run import", cfg)


# @task(name="Validate Raw Data")
# def validate_raw_data_task(cfg: PipelineConfig) -> Dict[str, Any]:
#     return pt.validate_raw_data(cfg, ["current_weather_stream", "forecast_stream"])


@task(name="Transform Staging Layer")
def transform_staging_layer_task(cfg: PipelineConfig) -> Dict[str, Any]:
    return dbt_run("run staging", cfg)


@task(name="Transform Silver Layer")
def transform_silver_layer_task(cfg: PipelineConfig) -> Dict[str, Any]:
    return dbt_run("run silver", cfg)


@task(name="Transform Gold Layer")
def transform_gold_layer_task(cfg: PipelineConfig) -> Dict[str, Any]:
    return dbt_run("run gold")


@task(name="Run dbt Tests")
def run_dbt_tests_task(cfg: PipelineConfig) -> Dict[str, Any]:
    return dbt_run("test", cfg)


@flow(name="copernicus-flow")
def copernicus_flow(config: Dict[str, Any] | None = None) -> Dict[str, Any]:
    cfg = PipelineConfig.model_validate(config or {})

    downloader = download_data(cfg)
    importer = import_raw_data(cfg)
    staging = transform_staging_layer_task(cfg)
    silver = transform_silver_layer_task(cfg)
    gold = transform_gold_layer_task(cfg)
    tests = run_dbt_tests_task(cfg)

    return {
        "status": "success",
        "downloader": downloader,
        "importer": importer,
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
