from prefect import task, flow
from typing import Dict, Any
import os
import datetime
from celine.utils.pipelines.pipeline import (
    PipelineConfig,
    PipelineTaskResult,
    dbt_run,
    dbt_run_operation,
    meltano_run,
    DEV_MODE,
)
from dwd_downloader.api import dwd_downloader

script_dir = os.path.dirname(__file__)


# Prefect tasks wrapping functional helpers
@task(name="Download data", retries=3, retry_delay_seconds=60)
def download_data(cfg: PipelineConfig):
    # pt = PipelineRunner(cfg)

    # status = True
    # details = None
    # try:
    #     dwd_downloader(f"{script_dir}/config.yaml")
    # except Exception as e:
    #     status = False
    #     details = f"dwd_downloader error: {e} {traceback.format_exc()}"

    dwd_downloader(f"{script_dir}/config.yaml")

    return PipelineTaskResult(
        status="success",
        command="dwd_downloader",
    )


# Prefect tasks wrapping functional helpers
@task(name="Extract Data", retries=3, retry_delay_seconds=60)
def import_raw_data(cfg: PipelineConfig):
    return meltano_run("run import", cfg)


@task(name="Transform Staging Layer")
def transform_staging_layer_task(cfg: PipelineConfig):
    return dbt_run("staging", cfg)


@task(name="Clean up tables")
def cleanup_tables(cfg: PipelineConfig):
    return dbt_run_operation("cleanup_icon_d2_models", {}, cfg)


@task(name="Clean up raw")
def cleanup_raw(cfg: PipelineConfig):
    return dbt_run_operation("cleanup_icon_d2_raw", {}, cfg)


@task(name="Transform Silver Layer")
def transform_silver_layer_task(cfg: PipelineConfig):
    return dbt_run("silver", cfg)


@task(name="Transform Gold Layer")
def transform_gold_layer_task(cfg: PipelineConfig):
    return dbt_run("gold", cfg)


@task(name="Run dbt Tests")
def run_dbt_tests_task(cfg: PipelineConfig):
    return dbt_run("test", cfg)


@flow(name="dwd-flow")
def dwd_flow(config: Dict[str, Any] | None = None):
    cfg = PipelineConfig.model_validate(config or {})

    downloader = download_data(cfg)

    #  cleanup old forecasts
    cleanup_raw_data = cleanup_raw(cfg)
    cleanup_tables_data = cleanup_tables(cfg)

    importer = import_raw_data(cfg)
    staging = transform_staging_layer_task(cfg)
    silver = transform_silver_layer_task(cfg)
    gold = transform_gold_layer_task(cfg)
    tests = run_dbt_tests_task(cfg)

    return {
        "status": "success",
        "downloader": downloader,
        "cleanup_raw_data": cleanup_raw_data,
        "cleanup_tables_data": cleanup_tables_data,
        "importer": importer,
        "staging": staging,
        "silver": silver,
        "gold": gold,
        "tests": tests,
    }


if __name__ == "__main__":
    if DEV_MODE:
        dwd_flow.serve(
            name="dwd-daily",
            cron="30 0,4,7,10,13,16,19,22 * * *",
        )
