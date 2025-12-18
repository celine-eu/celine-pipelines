from prefect import task, flow
from typing import Dict, Any
import os
import traceback
from celine.utils.pipelines.pipeline import (
    PipelineConfig,
    PipelineRunner,
    dbt_run,
    meltano_run,
    DEV_MODE,
)
from osm_downloader import osm_download

script_dir = os.path.dirname(__file__)


# Prefect tasks wrapping functional helpers
@task(name="Download data", retries=3, retry_delay_seconds=60)
def download_data(cfg: PipelineConfig):
    pt = PipelineRunner(cfg)

    status = True
    details = None
    try:
        osm_download(f"{script_dir}/osm_config.yaml")
    except Exception as e:
        status = False
        details = f"osm_download error: {e} {traceback.format_exc()}"

    return pt._task_result(status=status, command="osm_download", details=details)


# Prefect tasks wrapping functional helpers
@task(name="Extract Data", retries=3, retry_delay_seconds=60)
def import_raw_data(cfg: PipelineConfig):
    return meltano_run("run import", cfg)


@task(name="Transform Staging Layer")
def transform_staging_layer_task(cfg: PipelineConfig):
    return dbt_run("staging", cfg)


@task(name="Transform Silver Layer")
def transform_silver_layer_task(cfg: PipelineConfig):
    return dbt_run("silver", cfg)


@task(name="Transform Gold Layer")
def transform_gold_layer_task(cfg: PipelineConfig):
    return dbt_run("gold", cfg)


@task(name="Run dbt Tests")
def run_dbt_tests_task(cfg: PipelineConfig):
    return dbt_run("test", cfg)


@flow(name="osm-flow")
def osm_flow(config: Dict[str, Any] | None = None):
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
        osm_flow.serve(
            name="osm-daily",
            cron="0 0,6,12,18 * * *",
        )
