from prefect import task, flow
from typing import Dict, Any
import os
from celine.utils.pipelines.pipeline import (
    PipelineConfig,
    PipelineStatus,
    PipelineTaskResult,
    pipeline_context,
    dbt_run,
    meltano_run,
    DEV_MODE,
)
from osm_downloader import osm_download

script_dir = os.path.dirname(__file__)


# Prefect tasks wrapping functional helpers
@task(name="Download data", retries=3, retry_delay_seconds=60)
def download_data(cfg: PipelineConfig):

    osm_download(f"{script_dir}/osm_config.yaml")

    return PipelineTaskResult(
        status=PipelineStatus.COMPLETED,
        command="osm_download",
    )


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
async def osm_flow(config: Dict[str, Any] | None = None):
    cfg = PipelineConfig.model_validate(config or {})

    async with pipeline_context(cfg) as results:
        results["downloader"] = download_data(cfg)
        results["importer"] = import_raw_data(cfg)
        results["staging"] = transform_staging_layer_task(cfg)
        results["silver"] = transform_silver_layer_task(cfg)
        results["gold"] = transform_gold_layer_task(cfg)
        results["tests"] = run_dbt_tests_task(cfg)

    return results


if __name__ == "__main__":
    if DEV_MODE:
        osm_flow.serve(
            name="osm-daily",
            cron="0 0,6,12,18 * * *",
        )
