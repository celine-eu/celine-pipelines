from typing import Dict, Any

from prefect import task, flow
from celine.utils.pipelines.pipeline import (
    PipelineConfig,
    dbt_run,
    meltano_run,
    DEV_MODE,
)


# Prefect tasks wrapping functional helpers
@task(name="Extract OpenWeatherMap Data", retries=3, retry_delay_seconds=60)
def extract_weather_data_task(cfg: PipelineConfig):
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


@flow(name="openweathermap-flow")
async def openweathermap_flow(config: Dict[str, Any] | None = None):
    cfg = PipelineConfig.model_validate(config or {})

    results = {}
    results["extraction"] = extract_weather_data_task(cfg)
    results["staging"] = transform_staging_layer_task(cfg)
    results["silver"] = transform_silver_layer_task(cfg)
    results["gold"] = transform_gold_layer_task(cfg)
    results["tests"] = run_dbt_tests_task(cfg)

    return results


if __name__ == "__main__":
    if DEV_MODE:
        openweathermap_flow.serve(
            name="openweathermap-hourly",
            cron="2 * * * *",
        )
