from prefect import task, flow
from typing import Dict, Any

from celine.utils.pipelines.pipeline import (
    PipelineConfig,
    meltano_run,
    dbt_run,
    flow_hooks,
    DEV_MODE,
)


@task(name="Extract Data", retries=3, retry_delay_seconds=60)
def import_task(cfg: PipelineConfig):
    return meltano_run("run import", cfg)


@task(name="Transform Silver Layer")
def transform_silver_layer_task(cfg: PipelineConfig):
    return dbt_run("silver,tag:batches", cfg)


@task(name="Transform Gold Layer")
def transform_gold_layer_task(cfg: PipelineConfig):
    return dbt_run("gold,tag:batches", cfg)


@task(name="Run dbt Tests")
def run_dbt_tests_task(cfg: PipelineConfig):
    return dbt_run("test --select tag:batches", cfg)


@flow(
    name="rec-it-batches-flow",
)
def rec_it_batches_flow(config: Dict[str, Any] | None = None):
    cfg = PipelineConfig.model_validate(config or {})
    return {
        "import": import_task(cfg),
        "silver": transform_silver_layer_task(cfg),
        "gold": transform_gold_layer_task(cfg),
        "tests": run_dbt_tests_task(cfg),
    }


if __name__ == "__main__":
    if DEV_MODE:
        rec_it_batches_flow.serve(name="daily", cron="0 0 * * *")
