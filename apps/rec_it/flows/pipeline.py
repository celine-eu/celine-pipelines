from prefect import task, flow
from typing import Dict, Any

from celine.utils.pipelines.pipeline import (
    PipelineConfig,
    dbt_run,
    flow_hooks,
    DEV_MODE,
)

_cfg = PipelineConfig()
_on_running, _on_completion, _on_failure = flow_hooks(_cfg)


@task(name="Transform Gold Layer")
def transform_gold_layer_task(cfg: PipelineConfig):
    return dbt_run("tag:rec_it", cfg)


@task(name="Run dbt Tests")
def run_dbt_tests_task(cfg: PipelineConfig):
    return dbt_run("test --select tag:rec_it", cfg)


@flow(
    name="rec-it-flow",
    on_running=[_on_running],
    on_completion=[_on_completion],
    on_failure=[_on_failure],
)
def rec_it_flow(config: Dict[str, Any] | None = None):
    cfg = PipelineConfig.model_validate(config or {})
    return {
        "gold": transform_gold_layer_task(cfg),
        "tests": run_dbt_tests_task(cfg),
    }


if __name__ == "__main__":
    if DEV_MODE:
        rec_it_flow.serve(name="default", cron="*/15 * * * *")
