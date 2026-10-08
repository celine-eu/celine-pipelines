import sys
from pathlib import Path
from typing import Dict, Any

from prefect import task, flow

from celine.utils.pipelines.pipeline import (
    PipelineConfig,
    dbt_run,
    dbt_seed,
    flow_hooks,
    DEV_MODE,
)

_APP_DIR = Path(__file__).resolve().parent.parent
if str(_APP_DIR) not in sys.path:
    sys.path.insert(0, str(_APP_DIR))

from flows.auto_commit_task import auto_commit_task  # noqa: E402
from flows.baseline_task import compute_baselines_task  # noqa: E402
from flows.streak_task import update_streaks_task  # noqa: E402

# The fleet is the registry's membership (rec_registry's rec_device_membership): it is
# read by the rec_meters_15m view, so no device list is generated or configured here.

_cfg = PipelineConfig()
_on_running, _on_completion, _on_failure = flow_hooks(_cfg)


@task(name="Seed dbt")
def seed_task(cfg: PipelineConfig):
    return dbt_seed(cfg)


@task(name="Transform Silver Layer")
def transform_silver_layer_task(cfg: PipelineConfig):
    return dbt_run("silver", cfg)


@task(name="Transform Gold Layer")
def transform_gold_layer_task(cfg: PipelineConfig):
    return dbt_run("gold", cfg)


@task(name="Run dbt Tests")
def run_dbt_tests_task(cfg: PipelineConfig):
    return dbt_run("test", cfg)


@flow(
    name="rec-flexibility-flow",
    on_running=[_on_running],
    on_completion=[_on_completion],
    on_failure=[_on_failure],
)
def rec_flexibility_flow(config: Dict[str, Any] | None = None):
    cfg = PipelineConfig.model_validate(config or {})

    # Phase 1: seed + silver. The Python tasks (baselines, streaks) read the silver
    # view rec_meters_15m, so they wait for it: after a model change (a column the
    # view gained, such as community_id) the old view must be replaced first.
    seed = seed_task(cfg)
    silver = transform_silver_layer_task(cfg)
    baselines = compute_baselines_task(cfg, wait_for=[silver])
    streaks = update_streaks_task(cfg, wait_for=[silver])

    # Phase 2: gold models depend on silver + baselines + streaks
    gold = transform_gold_layer_task(cfg, wait_for=[seed, silver, baselines, streaks])

    # Phase 3: auto-commit needs windows from Phase 2
    auto_commit = auto_commit_task(cfg, wait_for=[gold])

    # Phase 4: re-run dbt to pick up new commitments in bonus/participant models
    gold_final = transform_gold_layer_task.with_options(
        name="Transform Gold Layer (with commitments)"
    )(cfg, wait_for=[auto_commit])

    # Phase 5: tests
    tests = run_dbt_tests_task(cfg, wait_for=[gold_final])

    return {
        "seed": seed,
        "silver": silver,
        "baselines": baselines,
        "streaks": streaks,
        "gold": gold,
        "auto_commit": auto_commit,
        "gold_final": gold_final,
        "tests": tests,
    }


if __name__ == "__main__":
    if DEV_MODE:
        rec_flexibility_flow.serve(name="default", cron="0 6 * * *")
