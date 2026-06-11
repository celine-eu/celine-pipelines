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
from lib.config import (  # noqa: E402
    get_active_devices,
    load_config,
    write_active_devices_seed,
)

# Private fleet seed: generated at flow start from REC_ACTIVE_DEVICES so dbt can resolve
# ref('rec_active_devices') without the device IDs ever living in git (the CSV is
# git-ignored). Path is the dbt seeds dir of this app.
_SEED_PATH = _APP_DIR / "dbt" / "seeds" / "rec_active_devices.csv"

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

    # Phase 0: materialise the private fleet seed from REC_ACTIVE_DEVICES before any
    # dbt task parses ref('rec_active_devices'). Runs synchronously at flow start so
    # the CSV exists prior to seed/silver task submission.
    n_devices = write_active_devices_seed(get_active_devices(load_config()), _SEED_PATH)
    if n_devices == 0:
        print(
            f"WARNING: {_SEED_PATH.name} written with 0 devices — set "
            "REC_ACTIVE_DEVICES; fleet scope will resolve to empty."
        )

    # Phase 1: seed + silver + Python tasks (baselines, streaks) — independent
    seed = seed_task(cfg)
    silver = transform_silver_layer_task(cfg)
    baselines = compute_baselines_task(cfg)
    streaks = update_streaks_task(cfg)

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
