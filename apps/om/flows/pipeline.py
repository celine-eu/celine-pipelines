"""
Open-Meteo weather pipeline: Meltano extraction + dbt transforms.

Extracts hourly weather data via tap-openmeteo (Meltano),
loads into Postgres raw schema, then runs dbt staging -> silver -> gold.

Schedule: daily at 06:00 (new forecast available ~05:00 UTC).
"""

import logging
import os
from pathlib import Path
from typing import Any, Dict

import pandas as pd
import sqlalchemy as sa
import yaml
from prefect import flow, task
from prefect.logging import get_run_logger

from celine.utils.pipelines.pipeline import (
    DEV_MODE,
    PipelineConfig,
    PipelineTaskResult,
    dbt_run,
    dbt_run_operation,
    meltano_run_import,
)

logger = logging.getLogger(__name__)

# Ensure APP_NAME and dbt paths are set for PipelineRunner
# In Docker these come from Dockerfile ENV; for local dev we derive from script location
os.environ.setdefault("APP_NAME", "om")

script_dir = Path(__file__).parent
app_dir = script_dir.parent  # apps/om/
dbt_dir = str(app_dir / "dbt")
meltano_dir = str(app_dir / "meltano")

os.environ.setdefault("DBT_PROJECT_DIR", dbt_dir)
os.environ.setdefault("DBT_PROFILES_DIR", dbt_dir)
os.environ.setdefault("MELTANO_PROJECT_ROOT", meltano_dir)


# ---------------------------------------------------------------------------
# Config loader
# ---------------------------------------------------------------------------

def _load_config() -> Dict[str, Any]:
    """Load pipeline configuration from config.yaml."""
    config_path = script_dir / "config.yaml"
    with open(config_path) as fh:
        return yaml.safe_load(fh)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_pg_engine(cfg: PipelineConfig) -> sa.Engine:
    """Build SQLAlchemy engine from PipelineConfig."""
    return sa.create_engine(
        f"postgresql://{cfg.postgres_user}:{cfg.postgres_password}"
        f"@{cfg.postgres_host}:{cfg.postgres_port}/{cfg.postgres_db}"
    )


def _load_to_postgres(
    df: pd.DataFrame,
    cfg: PipelineConfig,
    table_name: str,
    schema: str,
    if_exists: str = "append",
) -> int:
    """Write weather DataFrame into Postgres.

    Args:
        df: DataFrame to write.
        cfg: Pipeline configuration.
        table_name: Target table name.
        schema: Target schema name.
        if_exists: How to behave if the table exists ('append' or 'replace').
    """
    df = df.copy()
    df["_sdc_extracted_at"] = pd.Timestamp.now()

    engine = _get_pg_engine(cfg)

    with engine.begin() as conn:
        conn.execute(sa.text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))
        df.to_sql(
            table_name, conn, schema=schema, if_exists=if_exists, index=False,
        )

    return len(df)


# ---------------------------------------------------------------------------
# Prefect tasks
# ---------------------------------------------------------------------------

@task(name="Import raw data", retries=3, retry_delay_seconds=60)
def import_raw_data(cfg: PipelineConfig) -> PipelineTaskResult:
    """Extract weather data via tap-openmeteo and load into Postgres raw."""
    return meltano_run_import(cfg)


@task(name="Cleanup old data", retries=3, retry_delay_seconds=60)
def cleanup_old_data(cfg: PipelineConfig) -> PipelineTaskResult:
    """Delete old raw records beyond retention period."""
    return dbt_run_operation("cleanup_om_weather", {}, cfg)


@task(name="Compute Gold Features")
def compute_gold_features_task(cfg: PipelineConfig) -> PipelineTaskResult:
    """Read silver weather data, compute 29 ML features, write to gold table."""
    from .features import build_gold_features

    run_logger = get_run_logger()
    om_cfg = _load_config()

    silver = om_cfg["silver"]
    gold_raw = om_cfg["gold_raw"]
    engine = _get_pg_engine(cfg)

    run_logger.info(
        "Reading silver data from %s.%s", silver["schema"], silver["table"],
    )
    silver_df = pd.read_sql_table(
        silver["table"], engine, schema=silver["schema"],
    )

    if silver_df.empty:
        run_logger.warning("Silver table is empty, skipping gold features")
        return PipelineTaskResult(status="skipped", command="compute_gold_features")

    run_logger.info("Computing gold features for %d rows", len(silver_df))
    gold_df = build_gold_features(silver_df, impute_missing=True)

    rows = _load_to_postgres(
        gold_df, cfg, gold_raw["table"], gold_raw["schema"], if_exists="replace",
    )
    run_logger.info(
        "Loaded %d gold feature rows into %s.%s",
        rows, gold_raw["schema"], gold_raw["table"],
    )

    return PipelineTaskResult(status="success", command="compute_gold_features")


@task(name="Compute Gold Features Meters")
def compute_gold_features_meters_task(cfg: PipelineConfig) -> PipelineTaskResult:
    """Read silver weather data, compute 15 meters/PV features, write to gold table."""
    from .features import build_gold_features_meters

    run_logger = get_run_logger()
    om_cfg = _load_config()

    silver = om_cfg["silver"]
    gold_raw_meters = om_cfg["gold_raw_meters"]
    engine = _get_pg_engine(cfg)

    run_logger.info(
        "Reading silver data from %s.%s for meters features",
        silver["schema"], silver["table"],
    )
    silver_df = pd.read_sql_table(
        silver["table"], engine, schema=silver["schema"],
    )

    if silver_df.empty:
        run_logger.warning("Silver table is empty, skipping meters gold features")
        return PipelineTaskResult(status="skipped", command="compute_gold_features_meters")

    run_logger.info("Computing meters gold features for %d rows", len(silver_df))
    gold_df = build_gold_features_meters(silver_df, impute_missing=True)

    rows = _load_to_postgres(
        gold_df, cfg, gold_raw_meters["table"], gold_raw_meters["schema"],
        if_exists="replace",
    )
    run_logger.info(
        "Loaded %d meters gold feature rows into %s.%s",
        rows, gold_raw_meters["schema"], gold_raw_meters["table"],
    )

    return PipelineTaskResult(status="success", command="compute_gold_features_meters")


@task(name="Transform Staging Layer")
def transform_staging_task(cfg: PipelineConfig) -> PipelineTaskResult:
    """Run dbt staging for weather models."""
    return dbt_run("staging", cfg)


@task(name="Transform Silver Layer")
def transform_silver_task(cfg: PipelineConfig) -> PipelineTaskResult:
    """Run dbt silver for weather models."""
    return dbt_run("silver", cfg)


@task(name="Transform Gold Layer")
def transform_gold_task(cfg: PipelineConfig) -> PipelineTaskResult:
    """Run dbt gold for weather feature models."""
    return dbt_run("gold", cfg)


@task(name="Run dbt Tests")
def run_dbt_tests_task(cfg: PipelineConfig) -> PipelineTaskResult:
    """Run dbt tests for weather models."""
    return dbt_run("test", cfg)


# ---------------------------------------------------------------------------
# Flows
# ---------------------------------------------------------------------------

@flow(name="om-flow")
def om_flow(config: Dict[str, Any] | None = None) -> dict:
    """Full Open-Meteo weather pipeline: import -> cleanup -> dbt transforms -> tests."""
    cfg = PipelineConfig.model_validate(config or {})

    result: dict = {"status": "success"}

    # --- Extract + Load (via Meltano tap-openmeteo) ---
    result["import"] = import_raw_data(cfg)

    # --- Cleanup old raw records ---
    result["cleanup"] = cleanup_old_data(cfg)

    # --- Transform (staging + silver) ---
    result["staging"] = transform_staging_task(cfg)
    result["silver"] = transform_silver_task(cfg)

    # --- Gold features (Python compute + dbt model) ---
    result["gold_compute"] = compute_gold_features_task(cfg)
    result["gold_meters_compute"] = compute_gold_features_meters_task(cfg)
    result["gold_transform"] = transform_gold_task(cfg)

    # --- Tests (covers all layers) ---
    result["tests"] = run_dbt_tests_task(cfg)

    return result


if __name__ == "__main__":
    om_cfg = _load_config()
    schedule = om_cfg["schedule"]

    if DEV_MODE:
        om_flow.serve(name=schedule["name"], cron=schedule["cron"])
