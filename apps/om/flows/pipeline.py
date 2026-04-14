"""
Open-Meteo weather pipeline: Meltano extraction + dbt transforms.

Extracts hourly weather data via tap-openmeteo (Meltano),
loads into Postgres raw schema, then runs dbt staging -> silver -> gold.

Schedule: daily at 06:00 (new forecast available ~05:00 UTC).
"""

import logging
import os
import sys
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

# Add flows directory to sys.path so imports work when the module is loaded
# outside a package context (e.g. via celine-utils `exec_module`).
_flows_dir = str(Path(__file__).parent)
if _flows_dir not in sys.path:
    sys.path.insert(0, _flows_dir)

from features import build_gold_features

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


# Hours to re-compute on every incremental run.
# Open-Meteo serves forecast values for future hours; once those hours become
# past they are replaced by ERA5 actuals. Re-processing the last 2 days ensures
# gold features always reflect the most accurate silver values.
_RECOMPUTE_WINDOW_HOURS: int = 48
# Extra lookback added on top of the recompute window so that rolling-window
# features (max window = 24 h) are computed with sufficient history.
_ROLLING_BUFFER_HOURS: int = 24


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


def _upsert_gold_rows(
    df: pd.DataFrame,
    engine: sa.Engine,
    schema: str,
    table: str,
    recompute_from: pd.Timestamp,
    recompute_to: pd.Timestamp,
) -> int:
    """Delete stale gold rows in the recompute window, then re-insert fresh ones.

    Args:
        df: Recomputed gold rows for [recompute_from, recompute_to].
        engine: SQLAlchemy engine.
        schema: Target schema.
        table: Target table name.
        recompute_from: Start of the recompute window (inclusive).
        recompute_to: End of the recompute window (inclusive, = old max_processed).

    Returns:
        Number of rows inserted.
    """
    df = df.copy()
    df["_sdc_extracted_at"] = pd.Timestamp.now()

    with engine.begin() as conn:
        conn.execute(
            sa.text(
                f"DELETE FROM {schema}.{table}"
                f" WHERE datetime >= :start AND datetime <= :end"
            ),
            {"start": recompute_from, "end": recompute_to},
        )
        df.to_sql(table, conn, schema=schema, if_exists="append", index=False)

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


def _get_max_processed_datetime(
    engine: sa.Engine,
    schema: str,
    table: str,
) -> "pd.Timestamp | None":
    """Return the max datetime already in the raw gold staging table, or None."""
    try:
        result = pd.read_sql(
            f"SELECT MAX(datetime) AS max_dt FROM {schema}.{table}",
            engine,
        )
        val = result["max_dt"].iloc[0]
        return val if val is not pd.NaT and val is not None else None
    except Exception:
        return None


@task(name="Compute Gold Features")
def compute_gold_features_task(cfg: PipelineConfig) -> PipelineTaskResult:
    """Read new silver weather rows, compute 29 ML features, upsert to raw gold table.

    On every incremental run the last _RECOMPUTE_WINDOW_HOURS (48 h) of gold rows
    are deleted and recomputed from the latest silver values, so that hours that were
    initially stored as Open-Meteo forecast data are updated once ERA5 actuals arrive.
    Rows beyond max_processed are appended as new.
    """
    run_logger = get_run_logger()
    om_cfg = _load_config()

    silver = om_cfg["silver"]
    gold_raw = om_cfg["gold_raw"]
    engine = _get_pg_engine(cfg)

    max_processed = _get_max_processed_datetime(
        engine, gold_raw["schema"], gold_raw["table"]
    )

    if max_processed is None:
        run_logger.info(
            "First run — reading all silver data from %s.%s",
            silver["schema"], silver["table"],
        )
        silver_df = pd.read_sql_table(silver["table"], engine, schema=silver["schema"])
    else:
        # Lookback = recompute window + rolling-window buffer
        cutoff = max_processed - pd.Timedelta(
            hours=_RECOMPUTE_WINDOW_HOURS + _ROLLING_BUFFER_HOURS
        )
        run_logger.info(
            "Incremental run — reading silver from %s "
            "(max_processed=%s, recompute_window=%dh)",
            cutoff, max_processed, _RECOMPUTE_WINDOW_HOURS,
        )
        silver_df = pd.read_sql(
            f"SELECT * FROM {silver['schema']}.{silver['table']} "
            f"WHERE datetime >= %(cutoff)s",
            engine,
            params={"cutoff": cutoff},
        )

    if silver_df.empty:
        run_logger.warning("Silver table is empty, skipping gold features")
        return PipelineTaskResult(status="skipped", command="compute_gold_features")

    run_logger.info("Computing gold features for %d silver rows", len(silver_df))
    gold_df = build_gold_features(silver_df, impute_missing=True)

    if max_processed is None:
        rows = _load_to_postgres(
            gold_df, cfg, gold_raw["table"], gold_raw["schema"], if_exists="append",
        )
        run_logger.info(
            "Appended %d gold feature rows into %s.%s (first run)",
            rows, gold_raw["schema"], gold_raw["table"],
        )
    else:
        recompute_from = max_processed - pd.Timedelta(hours=_RECOMPUTE_WINDOW_HOURS)

        # Rows in the recompute window: delete stale values, re-insert with fresh silver
        recompute_df = gold_df[
            (gold_df["datetime"] >= recompute_from)
            & (gold_df["datetime"] <= max_processed)
        ]
        # Rows beyond max_processed: plain append
        new_df = gold_df[gold_df["datetime"] > max_processed]

        updated = 0
        if not recompute_df.empty:
            updated = _upsert_gold_rows(
                recompute_df, engine,
                gold_raw["schema"], gold_raw["table"],
                recompute_from, max_processed,
            )
            run_logger.info(
                "Re-inserted %d gold rows for recompute window [%s, %s]",
                updated, recompute_from, max_processed,
            )

        appended = 0
        if not new_df.empty:
            appended = _load_to_postgres(
                new_df, cfg, gold_raw["table"], gold_raw["schema"], if_exists="append",
            )
            run_logger.info(
                "Appended %d new gold feature rows into %s.%s",
                appended, gold_raw["schema"], gold_raw["table"],
            )

        if updated == 0 and appended == 0:
            run_logger.info("No rows to update or append — gold features are up to date")
            return PipelineTaskResult(status="skipped", command="compute_gold_features")

    return PipelineTaskResult(status="success", command="compute_gold_features")


@task(name="Compute Gold Features Meters")
def compute_gold_features_meters_task(cfg: PipelineConfig) -> PipelineTaskResult:
    """Read new silver weather rows, compute 15 meters/PV features, upsert to raw gold table.

    Same recompute-window strategy as compute_gold_features_task: the last
    _RECOMPUTE_WINDOW_HOURS (48 h) of rows are deleted and re-inserted from the
    latest silver so that forecast values are replaced by ERA5 actuals over time.
    """
    from features import build_gold_features_meters

    run_logger = get_run_logger()
    om_cfg = _load_config()

    silver = om_cfg["silver"]
    gold_raw_meters = om_cfg["gold_raw_meters"]
    engine = _get_pg_engine(cfg)

    max_processed = _get_max_processed_datetime(
        engine, gold_raw_meters["schema"], gold_raw_meters["table"]
    )

    if max_processed is None:
        run_logger.info(
            "First run — reading all silver data from %s.%s",
            silver["schema"], silver["table"],
        )
        silver_df = pd.read_sql_table(silver["table"], engine, schema=silver["schema"])
    else:
        cutoff = max_processed - pd.Timedelta(
            hours=_RECOMPUTE_WINDOW_HOURS + _ROLLING_BUFFER_HOURS
        )
        run_logger.info(
            "Incremental run — reading silver from %s "
            "(max_processed=%s, recompute_window=%dh)",
            cutoff, max_processed, _RECOMPUTE_WINDOW_HOURS,
        )
        silver_df = pd.read_sql(
            f"SELECT * FROM {silver['schema']}.{silver['table']} "
            f"WHERE datetime >= %(cutoff)s",
            engine,
            params={"cutoff": cutoff},
        )

    if silver_df.empty:
        run_logger.warning("Silver table is empty, skipping meters gold features")
        return PipelineTaskResult(status="skipped", command="compute_gold_features_meters")

    run_logger.info("Computing meters gold features for %d silver rows", len(silver_df))
    gold_df = build_gold_features_meters(silver_df, impute_missing=True)

    if max_processed is None:
        rows = _load_to_postgres(
            gold_df, cfg, gold_raw_meters["table"], gold_raw_meters["schema"],
            if_exists="append",
        )
        run_logger.info(
            "Appended %d meters gold feature rows into %s.%s (first run)",
            rows, gold_raw_meters["schema"], gold_raw_meters["table"],
        )
    else:
        recompute_from = max_processed - pd.Timedelta(hours=_RECOMPUTE_WINDOW_HOURS)

        recompute_df = gold_df[
            (gold_df["datetime"] >= recompute_from)
            & (gold_df["datetime"] <= max_processed)
        ]
        new_df = gold_df[gold_df["datetime"] > max_processed]

        updated = 0
        if not recompute_df.empty:
            updated = _upsert_gold_rows(
                recompute_df, engine,
                gold_raw_meters["schema"], gold_raw_meters["table"],
                recompute_from, max_processed,
            )
            run_logger.info(
                "Re-inserted %d meters gold rows for recompute window [%s, %s]",
                updated, recompute_from, max_processed,
            )

        appended = 0
        if not new_df.empty:
            appended = _load_to_postgres(
                new_df, cfg, gold_raw_meters["table"], gold_raw_meters["schema"],
                if_exists="append",
            )
            run_logger.info(
                "Appended %d new meters gold feature rows into %s.%s",
                appended, gold_raw_meters["schema"], gold_raw_meters["table"],
            )

        if updated == 0 and appended == 0:
            run_logger.info(
                "No rows to update or append — meters gold features are up to date"
            )
            return PipelineTaskResult(
                status="skipped", command="compute_gold_features_meters"
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
