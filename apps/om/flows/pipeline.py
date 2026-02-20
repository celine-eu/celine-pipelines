"""
Open-Meteo weather pipeline: forecast ingestion + dbt transforms.

Fetches hourly weather data from the Open-Meteo API,
loads into Postgres raw schema, then runs dbt staging -> silver.

Schedule: daily at 06:00 (new forecast available ~05:00 UTC).
"""

import logging
import os
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict

import pandas as pd
import requests
import sqlalchemy as sa
import yaml
from prefect import flow, task
from prefect.logging import get_run_logger

from celine.utils.pipelines.pipeline import (
    DEV_MODE,
    PipelineConfig,
    PipelineTaskResult,
    dbt_run,
)

logger = logging.getLogger(__name__)

# Ensure APP_NAME and dbt paths are set for PipelineRunner
# In Docker these come from Dockerfile ENV; for local dev we derive from script location
os.environ.setdefault("APP_NAME", "om")

script_dir = Path(__file__).parent
app_dir = script_dir.parent  # apps/om/
dbt_dir = str(app_dir / "dbt")

os.environ.setdefault("DBT_PROJECT_DIR", dbt_dir)
os.environ.setdefault("DBT_PROFILES_DIR", dbt_dir)


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
) -> int:
    """Append weather DataFrame into Postgres. Pure insert, no cleanup logic."""
    df = df.copy()
    df["_sdc_extracted_at"] = pd.Timestamp.now()

    engine = _get_pg_engine(cfg)

    with engine.begin() as conn:
        conn.execute(sa.text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))
        df.to_sql(
            table_name, conn, schema=schema, if_exists="append", index=False,
        )

    return len(df)


def _drop_om_table(
    cfg: PipelineConfig,
    table_name: str,
    schema: str,
) -> None:
    """Drop a specific OM table. Safe no-op if the table does not exist."""
    engine = _get_pg_engine(cfg)
    with engine.begin() as conn:
        conn.execute(sa.text(
            f"DROP TABLE IF EXISTS {schema}.{table_name} CASCADE"
        ))


# ---------------------------------------------------------------------------
# Prefect tasks
# ---------------------------------------------------------------------------

@task(name="Clean up OM tables")
def cleanup_om_tables(cfg: PipelineConfig) -> PipelineTaskResult:
    """Drop all OM tables before a fresh load.

    Only touches OM-owned tables. Other schemas and tables are never affected.
    Tables are recreated by subsequent fetch / dbt steps.
    """
    run_logger = get_run_logger()
    om_cfg = _load_config()

    tables = [
        (om_cfg["raw"]["schema"], om_cfg["raw"]["table"]),
        (om_cfg["staging"]["schema"], om_cfg["staging"]["table"]),
        (om_cfg["silver"]["schema"], om_cfg["silver"]["table"]),
        (om_cfg["gold_raw"]["schema"], om_cfg["gold_raw"]["table"]),
        (om_cfg["gold"]["schema"], om_cfg["gold"]["table"]),
        (om_cfg["gold_raw_meters"]["schema"], om_cfg["gold_raw_meters"]["table"]),
        (om_cfg["gold_meters"]["schema"], om_cfg["gold_meters"]["table"]),
    ]

    for schema, table in tables:
        run_logger.info("Dropping %s.%s", schema, table)
        _drop_om_table(cfg, table, schema)

    return PipelineTaskResult(status="success", command="cleanup_om_tables")


@task(name="Fetch Forecast Weather", retries=3, retry_delay_seconds=60)
def fetch_forecast_task(cfg: PipelineConfig) -> PipelineTaskResult:
    """Fetch Open-Meteo forecast and load into Postgres raw."""
    run_logger = get_run_logger()
    om_cfg = _load_config()

    location = om_cfg["location"]
    api = om_cfg["api"]
    forecast_cfg = om_cfg["forecast"]
    variables = om_cfg["variables"]["hourly"]
    raw = om_cfg["raw"]

    params: dict = {
        "latitude": location["latitude"],
        "longitude": location["longitude"],
        "hourly": variables,
        "timezone": location["timezone"],
    }
    if "forecast_hours" in forecast_cfg:
        params["forecast_hours"] = forecast_cfg["forecast_hours"]
    else:
        params["forecast_days"] = forecast_cfg["forecast_days"]
    if "past_hours" in forecast_cfg:
        params["past_hours"] = forecast_cfg["past_hours"]
    else:
        params["past_days"] = forecast_cfg["past_days"]

    response = requests.get(
        api["forecast_url"],
        params=params,
        timeout=api["timeout_seconds"],
    )
    response.raise_for_status()
    data = response.json()

    df = pd.DataFrame({
        "datetime": pd.to_datetime(data["hourly"]["time"]),
        **{k: v for k, v in data["hourly"].items() if k != "time"},
    })

    rows = _load_to_postgres(df, cfg, raw["table"], raw["schema"])
    run_logger.info("Loaded %d forecast rows into %s.%s", rows, raw["schema"], raw["table"])

    return PipelineTaskResult(status="success", command="fetch_forecast")


@task(name="Fetch Historical Weather", retries=3, retry_delay_seconds=60)
def fetch_historical_task(
    cfg: PipelineConfig,
    start_date: str | None = None,
    end_date: str | None = None,
) -> PipelineTaskResult:
    """Fetch Open-Meteo historical data in chunks and load into Postgres raw."""
    run_logger = get_run_logger()
    om_cfg = _load_config()

    location = om_cfg["location"]
    api = om_cfg["api"]
    historical_cfg = om_cfg["historical"]
    variables = om_cfg["variables"]["hourly"]
    raw = om_cfg["raw"]

    start_date = start_date or historical_cfg["start_date"]
    chunk_months = historical_cfg["chunk_months"]

    if end_date is None:
        end_date = datetime.now().strftime("%Y-%m-%d")

    start = pd.to_datetime(start_date)
    end = pd.to_datetime(end_date)
    chunks: list[pd.DataFrame] = []
    current_start = start

    while current_start < end:
        current_end = min(
            current_start + pd.DateOffset(months=chunk_months),
            end,
        )
        run_logger.info("Fetching %s to %s", current_start.date(), current_end.date())

        response = requests.get(
            api["archive_url"],
            params={
                "latitude": location["latitude"],
                "longitude": location["longitude"],
                "start_date": current_start.strftime("%Y-%m-%d"),
                "end_date": current_end.strftime("%Y-%m-%d"),
                "hourly": variables,
                "timezone": location["timezone"],
            },
            timeout=api["timeout_seconds"],
        )
        response.raise_for_status()
        data = response.json()

        chunk_df = pd.DataFrame({
            "datetime": pd.to_datetime(data["hourly"]["time"]),
            **{k: v for k, v in data["hourly"].items() if k != "time"},
        })
        chunks.append(chunk_df)

        current_start = current_end + pd.Timedelta(days=1)
        time.sleep(api["retry_sleep_seconds"])

    full_df = pd.concat(chunks, ignore_index=True)
    full_df = full_df.drop_duplicates(subset=["datetime"]).sort_values("datetime")

    rows = _load_to_postgres(full_df, cfg, raw["table"], raw["schema"])
    run_logger.info("Loaded %d historical rows into %s.%s", rows, raw["schema"], raw["table"])

    return PipelineTaskResult(status="success", command="fetch_historical")


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

    rows = _load_to_postgres(gold_df, cfg, gold_raw["table"], gold_raw["schema"])
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
    """
    Full Open-Meteo weather pipeline: ingest -> dbt staging -> silver -> tests.

    Pass config={"mode": "historical", "start_date": "2024-12-01"}
    for a one-time historical backfill.
    """
    run_logger = get_run_logger()

    cfg = PipelineConfig.model_validate(config or {})
    mode = (config or {}).get("mode", "forecast")

    result: dict = {"status": "success"}

    # --- Clean up old OM data ---
    result["cleanup"] = cleanup_om_tables(cfg)

    # --- Extract + Load ---
    if mode in ("historical", "both"):
        start_date = (config or {}).get("start_date")
        end_date = (config or {}).get("end_date")
        run_logger.info("Ingesting historical weather from %s", start_date)
        result["historical"] = fetch_historical_task(
            cfg, start_date=start_date, end_date=end_date,
        )

    if mode in ("forecast", "both"):
        run_logger.info("Ingesting forecast weather")
        result["forecast"] = fetch_forecast_task(cfg)

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
