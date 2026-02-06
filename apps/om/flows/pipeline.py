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
    """Upsert weather DataFrame into Postgres raw schema."""
    df = df.copy()
    df["_sdc_extracted_at"] = pd.Timestamp.now()

    engine = _get_pg_engine(cfg)

    with engine.begin() as conn:
        # Ensure table exists
        df.head(0).to_sql(
            table_name, conn, schema=schema, if_exists="append", index=False
        )

        # Upsert via temp table
        temp_table = f"_tmp_{table_name}"
        df.to_sql(temp_table, conn, schema=schema, if_exists="replace", index=False)

        conn.execute(sa.text(f"""
            INSERT INTO {schema}.{table_name}
            SELECT t.*
            FROM {schema}.{temp_table} t
            LEFT JOIN {schema}.{table_name} w
                ON t.datetime = w.datetime
            WHERE w.datetime IS NULL
        """))

        conn.execute(sa.text(f"DROP TABLE IF EXISTS {schema}.{temp_table}"))

    return len(df)


# ---------------------------------------------------------------------------
# Prefect tasks
# ---------------------------------------------------------------------------

@task(name="Fetch Forecast Weather", retries=3, retry_delay_seconds=60)
def fetch_forecast_task(cfg: PipelineConfig) -> PipelineTaskResult:
    """Fetch Open-Meteo forecast and load into Postgres raw."""
    run_logger = get_run_logger()
    om_cfg = _load_config()

    location = om_cfg["location"]
    api = om_cfg["api"]
    forecast_cfg = om_cfg["forecast"]
    variables = om_cfg["variables"]["forecast"]
    raw = om_cfg["raw"]

    response = requests.get(
        api["forecast_url"],
        params={
            "latitude": location["latitude"],
            "longitude": location["longitude"],
            "hourly": variables,
            "forecast_days": forecast_cfg["days"],
            "past_days": forecast_cfg["past_days"],
            "timezone": location["timezone"],
        },
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
    forecast_vars = om_cfg["variables"]["forecast"]
    extra_vars = om_cfg["variables"]["historical_extra"]
    variables = forecast_vars + extra_vars
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


@task(name="Transform Staging Layer")
def transform_staging_task(cfg: PipelineConfig) -> PipelineTaskResult:
    """Run dbt staging for weather models."""
    return dbt_run("staging", cfg)


@task(name="Transform Silver Layer")
def transform_silver_task(cfg: PipelineConfig) -> PipelineTaskResult:
    """Run dbt silver for weather models."""
    return dbt_run("silver", cfg)


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

    # --- Extract + Load ---
    if mode in ("historical", "both"):
        start_date = (config or {}).get("start_date")
        end_date = (config or {}).get("end_date")
        run_logger.info("Ingesting historical weather from %s", start_date)
        result["historical"] = fetch_historical_task(
            cfg, start_date=start_date, end_date=end_date
        )

    if mode in ("forecast", "both"):
        run_logger.info("Ingesting forecast weather")
        result["forecast"] = fetch_forecast_task(cfg)

    # --- Transform ---
    run_logger.info("Running dbt transforms")
    result["staging"] = transform_staging_task(cfg)
    result["silver"] = transform_silver_task(cfg)
    result["tests"] = run_dbt_tests_task(cfg)

    return result


if __name__ == "__main__":
    om_cfg = _load_config()
    schedule = om_cfg["schedule"]

    if DEV_MODE:
        om_flow.serve(name=schedule["name"], cron=schedule["cron"])
