"""Open-Meteo observations pipeline: 15-min API extraction + dbt transforms for Trentino.

Fetches 15-minute resolution weather data for a 4.4 km grid covering the
entire Trentino region (~798 grid points). Uses ICON-D2 model (2.2 km native)
via Open-Meteo minutely_15 endpoint. 9-hour sliding window (6h past + 3h ahead).

Schedule: every 15 minutes.
"""

import logging
import os
import time
from pathlib import Path
from typing import Any, Dict, List, Tuple

import numpy as np
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
    dbt_run_operation,
)

from api_retry import post_with_retry

logger = logging.getLogger(__name__)

# Ensure APP_NAME and dbt paths are set
os.environ.setdefault("APP_NAME", "om")

script_dir = Path(__file__).parent
app_dir = script_dir.parent  # apps/om/
dbt_dir = str(app_dir / "dbt")

os.environ.setdefault("DBT_PROJECT_DIR", dbt_dir)
os.environ.setdefault("DBT_PROFILES_DIR", dbt_dir)


# ---------------------------------------------------------------------------
# Config loader
# ---------------------------------------------------------------------------

def _load_obs_config() -> Dict[str, Any]:
    """Load observations pipeline configuration from config_obs.yaml."""
    config_path = script_dir / "config_obs.yaml"
    with open(config_path) as fh:
        return yaml.safe_load(fh)


# ---------------------------------------------------------------------------
# Grid generation
# ---------------------------------------------------------------------------

def _generate_grid(
    lat_min: float,
    lat_max: float,
    lon_min: float,
    lon_max: float,
    spacing: float,
) -> List[Tuple[float, float]]:
    """Generate a regular lat/lon grid for the Trentino region.

    Args:
        lat_min: Southern boundary latitude.
        lat_max: Northern boundary latitude.
        lon_min: Western boundary longitude.
        lon_max: Eastern boundary longitude.
        spacing: Grid spacing in degrees.

    Returns:
        List of (lat, lon) tuples covering the bounding box.
    """
    lats = np.arange(lat_min, lat_max + spacing / 2, spacing)
    lons = np.arange(lon_min, lon_max + spacing / 2, spacing)

    grid = [
        (round(float(lat), 4), round(float(lon), 4))
        for lat in lats
        for lon in lons
    ]
    return grid


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_pg_engine(cfg: PipelineConfig) -> sa.Engine:
    """Build SQLAlchemy engine from PipelineConfig."""
    return sa.create_engine(
        f"postgresql://{cfg.postgres_user}:{cfg.postgres_password}"
        f"@{cfg.postgres_host}:{cfg.postgres_port}/{cfg.postgres_db}"
    )


def _ensure_raw_table(engine: sa.Engine, schema: str, table: str) -> None:
    """Create the raw observations table if it doesn't exist.

    Args:
        engine: SQLAlchemy engine.
        schema: Target schema name.
        table: Target table name.
    """
    with engine.begin() as conn:
        conn.execute(sa.text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))
        conn.execute(sa.text(f"""
            CREATE TABLE IF NOT EXISTS {schema}.{table} (
                datetime               TIMESTAMP NOT NULL,
                lat                    DOUBLE PRECISION NOT NULL,
                lon                    DOUBLE PRECISION NOT NULL,
                temperature_2m         DOUBLE PRECISION,
                apparent_temperature   DOUBLE PRECISION,
                wind_speed_10m         DOUBLE PRECISION,
                wind_gusts_10m         DOUBLE PRECISION,
                wind_direction_10m     DOUBLE PRECISION,
                precipitation          DOUBLE PRECISION,
                model                  TEXT,
                _sdc_extracted_at      TIMESTAMP DEFAULT now()
            )
        """))


def _load_to_postgres(
    df: pd.DataFrame,
    engine: sa.Engine,
    table_name: str,
    schema: str,
) -> int:
    """Write observations DataFrame into Postgres raw table.

    Args:
        df: DataFrame to write.
        engine: SQLAlchemy engine.
        table_name: Target table name.
        schema: Target schema name.

    Returns:
        Number of rows written.
    """
    df = df.copy()
    df["_sdc_extracted_at"] = pd.Timestamp.now()

    with engine.begin() as conn:
        df.to_sql(
            table_name, conn, schema=schema, if_exists="append", index=False,
        )

    return len(df)


# ---------------------------------------------------------------------------
# Open-Meteo API
# ---------------------------------------------------------------------------

def _fetch_obs_data(
    grid: List[Tuple[float, float]],
    obs_cfg: Dict[str, Any],
) -> pd.DataFrame:
    """Fetch 15-minute weather data from Open-Meteo API for all grid points.

    Uses POST with form-encoded body to avoid GET URL length limits and
    per-location rate limiting. Batches into chunks of max_points_per_call
    to stay under the POST payload size limit (~1000 points).

    Args:
        grid: List of (lat, lon) tuples.
        obs_cfg: Observations pipeline configuration dict.

    Returns:
        DataFrame with columns: datetime, lat, lon, temperature_2m,
        apparent_temperature, wind_speed_10m, wind_gusts_10m,
        wind_direction_10m, precipitation, model.

    Raises:
        requests.HTTPError: If the Open-Meteo API returns an error.
    """
    api_cfg = obs_cfg["api"]
    max_per_call = api_cfg["max_points_per_call"]
    all_frames: List[pd.DataFrame] = []

    for batch_idx, batch_start in enumerate(range(0, len(grid), max_per_call)):
        if batch_idx > 0:
            time.sleep(70)  # Open-Meteo per-minute limit; 70 s gives margin

        batch = grid[batch_start:batch_start + max_per_call]
        lats = ",".join(str(point[0]) for point in batch)
        lons = ",".join(str(point[1]) for point in batch)

        post_data = {
            "latitude": lats,
            "longitude": lons,
            "minutely_15": ",".join(api_cfg["variables"]),
            "wind_speed_unit": api_cfg["wind_speed_unit"],
            "past_minutely_15": str(api_cfg["past_minutely_15"]),
            "forecast_minutely_15": str(api_cfg["forecast_minutely_15"]),
            "timezone": api_cfg["timezone"],
        }
        model_name = api_cfg.get("model")
        if model_name:
            post_data["models"] = model_name

        response = post_with_retry(
            api_cfg["base_url"],
            data=post_data,
            timeout=120,
        )

        data = response.json()

        # Single location returns a dict; multiple returns a list
        if isinstance(data, dict):
            data = [data]

        for location_data in data:
            location_lat = location_data["latitude"]
            location_lon = location_data["longitude"]
            minutely = location_data["minutely_15"]

            location_df = pd.DataFrame({
                "datetime": pd.to_datetime(minutely["time"]),
                "lat": location_lat,
                "lon": location_lon,
                "temperature_2m": minutely["temperature_2m"],
                "apparent_temperature": minutely["apparent_temperature"],
                "wind_speed_10m": minutely["wind_speed_10m"],
                "wind_gusts_10m": minutely["wind_gusts_10m"],
                "wind_direction_10m": minutely["wind_direction_10m"],
                "precipitation": minutely["precipitation"],
                "model": model_name or "best_match",
            })
            all_frames.append(location_df)

    return pd.concat(all_frames, ignore_index=True)


# ---------------------------------------------------------------------------
# Prefect tasks
# ---------------------------------------------------------------------------

@task(name="Extract obs data", retries=3, retry_delay_seconds=60)
def extract_obs_data(cfg: PipelineConfig) -> PipelineTaskResult:
    """Fetch 15-min weather data from Open-Meteo and load into raw table."""
    run_logger = get_run_logger()
    obs_cfg = _load_obs_config()

    grid_cfg = obs_cfg["grid"]
    raw_cfg = obs_cfg["raw"]

    grid = _generate_grid(
        lat_min=grid_cfg["lat_min"],
        lat_max=grid_cfg["lat_max"],
        lon_min=grid_cfg["lon_min"],
        lon_max=grid_cfg["lon_max"],
        spacing=grid_cfg["spacing_deg"],
    )
    run_logger.info(
        "Generated grid: %d points (%.1f km spacing)",
        len(grid),
        grid_cfg["spacing_deg"] * 111,
    )

    engine = _get_pg_engine(cfg)
    _ensure_raw_table(engine, raw_cfg["schema"], raw_cfg["table"])

    run_logger.info("Fetching obs data from Open-Meteo API (ICON-D2 minutely_15)...")
    obs_df = _fetch_obs_data(grid, obs_cfg)
    run_logger.info("Received %d rows from API", len(obs_df))

    rows = _load_to_postgres(
        obs_df, engine, raw_cfg["table"], raw_cfg["schema"],
    )
    run_logger.info(
        "Loaded %d rows into %s.%s", rows, raw_cfg["schema"], raw_cfg["table"],
    )

    return PipelineTaskResult(status="success", command="extract_obs_data")


@task(name="Cleanup obs data", retries=2, retry_delay_seconds=30)
def cleanup_obs_data(cfg: PipelineConfig) -> PipelineTaskResult:
    """Delete old raw observation records beyond retention period."""
    return dbt_run_operation("cleanup_om_obs", {}, cfg)


@task(name="Transform Staging Layer")
def transform_staging_task(cfg: PipelineConfig) -> PipelineTaskResult:
    """Run dbt staging models tagged 'obs'."""
    return dbt_run("-s staging,tag:obs", cfg)


@task(name="Transform Silver Layer")
def transform_silver_task(cfg: PipelineConfig) -> PipelineTaskResult:
    """Run dbt silver models tagged 'obs'."""
    return dbt_run("-s silver,tag:obs", cfg)


@task(name="Test obs models")
def test_obs_task(cfg: PipelineConfig) -> PipelineTaskResult:
    """Run dbt tests for obs-tagged models only."""
    return dbt_run("test -s tag:obs", cfg)


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------

@flow(name="om-obs-flow")
def om_obs_flow(config: Dict[str, Any] | None = None) -> dict:
    """Observations pipeline for Trentino: Open-Meteo API -> dbt transforms.

    Fetches 15-minute resolution weather data for a 4.4 km grid covering
    Trentino (~798 points), then transforms through staging -> silver layers.
    """
    cfg = PipelineConfig.model_validate(config or {})

    result: dict = {"status": "success"}

    # --- Extract + Load (direct API call, no Meltano) ---
    result["extract"] = extract_obs_data(cfg)

    # --- Cleanup old raw records ---
    result["cleanup"] = cleanup_obs_data(cfg)

    # --- Transform (staging -> silver) ---
    result["staging"] = transform_staging_task(cfg)
    result["silver"] = transform_silver_task(cfg)

    # --- Tests ---
    result["tests"] = test_obs_task(cfg)

    return result


if __name__ == "__main__":
    obs_cfg = _load_obs_config()
    schedule = obs_cfg["schedule"]

    if DEV_MODE:
        om_obs_flow.serve(name=schedule["name"], cron=schedule["cron"])
