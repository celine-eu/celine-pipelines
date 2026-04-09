"""Open-Meteo wind pipeline: API extraction + dbt transforms for Trentino.

Fetches hourly wind speed, gusts, and direction for a 4.4 km grid
covering the entire Trentino region (~798 grid points).
Uses ICON-D2 model (2.2 km native) via Open-Meteo API.

Schedule: every 4 hours (6x/day).
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

def _load_wind_config() -> Dict[str, Any]:
    """Load wind pipeline configuration from config_wind.yaml."""
    config_path = script_dir / "config_wind.yaml"
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
    """Create the raw wind table if it doesn't exist.

    Ensures the table schema matches what the extraction writes
    and what the dbt staging model expects.

    Args:
        engine: SQLAlchemy engine.
        schema: Target schema name.
        table: Target table name.
    """
    with engine.begin() as conn:
        conn.execute(sa.text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))
        conn.execute(sa.text(f"""
            CREATE TABLE IF NOT EXISTS {schema}.{table} (
                datetime           TIMESTAMP NOT NULL,
                lat                DOUBLE PRECISION NOT NULL,
                lon                DOUBLE PRECISION NOT NULL,
                wind_speed_10m     DOUBLE PRECISION,
                wind_gusts_10m     DOUBLE PRECISION,
                wind_direction_10m DOUBLE PRECISION,
                model              TEXT,
                _sdc_extracted_at  TIMESTAMP DEFAULT now()
            )
        """))


def _load_to_postgres(
    df: pd.DataFrame,
    engine: sa.Engine,
    table_name: str,
    schema: str,
) -> int:
    """Write wind DataFrame into Postgres raw table.

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

def _fetch_wind_data(
    grid: List[Tuple[float, float]],
    wind_cfg: Dict[str, Any],
) -> pd.DataFrame:
    """Fetch wind data from Open-Meteo API for all grid points.

    Uses POST with form-encoded body to avoid GET URL length limits and
    per-location rate limiting. Batches into chunks of max_points_per_call
    to stay under the POST payload size limit (~1000 points).

    Args:
        grid: List of (lat, lon) tuples.
        wind_cfg: Wind pipeline configuration dict.

    Returns:
        DataFrame with columns: datetime, lat, lon,
        wind_speed_10m, wind_gusts_10m, wind_direction_10m, model.

    Raises:
        requests.HTTPError: If the Open-Meteo API returns an error.
    """
    api_cfg = wind_cfg["api"]
    max_per_call = api_cfg["max_points_per_call"]
    all_frames: List[pd.DataFrame] = []

    for batch_idx, batch_start in enumerate(range(0, len(grid), max_per_call)):
        if batch_idx > 0:
            time.sleep(70)  # Open-Meteo per-minute limit; 70 s gives margin

        batch = grid[batch_start:batch_start + max_per_call]
        lats = ",".join(str(point[0]) for point in batch)
        lons = ",".join(str(point[1]) for point in batch)

        response = post_with_retry(
            api_cfg["base_url"],
            data={
                "latitude": lats,
                "longitude": lons,
                "hourly": ",".join(api_cfg["variables"]),
                "wind_speed_unit": api_cfg["wind_speed_unit"],
                "models": api_cfg["model"],
                "forecast_hours": str(api_cfg["forecast_hours"]),
                "timezone": api_cfg["timezone"],
            },
            timeout=120,
        )

        data = response.json()

        # Single location returns a dict; multiple returns a list
        if isinstance(data, dict):
            data = [data]

        for location_data in data:
            location_lat = location_data["latitude"]
            location_lon = location_data["longitude"]
            hourly = location_data["hourly"]

            location_df = pd.DataFrame({
                "datetime": pd.to_datetime(hourly["time"]),
                "lat": location_lat,
                "lon": location_lon,
                "wind_speed_10m": hourly["wind_speed_10m"],
                "wind_gusts_10m": hourly["wind_gusts_10m"],
                "wind_direction_10m": hourly["wind_direction_10m"],
                "model": api_cfg["model"],
            })
            all_frames.append(location_df)

    return pd.concat(all_frames, ignore_index=True)


# ---------------------------------------------------------------------------
# Prefect tasks
# ---------------------------------------------------------------------------

@task(name="Extract wind data", retries=3, retry_delay_seconds=120)
def extract_wind_data(cfg: PipelineConfig) -> PipelineTaskResult:
    """Fetch wind data from Open-Meteo and load into raw table."""
    run_logger = get_run_logger()
    wind_cfg = _load_wind_config()

    grid_cfg = wind_cfg["grid"]
    raw_cfg = wind_cfg["raw"]

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

    run_logger.info("Fetching wind data from Open-Meteo API (ICON-D2)...")
    wind_df = _fetch_wind_data(grid, wind_cfg)
    run_logger.info("Received %d rows from API", len(wind_df))

    rows = _load_to_postgres(
        wind_df, engine, raw_cfg["table"], raw_cfg["schema"],
    )
    run_logger.info(
        "Loaded %d rows into %s.%s", rows, raw_cfg["schema"], raw_cfg["table"],
    )

    return PipelineTaskResult(status="success", command="extract_wind_data")


@task(name="Cleanup wind data", retries=2, retry_delay_seconds=30)
def cleanup_wind_data(cfg: PipelineConfig) -> PipelineTaskResult:
    """Delete old raw wind records beyond retention period."""
    return dbt_run_operation("cleanup_om_wind", {}, cfg)


@task(name="Transform Staging Layer")
def transform_staging_task(cfg: PipelineConfig) -> PipelineTaskResult:
    """Run dbt staging models tagged 'wind'."""
    return dbt_run("-s staging,tag:wind", cfg)


@task(name="Transform Silver Layer")
def transform_silver_task(cfg: PipelineConfig) -> PipelineTaskResult:
    """Run dbt silver models tagged 'wind'."""
    return dbt_run("-s silver,tag:wind", cfg)


@task(name="Transform Gold Layer")
def transform_gold_task(cfg: PipelineConfig) -> PipelineTaskResult:
    """Run dbt gold models tagged 'wind'."""
    return dbt_run("-s gold,tag:wind", cfg)


@task(name="Test wind models")
def test_wind_task(cfg: PipelineConfig) -> PipelineTaskResult:
    """Run dbt tests for wind-tagged models only."""
    return dbt_run("test -s tag:wind", cfg)


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------

@flow(name="om-wind-flow")
def om_wind_flow(config: Dict[str, Any] | None = None) -> dict:
    """Wind pipeline for Trentino: Open-Meteo API -> dbt transforms.

    Fetches hourly wind speed, gusts, and direction for a 4.4 km grid
    covering Trentino (~798 points), then transforms through
    staging -> silver -> gold layers with gust alert classification.
    """
    cfg = PipelineConfig.model_validate(config or {})

    result: dict = {"status": "success"}

    # --- Extract + Load (direct API call, no Meltano) ---
    result["extract"] = extract_wind_data(cfg)

    # --- Cleanup old raw records ---
    result["cleanup"] = cleanup_wind_data(cfg)

    # --- Transform (staging -> silver -> gold) ---
    result["staging"] = transform_staging_task(cfg)
    result["silver"] = transform_silver_task(cfg)
    result["gold"] = transform_gold_task(cfg)

    # --- Tests ---
    result["tests"] = test_wind_task(cfg)

    return result


if __name__ == "__main__":
    wind_cfg = _load_wind_config()
    schedule = wind_cfg["schedule"]

    if DEV_MODE:
        om_wind_flow.serve(name=schedule["name"], cron=schedule["cron"])
