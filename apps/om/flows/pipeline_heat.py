"""Open-Meteo heat pipeline: API extraction + dbt transforms for Trentino.

Fetches daily max temperature for a 4.4 km grid covering the entire
Trentino region (~1064 grid points). Uses ICON-D2 model via Open-Meteo API.
Includes elevation from API response for altitude-band classification.

Schedule: 2x/day (06:00, 18:00).
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

def _load_heat_config() -> Dict[str, Any]:
    """Load heat pipeline configuration from config_heat.yaml."""
    config_path = script_dir / "config_heat.yaml"
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
    """Create the raw heat table if it doesn't exist.

    Args:
        engine: SQLAlchemy engine.
        schema: Target schema name.
        table: Target table name.
    """
    with engine.begin() as conn:
        conn.execute(sa.text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))
        conn.execute(sa.text(f"""
            CREATE TABLE IF NOT EXISTS {schema}.{table} (
                date               DATE NOT NULL,
                lat                DOUBLE PRECISION NOT NULL,
                lon                DOUBLE PRECISION NOT NULL,
                temperature_2m_max DOUBLE PRECISION,
                elevation          DOUBLE PRECISION,
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
    """Write heat DataFrame into Postgres raw table.

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

def _fetch_heat_data(
    grid: List[Tuple[float, float]],
    heat_cfg: Dict[str, Any],
) -> pd.DataFrame:
    """Fetch daily max temperature from Open-Meteo API for all grid points.

    Uses POST with form-encoded body to avoid GET URL length limits and
    per-location rate limiting. Batches into chunks of max_points_per_call
    to stay under the POST payload size limit (~1000 points).

    Args:
        grid: List of (lat, lon) tuples.
        heat_cfg: Heat pipeline configuration dict.

    Returns:
        DataFrame with columns: date, lat, lon,
        temperature_2m_max, elevation, model.

    Raises:
        requests.HTTPError: If the Open-Meteo API returns an error.
    """
    api_cfg = heat_cfg["api"]
    max_per_call = api_cfg["max_points_per_call"]
    all_frames: List[pd.DataFrame] = []

    for batch_idx, batch_start in enumerate(range(0, len(grid), max_per_call)):
        if batch_idx > 0:
            time.sleep(61)  # Open-Meteo counts locations per minute; wait for reset

        batch = grid[batch_start:batch_start + max_per_call]
        lats = ",".join(str(point[0]) for point in batch)
        lons = ",".join(str(point[1]) for point in batch)

        response = requests.post(
            api_cfg["base_url"],
            data={
                "latitude": lats,
                "longitude": lons,
                "daily": ",".join(api_cfg["variables"]),
                "models": api_cfg["model"],
                "forecast_days": str(api_cfg["forecast_days"]),
                "past_days": str(api_cfg["past_days"]),
                "timezone": api_cfg["timezone"],
            },
            timeout=120,
        )
        response.raise_for_status()

        data = response.json()

        # Single location returns a dict; multiple returns a list
        if isinstance(data, dict):
            data = [data]

        for location_data in data:
            location_lat = location_data["latitude"]
            location_lon = location_data["longitude"]
            location_elevation = location_data.get("elevation", None)
            daily = location_data["daily"]

            dates = [pd.Timestamp(ts).date() for ts in daily["time"]]
            location_df = pd.DataFrame({
                "date": dates,
                "lat": location_lat,
                "lon": location_lon,
                "temperature_2m_max": daily["temperature_2m_max"],
                "elevation": location_elevation,
                "model": api_cfg["model"],
            })
            all_frames.append(location_df)

    return pd.concat(all_frames, ignore_index=True)


# ---------------------------------------------------------------------------
# Prefect tasks
# ---------------------------------------------------------------------------

@task(name="Extract heat data", retries=3, retry_delay_seconds=120)
def extract_heat_data(cfg: PipelineConfig) -> PipelineTaskResult:
    """Fetch temperature data from Open-Meteo and load into raw table."""
    run_logger = get_run_logger()
    heat_cfg = _load_heat_config()

    grid_cfg = heat_cfg["grid"]
    raw_cfg = heat_cfg["raw"]

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

    run_logger.info("Fetching heat data from Open-Meteo API (ICON-D2)...")
    heat_df = _fetch_heat_data(grid, heat_cfg)
    run_logger.info("Received %d rows from API", len(heat_df))

    rows = _load_to_postgres(
        heat_df, engine, raw_cfg["table"], raw_cfg["schema"],
    )
    run_logger.info(
        "Loaded %d rows into %s.%s", rows, raw_cfg["schema"], raw_cfg["table"],
    )

    return PipelineTaskResult(status="success", command="extract_heat_data")


@task(name="Cleanup heat data", retries=2, retry_delay_seconds=30)
def cleanup_heat_data(cfg: PipelineConfig) -> PipelineTaskResult:
    """Delete old raw heat records beyond retention period."""
    return dbt_run_operation("cleanup_om_heat", {}, cfg)


@task(name="Transform Staging Layer")
def transform_staging_task(cfg: PipelineConfig) -> PipelineTaskResult:
    """Run dbt staging models tagged 'heat'."""
    return dbt_run("-s staging,tag:heat", cfg)


@task(name="Transform Silver Layer")
def transform_silver_task(cfg: PipelineConfig) -> PipelineTaskResult:
    """Run dbt silver models tagged 'heat'."""
    return dbt_run("-s silver,tag:heat", cfg)


@task(name="Transform Gold Layer")
def transform_gold_task(cfg: PipelineConfig) -> PipelineTaskResult:
    """Run dbt gold models tagged 'heat'."""
    return dbt_run("-s gold,tag:heat", cfg)


@task(name="Test heat models")
def test_heat_task(cfg: PipelineConfig) -> PipelineTaskResult:
    """Run dbt tests for heat-tagged models only."""
    return dbt_run("test -s tag:heat", cfg)


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------

@flow(name="om-heat-flow")
def om_heat_flow(config: Dict[str, Any] | None = None) -> dict:
    """Heat pipeline for Trentino: Open-Meteo API -> dbt transforms.

    Fetches daily max temperature for a 4.4 km grid covering Trentino
    (~1064 points), then transforms through staging -> silver -> gold
    layers with altitude-band P90 heat stress classification.
    """
    cfg = PipelineConfig.model_validate(config or {})

    result: dict = {"status": "success"}

    # --- Extract + Load (direct API call, no Meltano) ---
    result["extract"] = extract_heat_data(cfg)

    # --- Cleanup old raw records ---
    result["cleanup"] = cleanup_heat_data(cfg)

    # --- Transform (staging -> silver -> gold) ---
    result["staging"] = transform_staging_task(cfg)
    result["silver"] = transform_silver_task(cfg)
    result["gold"] = transform_gold_task(cfg)

    # --- Tests ---
    result["tests"] = test_heat_task(cfg)

    return result


if __name__ == "__main__":
    heat_cfg = _load_heat_config()
    schedule = heat_cfg["schedule"]

    if DEV_MODE:
        om_heat_flow.serve(name=schedule["name"], cron=schedule["cron"])
