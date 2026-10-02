"""Open-Meteo soil pipeline: API extraction + dbt transforms for Trentino.

Fetches daily soil temperature (28-100cm and 100-255cm layers) from the
Open-Meteo ERA5-Land archive API for the same 4.4 km Trentino grid used by
pipeline_heat.py (~1064 points). This is a separate flow, not a variable
added to pipeline_heat.py, because:

- Soil temperature at these depths only exists on the archive API
  (archive-api.open-meteo.com), a different host with different rate
  limits than the forecast API the heat flow hits.
- The archive API lags a few days for the most recent dates (they come
  back with NULL soil values until ERA5-Land catches up); that lag must
  not fail or delay the heat forecast flow.

Coordinate trap: the archive API snaps requested coordinates to the
nearest 0.1 deg ERA5-Land cell and returns the snapped coordinates in the
response. pipeline_heat.py stores the API-returned (snapped) lat/lon;
this pipeline instead stores the *requested* grid coordinates as lat/lon
(api_lat/api_lon kept for provenance), so soil and heat points never
silently fail to join by equality -- Task 3.2 joins them spatially.

Schedule: 1x/day at 07:45.
"""

import logging
import os
import time
from pathlib import Path
from typing import Any

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
)

from api_retry import post_with_retry
from pipeline_heat import _generate_grid, _get_pg_engine, _load_to_postgres

logger = logging.getLogger(__name__)

# Ensure APP_NAME and dbt paths are set
os.environ.setdefault("APP_NAME", "om")

script_dir = Path(__file__).parent
app_dir = script_dir.parent  # apps/om/
dbt_dir = str(app_dir / "dbt")

os.environ.setdefault("DBT_PROJECT_DIR", dbt_dir)
os.environ.setdefault("DBT_PROFILES_DIR", dbt_dir)

# Depth-weighted composite at 1m (100cm) depth: linear interpolation between
# the two ERA5-Land layer midpoints (64cm, 177.5cm) that solves for the
# value at exactly 100cm. Mirrored in dbt/models/staging/soil/stg_om_soil.sql
# -- keep the two in sync if these ever change.
SOIL_WEIGHT_UPPER = 0.683  # soil_temperature_28_to_100cm_mean
SOIL_WEIGHT_LOWER = 0.317  # soil_temperature_100_to_255cm_mean


# ---------------------------------------------------------------------------
# Config loader
# ---------------------------------------------------------------------------


def _load_soil_config() -> dict[str, Any]:
    """Load soil pipeline configuration from config_soil.yaml."""
    config_path = script_dir / "config_soil.yaml"
    with open(config_path) as fh:
        return yaml.safe_load(fh)


# ---------------------------------------------------------------------------
# Pure helpers
# ---------------------------------------------------------------------------


def _soil_1m(soil_28_100, soil_100_255):
    """Depth-weighted composite soil temperature at 1m (100cm) depth.

    Linearly interpolates between the two ERA5-Land layer midpoints
    (64cm for the 28-100cm layer, 177.5cm for the 100-255cm layer) to
    estimate the value at exactly 100cm. Accepts scalars or, since the
    weights are plain floats, pandas Series/numpy arrays.

    Args:
        soil_28_100: soil_temperature_28_to_100cm_mean value(s).
        soil_100_255: soil_temperature_100_to_255cm_mean value(s).

    Returns:
        The composite soil_1m_c value(s).
    """
    return SOIL_WEIGHT_UPPER * soil_28_100 + SOIL_WEIGHT_LOWER * soil_100_255


def _soil_rows_from_response(
    batch: list[tuple[float, float]],
    data: list[dict[str, Any]],
) -> pd.DataFrame:
    """Flatten one archive API response into soil rows for one batch.

    Stores the *requested* grid lat/lon (batch) as lat/lon, and the
    API-returned (snapped to the nearest 0.1 deg ERA5-Land cell)
    coordinates as api_lat/api_lon for provenance. Drops rows where both
    soil values are NULL (day not yet available in the archive). Pure:
    no network calls.

    Args:
        batch: Requested (lat, lon) points, in the order sent to the API.
        data: Parsed JSON response, already normalized to a list (one
            dict per location, in the same order as `batch`).

    Returns:
        DataFrame with columns: date, lat, lon,
        soil_temperature_28_to_100cm_mean, soil_temperature_100_to_255cm_mean,
        api_lat, api_lon.

    Raises:
        ValueError: If the response doesn't have exactly one entry per
            requested point.
    """
    if len(data) != len(batch):
        raise ValueError(
            f"Archive API returned {len(data)} locations for a batch of "
            f"{len(batch)} requested points"
        )

    frames: list[pd.DataFrame] = []
    for (requested_lat, requested_lon), location_data in zip(batch, data):
        daily = location_data["daily"]
        location_df = pd.DataFrame(
            {
                "date": [pd.Timestamp(ts).date() for ts in daily["time"]],
                "lat": requested_lat,
                "lon": requested_lon,
                "soil_temperature_28_to_100cm_mean": daily[
                    "soil_temperature_28_to_100cm_mean"
                ],
                "soil_temperature_100_to_255cm_mean": daily[
                    "soil_temperature_100_to_255cm_mean"
                ],
                "api_lat": location_data["latitude"],
                "api_lon": location_data["longitude"],
            }
        )
        frames.append(location_df)

    result = pd.concat(frames, ignore_index=True)

    both_null = (
        result["soil_temperature_28_to_100cm_mean"].isna()
        & result["soil_temperature_100_to_255cm_mean"].isna()
    )
    return result.loc[~both_null].reset_index(drop=True)


# ---------------------------------------------------------------------------
# Postgres
# ---------------------------------------------------------------------------


def _ensure_raw_table(engine: sa.Engine, schema: str, table: str) -> None:
    """Create the raw soil table if it doesn't exist.

    Args:
        engine: SQLAlchemy engine.
        schema: Target schema name.
        table: Target table name.
    """
    with engine.begin() as conn:
        conn.execute(sa.text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))
        conn.execute(
            sa.text(f"""
            CREATE TABLE IF NOT EXISTS {schema}.{table} (
                date                                DATE NOT NULL,
                lat                                 DOUBLE PRECISION NOT NULL,
                lon                                 DOUBLE PRECISION NOT NULL,
                soil_temperature_28_to_100cm_mean   DOUBLE PRECISION,
                soil_temperature_100_to_255cm_mean  DOUBLE PRECISION,
                api_lat                             DOUBLE PRECISION,
                api_lon                             DOUBLE PRECISION,
                _sdc_extracted_at                   TIMESTAMP DEFAULT now()
            )
        """)
        )


# ---------------------------------------------------------------------------
# Open-Meteo archive API
# ---------------------------------------------------------------------------


def _fetch_soil_data(
    grid: list[tuple[float, float]],
    soil_cfg: dict[str, Any],
) -> pd.DataFrame:
    """Fetch daily soil temperature from the Open-Meteo archive API.

    Uses POST with form-encoded body, batched into chunks of
    max_points_per_call, sleeping between batches to respect the archive
    API's per-minute rate limit.

    Args:
        grid: List of (lat, lon) tuples.
        soil_cfg: Soil pipeline configuration dict.

    Returns:
        DataFrame with columns: date, lat, lon,
        soil_temperature_28_to_100cm_mean, soil_temperature_100_to_255cm_mean,
        api_lat, api_lon.

    Raises:
        requests.HTTPError: If the Open-Meteo API returns an error.
        ValueError: If a response doesn't match its requested batch.
    """
    api_cfg = soil_cfg["api"]
    max_per_call = api_cfg["max_points_per_call"]

    today = pd.Timestamp.now(tz="Europe/Rome").normalize().date()
    start_date = today - pd.Timedelta(days=api_cfg["lookback_days"])

    all_frames: list[pd.DataFrame] = []

    for batch_idx, batch_start in enumerate(range(0, len(grid), max_per_call)):
        if batch_idx > 0:
            time.sleep(70)  # Archive API per-minute limit; 70s gives margin

        batch = grid[batch_start : batch_start + max_per_call]
        lats = ",".join(str(point[0]) for point in batch)
        lons = ",".join(str(point[1]) for point in batch)

        post_data = {
            "latitude": lats,
            "longitude": lons,
            "daily": ",".join(api_cfg["variables"]),
            "start_date": start_date.isoformat(),
            "end_date": today.isoformat(),
            "timezone": api_cfg["timezone"],
        }

        response = post_with_retry(
            api_cfg["base_url"],
            data=post_data,
            timeout=120,
        )

        data = response.json()

        # Single location returns a dict; multiple returns a list
        if isinstance(data, dict):
            data = [data]

        all_frames.append(_soil_rows_from_response(batch, data))

    return pd.concat(all_frames, ignore_index=True)


# ---------------------------------------------------------------------------
# Prefect tasks
# ---------------------------------------------------------------------------


@task(name="Extract soil data", retries=1, retry_delay_seconds=180)
def extract_soil_data(cfg: PipelineConfig) -> PipelineTaskResult:
    """Fetch soil temperature data from Open-Meteo and load into raw table."""
    run_logger = get_run_logger()
    soil_cfg = _load_soil_config()

    grid_cfg = soil_cfg["grid"]
    raw_cfg = soil_cfg["raw"]

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

    run_logger.info("Fetching soil data from Open-Meteo archive API...")
    soil_df = _fetch_soil_data(grid, soil_cfg)

    if len(soil_df) > 0:
        mean_soil_1m = _soil_1m(
            soil_df["soil_temperature_28_to_100cm_mean"],
            soil_df["soil_temperature_100_to_255cm_mean"],
        ).mean()
        run_logger.info(
            "Received %d rows from API (mean soil_1m_c=%.2f)",
            len(soil_df),
            mean_soil_1m,
        )
    else:
        run_logger.warning("Received 0 rows from API")

    rows = _load_to_postgres(
        soil_df,
        engine,
        raw_cfg["table"],
        raw_cfg["schema"],
    )
    run_logger.info(
        "Loaded %d rows into %s.%s",
        rows,
        raw_cfg["schema"],
        raw_cfg["table"],
    )

    return PipelineTaskResult(status="success", command="extract_soil_data")


@task(name="Cleanup soil data", retries=2, retry_delay_seconds=30)
def cleanup_soil_data(cfg: PipelineConfig) -> PipelineTaskResult:
    """Delete old raw soil records beyond retention period."""
    return dbt_run_operation("cleanup_om_soil", {}, cfg)


@task(name="Transform Staging Layer")
def transform_staging_task(cfg: PipelineConfig) -> PipelineTaskResult:
    """Run dbt staging models tagged 'soil'."""
    return dbt_run("-s staging,tag:soil", cfg)


@task(name="Transform Silver Layer")
def transform_silver_task(cfg: PipelineConfig) -> PipelineTaskResult:
    """Run dbt silver models tagged 'soil'."""
    return dbt_run("-s silver,tag:soil", cfg)


@task(name="Transform Gold Layer")
def transform_gold_task(cfg: PipelineConfig) -> PipelineTaskResult:
    """Run dbt gold models tagged 'soil'."""
    return dbt_run("-s gold,tag:soil", cfg)


@task(name="Test soil models")
def test_soil_task(cfg: PipelineConfig) -> PipelineTaskResult:
    """Run dbt tests for soil-tagged models only."""
    return dbt_run("test -s tag:soil", cfg)


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------


@flow(name="om-soil-flow")
def om_soil_flow(config: dict[str, Any] | None = None) -> dict:
    """Soil pipeline for Trentino: Open-Meteo archive API -> dbt transforms.

    Fetches daily soil temperature (28-100cm and 100-255cm layers) for the
    same 4.4 km grid as the heat pipeline (~1064 points), then transforms
    through staging -> silver -> gold layers with a 7-day trailing mean
    and P90-based soil heat status.
    """
    cfg = PipelineConfig.model_validate(config or {})

    result: dict = {"status": "success"}

    # --- Extract + Load (direct API call, no Meltano) ---
    result["extract"] = extract_soil_data(cfg)

    # --- Cleanup old raw records ---
    result["cleanup"] = cleanup_soil_data(cfg)

    # --- Transform (staging -> silver -> gold) ---
    result["staging"] = transform_staging_task(cfg)
    result["silver"] = transform_silver_task(cfg)
    result["gold"] = transform_gold_task(cfg)

    # --- Tests ---
    result["tests"] = test_soil_task(cfg)

    return result


if __name__ == "__main__":
    soil_cfg = _load_soil_config()
    schedule = soil_cfg["schedule"]

    if DEV_MODE:
        om_soil_flow.serve(name=schedule["name"], cron=schedule["cron"])
