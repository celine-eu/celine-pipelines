import logging
import os
import sys
from pathlib import Path
from typing import Any, Dict

import pandas as pd
import sqlalchemy as sa
import yaml
from prefect import flow, task

from celine.utils.pipelines.pipeline import (
    DEV_MODE,
    PipelineConfig,
    PipelineTaskResult,
    PipelineStatus,
    dbt_run,
)

_flows_dir = str(Path(__file__).parent)
if _flows_dir not in sys.path:
    sys.path.insert(0, _flows_dir)

from db import get_engine_from_config, load_predictions, already_processed
from detector import create_detector
from providers import create_provider, FilesystemProvider

logger = logging.getLogger(__name__)

os.environ.setdefault("APP_NAME", "pv_detection")

script_dir = Path(__file__).parent
app_dir = script_dir.parent
dbt_dir = str(app_dir / "dbt")

os.environ.setdefault("DBT_PROJECT_DIR", dbt_dir)
os.environ.setdefault("DBT_PROFILES_DIR", dbt_dir)


def _load_config() -> Dict[str, Any]:
    config_path = script_dir / "config.yaml"
    with open(config_path) as fh:
        config = yaml.safe_load(fh)
    ollama = config.get("detector", {}).get("ollama", {})
    if os.environ.get("OLLAMA_HOST"):
        ollama["host"] = os.environ["OLLAMA_HOST"]
    if os.environ.get("OLLAMA_MODEL"):
        ollama["model"] = os.environ["OLLAMA_MODEL"]
    return config


def _get_pg_engine(cfg: PipelineConfig) -> sa.Engine:
    return get_engine_from_config(cfg)


def _load_buildings(engine: sa.Engine, config: dict) -> pd.DataFrame:
    """Load building list from DB, applying optional filters."""
    bld_cfg = config["buildings"]
    schema = bld_cfg["schema"]
    table = bld_cfg["table"]

    filters = config.get("filters", {})

    where_clauses = []
    params: dict[str, Any] = {}

    if filters.get("bbox"):
        bbox = filters["bbox"]
        where_clauses.append(
            "ST_Intersects(geometry, ST_MakeEnvelope(:xmin, :ymin, :xmax, :ymax, 4326))"
        )
        params.update(xmin=bbox[0], ymin=bbox[1], xmax=bbox[2], ymax=bbox[3])

    if filters.get("min_area_m2"):
        where_clauses.append("footprint_area_m2 >= :min_area")
        params["min_area"] = filters["min_area_m2"]

    if filters.get("building_ids"):
        where_clauses.append("building_id = ANY(:bids)")
        params["bids"] = filters["building_ids"]

    if filters.get("limit"):
        limit_clause = f"LIMIT {int(filters['limit'])}"
    else:
        limit_clause = ""

    where_sql = (" AND ".join(where_clauses)) if where_clauses else "TRUE"

    query = sa.text(f"""
        SELECT
            building_id,
            ST_X(ST_Centroid(geometry)) as lon,
            ST_Y(ST_Centroid(geometry)) as lat,
            footprint_area_m2
        FROM {schema}.{table}
        WHERE {where_sql}
        ORDER BY building_id
        {limit_clause}
    """)

    with engine.connect() as conn:
        return pd.read_sql(query, conn, params=params)


@task(name="Detect PV Panels", retries=1, retry_delay_seconds=60)
def detect_pv_task(cfg: PipelineConfig) -> PipelineTaskResult:
    det_cfg = _load_config()

    provider = create_provider(det_cfg["provider"])
    detector = create_detector(det_cfg["detector"])
    engine = _get_pg_engine(cfg)

    if isinstance(provider, FilesystemProvider) and det_cfg.get("filters", {}).get("use_available_tiles"):
        available = provider.available_ids()
        building_ids = [{"building_id": bid, "lon": None, "lat": None, "footprint_area_m2": None} for bid in available]
        buildings = pd.DataFrame(building_ids)
        logger.info("Filesystem mode: using %d available tiles", len(buildings))
    else:
        buildings = _load_buildings(engine, det_cfg)
        logger.info("Loaded %d buildings from DB", len(buildings))

    total_buildings = len(buildings)
    force_refresh = det_cfg.get("filters", {}).get("force_refresh", False)

    pred_cfg = det_cfg["predictions"]
    if not force_refresh:
        already_done = already_processed(engine, pred_cfg["schema"], pred_cfg["table"])
        if already_done:
            buildings = buildings[~buildings["building_id"].isin(already_done)]
            logger.info(
                "Skipping %d already-processed buildings, %d remaining",
                total_buildings - len(buildings), len(buildings),
            )
    else:
        logger.info("Force refresh: reprocessing all %d buildings", total_buildings)

    if buildings.empty:
        return PipelineTaskResult(
            status=PipelineStatus.COMPLETED,
            command="detect_pv",
            details=f"All {total_buildings} buildings already processed",
        )

    results = []
    skipped = 0

    for _, row in buildings.iterrows():
        bid = row["building_id"]

        image_bytes = provider.get_tile(bid)
        if image_bytes is None:
            skipped += 1
            continue

        try:
            result = detector.detect(image_bytes, bid)
        except Exception as e:
            logger.error("Detection failed for %s: %s", bid, e)
            continue

        result_dict = result.to_dict()

        if row.get("lon") is not None:
            result_dict["lon"] = row["lon"]
            result_dict["lat"] = row["lat"]

        results.append(result_dict)
        logger.info(
            "%s: has_pv=%s confidence=%.2f",
            bid, result.has_pv, result.confidence,
        )

    if not results:
        return PipelineTaskResult(
            status=PipelineStatus.COMPLETED,
            command="detect_pv",
            details=f"No tiles matched ({len(buildings)} buildings, {skipped} skipped)",
        )

    df = pd.DataFrame(results)
    rows = load_predictions(engine, df, pred_cfg["schema"], pred_cfg["table"])

    return PipelineTaskResult(
        status=PipelineStatus.COMPLETED,
        command="detect_pv",
        details=f"{total_buildings} buildings, {total_buildings - len(buildings)} already done, {len(results)} detected, {skipped} no tile, {rows} written",
    )


@task(name="Transform Staging Layer")
def transform_staging_task(cfg: PipelineConfig) -> PipelineTaskResult:
    return dbt_run("staging", cfg)


@task(name="Transform Silver Layer")
def transform_silver_task(cfg: PipelineConfig) -> PipelineTaskResult:
    return dbt_run("silver", cfg)


@task(name="Transform Gold Layer")
def transform_gold_task(cfg: PipelineConfig) -> PipelineTaskResult:
    return dbt_run("gold", cfg)


@task(name="Run dbt Tests")
def run_dbt_tests_task(cfg: PipelineConfig) -> PipelineTaskResult:
    return dbt_run("test", cfg)


@flow(name="pv-detection-flow")
async def pv_detection_flow(config: Dict[str, Any] | None = None):
    cfg = PipelineConfig.model_validate(config or {})

    results = {}
    results["detect"] = detect_pv_task(cfg)
    results["staging"] = transform_staging_task(cfg)
    results["silver"] = transform_silver_task(cfg)
    results["gold"] = transform_gold_task(cfg)
    results["tests"] = run_dbt_tests_task(cfg)

    return results


if __name__ == "__main__":
    det_cfg = _load_config()
    schedule = det_cfg.get("schedule", {})
    if DEV_MODE:
        pv_detection_flow.serve(
            name=schedule.get("name", "pv-detection-weekly"),
            cron=schedule.get("cron", "0 3 * * 0"),
        )
