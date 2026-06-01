import asyncio
import logging
import os
import sys
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
from typing import Any, Dict

import pandas as pd
import yaml
import sqlalchemy as sa

from db import (
    get_engine,
    ensure_table,
    truncate_estimates,
    load_estimates,
    already_processed,
    load_eligible_buildings,
)

logger = logging.getLogger(__name__)

script_dir = Path(__file__).parent


def load_config() -> Dict[str, Any]:
    config_path = script_dir / "config.yaml"
    with open(config_path) as fh:
        return yaml.safe_load(fh)


def classify_building(area: float, num_floors, building_class, config: dict) -> str:
    """Classify a building as residential/commercial/industrial/office.

    Priority: explicit Overture class > heuristic from area + floors > default.
    """
    est = config["estimation"]

    if isinstance(building_class, str):
        mapped = est["user_type_mapping"].get(building_class.lower())
        if mapped:
            return mapped

    clf = est.get("classification", {})
    max_res_area = clf.get("max_residential_area_m2", 200)
    max_res_floors = clf.get("max_residential_floors", 3)
    industrial_min_area = clf.get("industrial_min_area_m2", 500)

    floors = int(num_floors) if isinstance(num_floors, (int, float)) and num_floors == num_floors else 0

    if floors > max_res_floors:
        return "commercial"

    if area > max_res_area:
        if area >= industrial_min_area and floors <= 1:
            return "industrial"
        return "commercial"

    return est["default_user_type"]


def estimate_consumption(area: float, user_type: str, config: dict) -> float:
    est = config["estimation"]
    if user_type == "residential":
        return est.get("residential_consumption_kwh", 3500)
    factor = est["consumption_per_m2"].get(user_type, 80)
    return area * factor


def size_system(area: float, user_type: str, consumption: float, config: dict) -> float:
    """Size PV system in kWp, accounting for rooftop capacity and consumption."""
    est = config["estimation"]
    rooftop_kwp = area * est["panel_kwp_per_m2"]
    max_kwp = est.get("max_kwp", 20)

    if user_type == "residential":
        # Size to cover consumption + headroom, don't over-build
        headroom = est.get("residential_sizing_headroom", 0.2)
        consumption_kwp = consumption * (1 + headroom) / est["specific_yield"]
        kwp = min(rooftop_kwp, consumption_kwp, max_kwp)
    else:
        kwp = min(rooftop_kwp, max_kwp)

    return round(kwp, 2)


def _compute_one(args: tuple) -> dict | None:
    """Worker function for ProcessPoolExecutor. Must be top-level and picklable."""
    building_id, lat, lon, area, building_class, num_floors, config = args

    from celine.roi import calculate_roi

    est = config["estimation"]

    user_type = classify_building(area, num_floors, building_class, config)

    if not area or area <= 0:
        return None

    min_kwp = est.get("min_kwp", 3)
    annual_consumption = estimate_consumption(area, user_type, config)
    kwp = size_system(area, user_type, annual_consumption, config)
    if kwp < min_kwp:
        return None

    annual_production = kwp * est["specific_yield"]
    capex = kwp * est["capex_per_kwp"]

    result = calculate_roi(
        kwp=kwp,
        latitude=lat,
        longitude=lon,
        capex=capex,
        annual_consumption_kwh=annual_consumption,
        user_type=user_type,
        regime=est["regime"],
        annual_production_kwh=annual_production,
    )

    return {
        "building_id": building_id,
        "kwp": round(kwp, 2),
        "capex": round(capex, 2),
        "annual_production_kwh": round(annual_production, 1),
        "annual_consumption_kwh": round(annual_consumption, 1),
        "user_type": user_type,
        "regime": est["regime"],
        "npv": round(result.finance.npv, 2),
        "irr": round(result.finance.irr, 4) if result.finance.irr else None,
        "payback_simple": round(result.finance.payback_simple, 1) if result.finance.payback_simple else None,
        "payback_discounted": round(result.finance.payback_discounted, 1) if result.finance.payback_discounted else None,
        "tasso_autoconsumo": round(result.energy.tasso_autoconsumo, 4),
    }


async def estimate_building(row: pd.Series, config: dict) -> dict | None:
    """Single-building async estimate (used by Prefect flow)."""
    from celine.roi import calculate_roi_async

    est = config["estimation"]
    area = row["footprint_area_m2"]
    if not area or area <= 0:
        return None

    user_type = classify_building(area, row.get("num_floors"), row.get("building_class"), config)

    min_kwp = est.get("min_kwp", 3)
    annual_consumption = estimate_consumption(area, user_type, config)
    kwp = size_system(area, user_type, annual_consumption, config)
    if kwp < min_kwp:
        return None

    annual_production = kwp * est["specific_yield"]
    capex = kwp * est["capex_per_kwp"]

    result = await calculate_roi_async(
        kwp=kwp,
        latitude=row["lat"],
        longitude=row["lon"],
        capex=capex,
        annual_consumption_kwh=annual_consumption,
        user_type=user_type,
        regime=est["regime"],
        annual_production_kwh=annual_production,
    )

    return {
        "building_id": row["building_id"],
        "kwp": round(kwp, 2),
        "capex": round(capex, 2),
        "annual_production_kwh": round(annual_production, 1),
        "annual_consumption_kwh": round(annual_consumption, 1),
        "user_type": user_type,
        "regime": est["regime"],
        "npv": round(result.finance.npv, 2),
        "irr": round(result.finance.irr, 4) if result.finance.irr else None,
        "payback_simple": round(result.finance.payback_simple, 1) if result.finance.payback_simple else None,
        "payback_discounted": round(result.finance.payback_discounted, 1) if result.finance.payback_discounted else None,
        "tasso_autoconsumo": round(result.energy.tasso_autoconsumo, 4),
    }


def _prepare_work_items(buildings: pd.DataFrame, config: dict) -> list[tuple]:
    items = []
    for _, row in buildings.iterrows():
        bc = row.get("building_class")
        if not isinstance(bc, str):
            bc = None
        nf = row.get("num_floors")
        if not isinstance(nf, (int, float)) or nf != nf:
            nf = None
        items.append((
            row["building_id"],
            row["lat"],
            row["lon"],
            row["footprint_area_m2"],
            bc,
            nf,
            config,
        ))
    return items


def run_parallel(
    engine: sa.Engine,
    config: dict | None = None,
    limit: int | None = None,
    skip_write: bool = False,
    workers: int | None = None,
    batch_size: int = 1000,
    full_refresh: bool = False,
) -> int:
    config = config or load_config()
    pred_cfg = config["predictions"]

    ensure_table(engine, pred_cfg["schema"], pred_cfg["table"])

    if full_refresh:
        logger.info("Full refresh: truncating %s.%s", pred_cfg["schema"], pred_cfg["table"])
        truncate_estimates(engine, pred_cfg["schema"], pred_cfg["table"])

    bld = config["buildings"]
    require_detection = bld.get("require_detection", True)
    buildings = load_eligible_buildings(
        engine,
        bld["suitability"]["schema"],
        bld["suitability"]["table"],
        bld["detections"]["schema"],
        bld["detections"]["table"],
        require_detection=require_detection,
    )

    if buildings.empty:
        logger.warning("No eligible buildings found")
        return 0

    total = len(buildings)
    logger.info("Found %d eligible buildings", total)

    done = already_processed(engine, pred_cfg["schema"], pred_cfg["table"])
    if done:
        buildings = buildings[~buildings["building_id"].isin(done)]
        logger.info("Skipping %d already-processed, %d remaining", total - len(buildings), len(buildings))

    if limit:
        buildings = buildings.head(limit)

    remaining = len(buildings)
    if remaining == 0:
        logger.info("All buildings already processed")
        return 0

    logger.info("Processing %d buildings with %s workers, batch_size=%d",
                remaining, workers or "auto", batch_size)

    work_items = _prepare_work_items(buildings, config)
    workers = workers or min(os.cpu_count() or 4, 8)

    written = 0
    errors = 0
    batch = []

    with ProcessPoolExecutor(max_workers=workers) as pool:
        for i, result in enumerate(pool.map(_compute_one, work_items, chunksize=64)):
            if result is not None:
                batch.append(result)
            else:
                errors += 1

            if len(batch) >= batch_size:
                if not skip_write:
                    df = pd.DataFrame(batch)
                    load_estimates(engine, df, pred_cfg["schema"], pred_cfg["table"])
                written += len(batch)
                logger.info(
                    "Progress: %d/%d (%.0f%%) — %d written, %d errors",
                    i + 1, remaining, 100 * (i + 1) / remaining, written, errors,
                )
                batch = []

    if batch:
        if not skip_write:
            df = pd.DataFrame(batch)
            load_estimates(engine, df, pred_cfg["schema"], pred_cfg["table"])
        written += len(batch)

    logger.info("Done: %d written, %d errors out of %d", written, errors, remaining)
    return written


async def run(
    engine: sa.Engine,
    config: dict | None = None,
    limit: int | None = None,
    skip_write: bool = False,
) -> list[dict]:
    """Sequential async version (used by Prefect flow)."""
    config = config or load_config()
    pred_cfg = config["predictions"]

    ensure_table(engine, pred_cfg["schema"], pred_cfg["table"])

    bld = config["buildings"]
    require_detection = bld.get("require_detection", True)
    buildings = load_eligible_buildings(
        engine,
        bld["suitability"]["schema"],
        bld["suitability"]["table"],
        bld["detections"]["schema"],
        bld["detections"]["table"],
        require_detection=require_detection,
    )

    if buildings.empty:
        logger.warning("No eligible buildings found")
        return []

    total = len(buildings)
    logger.info("Found %d eligible buildings", total)

    done = already_processed(engine, pred_cfg["schema"], pred_cfg["table"])
    if done:
        buildings = buildings[~buildings["building_id"].isin(done)]
        logger.info("Skipping %d already-processed, %d remaining", total - len(buildings), len(buildings))

    if limit:
        buildings = buildings.head(limit)
        logger.info("Limiting to %d buildings", len(buildings))

    results = []
    errors = 0

    for i, (_, row) in enumerate(buildings.iterrows()):
        try:
            estimate = await estimate_building(row, config)
            if estimate:
                results.append(estimate)
            else:
                errors += 1
        except Exception as e:
            logger.error("%s: FAILED %s", row["building_id"], e, exc_info=True)
            errors += 1

    logger.info("Done: %d estimated, %d errors out of %d", len(results), errors, len(buildings))

    if results and not skip_write:
        df = pd.DataFrame(results)
        rows = load_estimates(engine, df, pred_cfg["schema"], pred_cfg["table"])
        logger.info("Wrote %d rows to %s.%s", rows, pred_cfg["schema"], pred_cfg["table"])

    return results


def main():
    import argparse

    parser = argparse.ArgumentParser(description="PV ROI estimator")
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", default="15432")
    parser.add_argument("--user", default="postgres")
    parser.add_argument("--password", default="securepassword123")
    parser.add_argument("--dbname", default="datasets")
    parser.add_argument("--limit", type=int, default=None, help="Max buildings to process")
    parser.add_argument("--dry-run", action="store_true", help="Don't write to DB")
    parser.add_argument("--workers", type=int, default=None, help="Parallel workers (default: auto)")
    parser.add_argument("--batch-size", type=int, default=1000, help="DB write batch size")
    parser.add_argument("--sequential", action="store_true", help="Use sequential async mode")
    parser.add_argument("--full-refresh", action="store_true", help="Truncate and recompute all")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        stream=sys.stderr,
    )

    engine = get_engine(args.host, args.port, args.user, args.password, args.dbname)

    if args.sequential:
        results = asyncio.run(run(engine, limit=args.limit, skip_write=args.dry_run))
        if results:
            df = pd.DataFrame(results)
            print(df.to_string(index=False))
        else:
            print("No results")
    else:
        written = run_parallel(
            engine,
            limit=args.limit,
            skip_write=args.dry_run,
            workers=args.workers,
            batch_size=args.batch_size,
            full_refresh=args.full_refresh,
        )
        print(f"{written} buildings estimated")


if __name__ == "__main__":
    main()
