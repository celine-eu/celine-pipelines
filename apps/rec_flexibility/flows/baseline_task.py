"""Prefect task: compute settlement + reference baselines and write to gold."""

from __future__ import annotations

import logging
import os
import sys
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from prefect import task
from sqlalchemy import create_engine, text

from celine.utils.pipelines.pipeline import PipelineConfig

_APP_DIR = Path(__file__).resolve().parent.parent
if str(_APP_DIR) not in sys.path:
    sys.path.insert(0, str(_APP_DIR))

from lib import baselines as bl  # noqa: E402
from lib import meters as mt  # noqa: E402
from lib.config import get_active_devices, load_config  # noqa: E402

logger = logging.getLogger(__name__)

RAW_TABLE = "_rec_device_baselines_raw"
GOLD_SCHEMA = os.environ.get("CELINE_GOLD_SCHEMA", "ds_dev_gold")


def _build_db_url(cfg: dict[str, Any]) -> str:
    """Build DB URL from PipelineConfig flat keys (POSTGRES_HOST, etc.)."""
    return (
        f"postgresql+psycopg://{cfg.get('postgres_user', 'postgres')}:{cfg.get('postgres_password', '')}"
        f"@{cfg.get('postgres_host', 'localhost')}:{cfg.get('postgres_port', 15432)}/{cfg.get('postgres_db', 'datasets')}"
    )


def _prepare_history(
    engine, lookback_days: int, devices: list[str] | None = None
) -> pd.DataFrame:
    """rec_meters_15m (over gold meters_data_15m) -> time features -> renamed bases.

    Returns a per-(device, ts) frame carrying every basis the v2 settlement needs.
    All columns already arrive as kWh per 15-min bucket — no unit conversion:

    - ``grid_import_kwh``       = consumption_kwh (energy drawn from grid)
    - ``grid_export_kwh``       = production_kwh (energy fed to grid)
    - ``total_consumption_kwh`` = behind-meter total (grid import + self-consumed PV)
    - ``pv_production_kwh``     = gross PV (used only for M1-only detection)
    """
    merged = mt.load_meters(engine, lookback_days=lookback_days, devices=devices)
    merged = mt.add_time_features(merged)
    merged = merged.rename(
        columns={
            "consumption_kwh": "grid_import_kwh",
            "production_kwh": "grid_export_kwh",
        }
    )
    return merged


def _identify_m1_only(history: pd.DataFrame) -> set[str]:
    """Return device_ids that never report behind-meter PV (max pv_production == 0).

    SQL parity: ``rec_device_class.is_m1_only`` / gamification v2
    ``identify_m1_only_devices``. These devices use the consumption proxy.
    """
    by_dev = history.groupby("device_id")["pv_production_kwh"].max()
    return set(by_dev[by_dev == 0.0].index)


def _grid_export_median_frame(
    history_reference: pd.DataFrame, m1_only: set[str]
) -> pd.DataFrame:
    """Per (device, slot, is_weekday) median grid-export kWh for M1-only devices.

    This is the export reference the proxy subtracts against. Computed over the
    (longer) reference window for stability and persisted so dbt can rebuild the
    per-interval proxy at settlement time.
    """
    rows: list[dict[str, Any]] = []
    if not m1_only:
        return pd.DataFrame(columns=["device_id", "slot", "is_weekday", "ge_median_kwh"])
    scoped = history_reference[history_reference["device_id"].isin(m1_only)]
    for device_id, df_dev in scoped.groupby("device_id"):
        med = bl.compute_median_baseline(df_dev, value_col="grid_export_kwh")
        for (slot, is_wkday), val in med.items():
            rows.append(
                {
                    "device_id": device_id,
                    "slot": slot,
                    "is_weekday": is_wkday,
                    "ge_median_kwh": val,
                }
            )
    return pd.DataFrame(
        rows, columns=["device_id", "slot", "is_weekday", "ge_median_kwh"]
    )


def _apply_consumption_basis(
    history: pd.DataFrame, m1_only: set[str], ge_med: pd.DataFrame
) -> pd.DataFrame:
    """Set ``consumption_kwh``: total (M1+M2) or proxy (M1-only).

    For M1+M2 devices the basis is behind-meter ``total_consumption_kwh`` (v1 parity).
    For M1-only devices it is the proxy ``grid_import + max(0, ge_median - grid_export)``
    using the persisted median export reference per (slot, is_weekday).
    """
    history = history.copy()
    history["consumption_kwh"] = history["total_consumption_kwh"]
    if not m1_only:
        return history

    mask = history["device_id"].isin(m1_only)
    sub = history[mask]
    if ge_med.empty:
        ge_base = np.zeros(len(sub), dtype=float)
    else:
        lookup = ge_med.set_index(["device_id", "slot", "is_weekday"])["ge_median_kwh"]
        keys = list(
            zip(
                sub["device_id"],
                sub["slot"].astype(int),
                sub["is_weekday"].astype(bool),
            )
        )
        ge_base = lookup.reindex(keys).fillna(0.0).to_numpy(dtype=float)

    proxy = bl.compute_m1_only_consumption_proxy(
        sub["grid_import_kwh"].to_numpy(dtype=float),
        sub["grid_export_kwh"].to_numpy(dtype=float),
        ge_base,
    )
    history.loc[mask, "consumption_kwh"] = proxy
    return history


@task(name="Compute Baselines", retries=2, retry_delay_seconds=60)
def compute_baselines_task(cfg: PipelineConfig) -> int:
    """Compute settlement (rolling) and reference (winsorized) baselines.

    Writes to ``{CELINE_GOLD_SCHEMA}._rec_device_baselines_raw``.
    """
    yaml_cfg = load_config()
    bl_cfg = yaml_cfg["baseline"]
    ref_cfg = bl_cfg["bonus_reference"]
    active_devices = get_active_devices(yaml_cfg) or None

    engine = create_engine(_build_db_url(cfg.model_dump()))

    today_utc = pd.Timestamp.now(tz="UTC").normalize()

    history_settlement = _prepare_history(
        engine, lookback_days=bl_cfg["candidate_days"], devices=active_devices
    )
    history_reference = _prepare_history(
        engine, lookback_days=ref_cfg["lookback_days"], devices=active_devices
    )

    # v2: M1-only devices use the consumption proxy; everyone else uses behind-meter
    # total. M1-only detection runs over the long reference window (a device must
    # *never* report PV to qualify).
    m1_only = _identify_m1_only(history_reference)
    ge_med = _grid_export_median_frame(history_reference, m1_only)
    history_settlement = _apply_consumption_basis(history_settlement, m1_only, ge_med)
    history_reference = _apply_consumption_basis(history_reference, m1_only, ge_med)
    logger.info(
        "Fleet=%s devices, M1-only=%s: %s",
        len(active_devices) if active_devices else "all",
        len(m1_only),
        sorted(m1_only),
    )

    frames = []
    for device_id, df_dev in history_settlement.groupby("device_id"):
        settlement = bl.compute_settlement_baseline(
            df_dev,
            select=bl_cfg["select_days"],
            candidates=bl_cfg["candidate_days"],
            min_readings=bl_cfg["min_readings_per_day"],
        )
        frames.append(bl.baselines_to_dataframe(settlement, device_id, "settlement", today_utc))

    for device_id, df_dev in history_reference.groupby("device_id"):
        reference = bl.compute_winsorized_reference_baseline(
            df_dev,
            select=bl_cfg["select_days"],
            candidates=bl_cfg["candidate_days"],
            min_readings=bl_cfg["min_readings_per_day"],
            winsorize_pct=ref_cfg["winsorize_pct"],
        )
        frames.append(bl.baselines_to_dataframe(reference, device_id, "reference", today_utc))

    # v2: persist the median grid-export reference for M1-only devices so the dbt
    # settlement model can rebuild the per-interval consumption proxy.
    for device_id, df_dev in ge_med.groupby("device_id"):
        ge_dict = {
            (int(r.slot), bool(r.is_weekday)): float(r.ge_median_kwh)
            for r in df_dev.itertuples()
        }
        frames.append(
            bl.baselines_to_dataframe(ge_dict, device_id, "grid_export_median", today_utc)
        )

    if not frames:
        logger.warning("No baseline rows computed.")
        return 0

    out_df = pd.concat(frames, ignore_index=True)

    with engine.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {GOLD_SCHEMA}"))
        conn.execute(
            text(
                f"""
                CREATE TABLE IF NOT EXISTS {GOLD_SCHEMA}.{RAW_TABLE} (
                    device_id text not null,
                    baseline_type text not null,
                    slot int not null,
                    is_weekday bool not null,
                    baseline_kwh float not null,
                    computed_at timestamp not null,
                    primary key (device_id, baseline_type, slot, is_weekday)
                )
                """
            )
        )
        conn.execute(text(f"DELETE FROM {GOLD_SCHEMA}.{RAW_TABLE}"))
        out_df.to_sql(RAW_TABLE, conn, schema=GOLD_SCHEMA, if_exists="append", index=False)

    logger.info("Wrote %d baseline rows.", len(out_df))
    return len(out_df)
