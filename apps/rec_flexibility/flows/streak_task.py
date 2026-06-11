"""Prefect task: update streak state weekly based on bonus events."""

from __future__ import annotations

import logging
import os
import sys
from pathlib import Path
from typing import Any

import pandas as pd
from prefect import task
from sqlalchemy import create_engine, text

from celine.utils.pipelines.pipeline import PipelineConfig

_APP_DIR = Path(__file__).resolve().parent.parent
if str(_APP_DIR) not in sys.path:
    sys.path.insert(0, str(_APP_DIR))

from lib import streaks as st  # noqa: E402
from lib.config import get_active_devices, load_config  # noqa: E402

logger = logging.getLogger(__name__)

RAW_TABLE = "_rec_device_streaks_raw"
GOLD_SCHEMA = os.environ.get("CELINE_GOLD_SCHEMA", "ds_dev_gold")


def _build_db_url(cfg: dict[str, Any]) -> str:
    """Build DB URL from PipelineConfig flat keys (POSTGRES_HOST, etc.)."""
    return (
        f"postgresql+psycopg://{cfg.get('postgres_user', 'postgres')}:{cfg.get('postgres_password', '')}"
        f"@{cfg.get('postgres_host', 'localhost')}:{cfg.get('postgres_port', 15432)}/{cfg.get('postgres_db', 'datasets')}"
    )


def _load_previous_state(engine) -> st.StreakState:
    sql = text(f"select device_id, level, peak from {GOLD_SCHEMA}.{RAW_TABLE}")
    try:
        with engine.connect() as conn:
            df = pd.read_sql(sql, conn)
    except Exception:
        logger.info("Streak table does not yet exist; starting from empty state.")
        return {}
    return st.state_from_dataframe(df)


def _detect_responses(
    engine, week_start: pd.Timestamp, week_end: pd.Timestamp
) -> dict[str, bool]:
    """Mark a device as having responded if it earned any bonus points in the week."""
    sql = text(
        f"""
        select device_id, sum(bonus_points) as total_bonus
        from {GOLD_SCHEMA}.rec_flexibility_bonus
        where window_start >= :start and window_start < :end
        group by device_id
        """
    )
    try:
        with engine.connect() as conn:
            df = pd.read_sql(sql, conn, params={"start": week_start, "end": week_end})
    except Exception:
        logger.info("rec_flexibility_bonus does not yet exist; no responses detected.")
        return {}
    return {row["device_id"]: row["total_bonus"] > 0 for _, row in df.iterrows()}


@task(name="Update Streaks", retries=2, retry_delay_seconds=60)
def update_streaks_task(cfg: PipelineConfig) -> int:
    """Update streak state for the prior week.

    Reads previous state from ``_rec_device_streaks_raw``, detects which devices
    earned bonus in the past 7 days, applies one period of streak update, writes
    back. Returns the number of device rows written.
    """
    yaml_cfg = load_config()
    streak_cfg = yaml_cfg["flexibility_bonus"]["streak"]
    active_devices = get_active_devices(yaml_cfg) or None

    engine = create_engine(_build_db_url(cfg.model_dump()))

    now = pd.Timestamp.now(tz="UTC").normalize()
    week_start = now - pd.Timedelta(days=7)
    week_end = now

    prev_state = _load_previous_state(engine)
    responses = _detect_responses(engine, week_start, week_end)

    max_level = int(
        round(
            (streak_cfg["max_multiplier"] - 1.0) / streak_cfg["increment_per_response"]
        )
    )
    new_state = st.update_streaks(
        prev_levels=prev_state,
        responses=responses,
        increment=1,
        decay_per_period=streak_cfg["decay_per_period"],
        max_level=max_level,
        floor_fraction=streak_cfg["floor_fraction"],
        devices=active_devices,
    )

    out_df = st.state_to_dataframe(new_state, now)
    with engine.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {GOLD_SCHEMA}"))
        conn.execute(
            text(
                f"""
                CREATE TABLE IF NOT EXISTS {GOLD_SCHEMA}.{RAW_TABLE} (
                    device_id text primary key,
                    level int not null,
                    peak int not null,
                    multiplier float not null,
                    computed_at timestamp not null
                )
                """
            )
        )
        conn.execute(text(f"DELETE FROM {GOLD_SCHEMA}.{RAW_TABLE}"))
        if not out_df.empty:
            out_df.to_sql(RAW_TABLE, conn, schema=GOLD_SCHEMA, if_exists="append", index=False)

    logger.info("Wrote %d streak rows.", len(out_df))
    return len(out_df)
