"""Prefect task: auto-inject commitments for all devices into all active windows.

TEST-PHASE ONLY. Simulates "every user accepts every suggestion" by inserting
synthetic commitments into raw.flexibility_commitments_mirror for every
(device, window) pair. Gated by the AUTO_COMMIT_ENABLED env var (default: false).

Remove this task (and its import in pipeline.py) once the flexibility-api is
deployed and real user commitments flow through the webapp.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Any

import pandas as pd
from prefect import task
from sqlalchemy import create_engine, text

from celine.utils.pipelines.pipeline import PipelineConfig

logger = logging.getLogger(__name__)

AUTO_COMMIT_ENV = "AUTO_COMMIT_ENABLED"
_SILVER_SCHEMA = os.environ.get("CELINE_SILVER_SCHEMA", "ds_dev_silver")
_GOLD_SCHEMA = os.environ.get("CELINE_GOLD_SCHEMA", "ds_dev_gold")


def _build_db_url(cfg: dict[str, Any]) -> str:
    return (
        f"postgresql+psycopg://{cfg.get('postgres_user', 'postgres')}:{cfg.get('postgres_password', '')}"
        f"@{cfg.get('postgres_host', 'localhost')}:{cfg.get('postgres_port', 15432)}/{cfg.get('postgres_db', 'datasets')}"
    )


@task(name="Auto-commit all devices (test phase)")
def auto_commit_task(cfg: PipelineConfig) -> int:
    """Insert commitments for all devices × today's+tomorrow's windows.

    Returns the number of commitments upserted. Skipped entirely when
    AUTO_COMMIT_ENABLED is not set to "true"/"1".
    """
    enabled = os.environ.get(AUTO_COMMIT_ENV, "").lower() in ("true", "1")
    if not enabled:
        logger.info("Auto-commit disabled (%s not set). Skipping.", AUTO_COMMIT_ENV)
        return 0

    engine = create_engine(_build_db_url(cfg.model_dump()))
    now = datetime.now(timezone.utc)
    today = now.date()
    tomorrow = today + timedelta(days=1)

    with engine.connect() as conn:
        devices = pd.read_sql(text(
            f"SELECT DISTINCT device_id FROM {_SILVER_SCHEMA}.meters_data WHERE meter_type = 'M1'"
        ), conn)

        windows = pd.read_sql(text(f"""
            SELECT DISTINCT window_start, window_end
            FROM {_GOLD_SCHEMA}.rec_flexibility_windows
            WHERE ts_date >= :today AND ts_date <= :tomorrow
        """), conn, params={"today": today, "tomorrow": tomorrow})

    if devices.empty or windows.empty:
        logger.info("No devices or windows — nothing to auto-commit.")
        return 0

    rows = []
    for _, win in windows.iterrows():
        for _, dev in devices.iterrows():
            device_id = dev["device_id"]
            ws = win["window_start"]
            we = win["window_end"]
            cid = f"auto-{device_id}-{ws.strftime('%Y%m%d%H%M')}-{we.strftime('%H%M')}"
            rows.append({
                "id": cid,
                "user_id": f"auto-user-{device_id}",
                "suggestion_id": f"auto-sug-{ws.strftime('%Y%m%d%H%M')}-{we.strftime('%H%M')}",
                "suggestion_type": "solar_overproduction",
                "community_id": "gr-renewable-community",
                "device_id": device_id,
                "period_start": ws,
                "period_end": we,
                "committed_at": now,
                "settled_at": None,
                "reminded_at": None,
                "status": "committed",
                "reward_points_estimated": 10,
                "reward_points_actual": None,
                "last_updated": now,
            })

    with engine.begin() as conn:
        conn.execute(text(
            "DELETE FROM raw.flexibility_commitments_mirror WHERE id LIKE 'auto-%'"
        ))
        for row in rows:
            conn.execute(text("""
                INSERT INTO raw.flexibility_commitments_mirror
                (id, user_id, suggestion_id, suggestion_type, community_id,
                 device_id, period_start, period_end, committed_at, settled_at,
                 reminded_at, status, reward_points_estimated, reward_points_actual,
                 last_updated)
                VALUES (:id, :user_id, :suggestion_id, :suggestion_type, :community_id,
                        :device_id, :period_start, :period_end, :committed_at, :settled_at,
                        :reminded_at, :status, :reward_points_estimated, :reward_points_actual,
                        :last_updated)
            """), row)

    logger.info("Auto-committed %d rows (%d devices × %d windows).",
                len(rows), len(devices), len(windows))
    return len(rows)
