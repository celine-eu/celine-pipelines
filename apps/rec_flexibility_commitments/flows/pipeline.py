"""
Flexibility commitments mirror pipeline.

Fetches all flexibility commitments from the CELINE Flexibility API via the SDK
and writes them into raw.flexibility_commitments_mirror — a 90-day sliding window
table that dbt pipelines use for settlement analytics, gamification, and acceptance
rate tracking.

Write strategy: DELETE rows older than 90 days, then upsert the fetched records.
Unlike rec_registry (full-replace), we upsert because commitment status evolves
after creation (committed → settled) and we never want to lose an open commitment.

Schedule: every 15 minutes.
"""

import asyncio
import logging
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import psycopg2
import psycopg2.extras
import yaml
from prefect import flow, task

from celine.sdk.auth import OidcClientCredentialsProvider
from celine.sdk.flexibility.client import FlexibilityAdminClient
from celine.utils.pipelines.pipeline import (
    DEV_MODE,
    PipelineConfig,
    PipelineTaskResult,
    PipelineStatus,
)

logger = logging.getLogger(__name__)

os.environ.setdefault("APP_NAME", "rec_flexibility_commitments")

script_dir = Path(__file__).parent

_WINDOW_DAYS = 90

_DDL = """
CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.flexibility_commitments_mirror (
    id                      text        NOT NULL,
    user_id                 text        NOT NULL,
    suggestion_id           text        NOT NULL,
    suggestion_type         text        NOT NULL,
    community_id            text,
    device_id               text,
    period_start            timestamptz NOT NULL,
    period_end              timestamptz NOT NULL,
    committed_at            timestamptz NOT NULL,
    settled_at              timestamptz,
    reminded_at             timestamptz,
    status                  text        NOT NULL,
    reward_points_estimated int         NOT NULL,
    reward_points_actual    int,
    last_updated            timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (id)
);

CREATE INDEX IF NOT EXISTS ix_flex_mirror_user_id
    ON raw.flexibility_commitments_mirror (user_id);

CREATE INDEX IF NOT EXISTS ix_flex_mirror_device_id
    ON raw.flexibility_commitments_mirror (device_id);

CREATE INDEX IF NOT EXISTS ix_flex_mirror_community_id
    ON raw.flexibility_commitments_mirror (community_id);

CREATE INDEX IF NOT EXISTS ix_flex_mirror_status
    ON raw.flexibility_commitments_mirror (status);

CREATE INDEX IF NOT EXISTS ix_flex_mirror_committed_at
    ON raw.flexibility_commitments_mirror (committed_at);

CREATE INDEX IF NOT EXISTS ix_flex_mirror_period_start
    ON raw.flexibility_commitments_mirror (period_start);
"""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _db_conn(cfg: PipelineConfig):
    return psycopg2.connect(
        host=cfg.postgres_host,
        port=cfg.postgres_port,
        dbname=cfg.postgres_db,
        user=cfg.postgres_user,
        password=cfg.postgres_password,
    )


def _flexibility_url() -> str:
    return os.getenv("CELINE_FLEXIBILITY_API_URL", "http://host.docker.internal:8017")


async def _fetch_commitments(
    cfg: PipelineConfig, created_after: datetime
) -> list[dict[str, Any]]:
    """Fetch all commitments since created_after via OIDC client credentials."""
    oidc = cfg.sdk.oidc
    if not oidc.client_id or not oidc.client_secret:
        raise ValueError(
            "OIDC client_id and client_secret are required "
            "(set CELINE_OIDC_CLIENT_ID / CELINE_OIDC_CLIENT_SECRET)"
        )
    provider = OidcClientCredentialsProvider(
        base_url=oidc.base_url,
        client_id=oidc.client_id,
        client_secret=oidc.client_secret,
        scope=oidc.scope,
        verify_ssl=oidc.verify_ssl,
    )
    client = FlexibilityAdminClient(
        base_url=_flexibility_url(),
        token_provider=provider,
    )
    commitments = await client.export_commitments(created_after=created_after)
    return [c.model_dump() for c in commitments]


def _to_tuples(rows: list[dict[str, Any]]) -> list[tuple]:
    return [
        (
            str(r["id"]),
            r["user_id"],
            r["suggestion_id"],
            str(r["suggestion_type"]),  # generated enum → str
            r.get("community_id"),
            r.get("device_id"),
            r["period_start"],
            r["period_end"],
            r["committed_at"],
            r.get("settled_at"),
            r.get("reminded_at"),
            str(r["status"]),           # generated enum → str
            r["reward_points_estimated"],
            r.get("reward_points_actual"),
        )
        for r in rows
    ]


# ---------------------------------------------------------------------------
# Prefect tasks
# ---------------------------------------------------------------------------


@task(name="Ensure raw table", retries=2, retry_delay_seconds=30)
def ensure_table(cfg: PipelineConfig) -> PipelineTaskResult:
    """Create raw.flexibility_commitments_mirror and its indexes if they don't exist."""
    with _db_conn(cfg) as conn:
        with conn.cursor() as cur:
            cur.execute(_DDL)
        conn.commit()

    logger.info("raw.flexibility_commitments_mirror: DDL applied")
    return PipelineTaskResult(command="ensure_table", status=PipelineStatus.COMPLETED)


@task(name="Fetch and mirror commitments", retries=3, retry_delay_seconds=60)
def mirror_to_db(cfg: PipelineConfig) -> PipelineTaskResult:
    """Fetch commitments from flexibility-api and upsert into the raw mirror.

    1. Fetch all commitments within the 90-day sliding window.
    2. Delete rows older than 90 days (expired window).
    3. Upsert fresh rows — updates status, settled_at, reward_points_actual
       which evolve after commitment creation.
    """
    created_after = datetime.now(timezone.utc) - timedelta(days=_WINDOW_DAYS)
    rows = asyncio.run(_fetch_commitments(cfg, created_after))
    logger.info("Fetched %d commitment rows from flexibility-api", len(rows))
    tuples = _to_tuples(rows)

    with _db_conn(cfg) as conn:
        with conn.cursor() as cur:
            # Prune expired rows
            cur.execute(
                "DELETE FROM raw.flexibility_commitments_mirror "
                "WHERE committed_at < now() - interval '%s days'",
                (_WINDOW_DAYS,),
            )
            deleted = cur.rowcount

            if tuples:
                psycopg2.extras.execute_values(
                    cur,
                    """
                    INSERT INTO raw.flexibility_commitments_mirror (
                        id, user_id, suggestion_id, suggestion_type,
                        community_id, device_id,
                        period_start, period_end,
                        committed_at, settled_at, reminded_at,
                        status, reward_points_estimated, reward_points_actual,
                        last_updated
                    ) VALUES %s
                    ON CONFLICT (id) DO UPDATE SET
                        status                  = EXCLUDED.status,
                        settled_at              = EXCLUDED.settled_at,
                        reminded_at             = EXCLUDED.reminded_at,
                        reward_points_actual    = EXCLUDED.reward_points_actual,
                        last_updated            = now()
                    """,
                    tuples,
                    template=(
                        "(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now())"
                    ),
                    page_size=500,
                )
        conn.commit()

    logger.info(
        "flexibility_commitments_mirror: %d pruned, %d upserted",
        deleted,
        len(tuples),
    )
    return PipelineTaskResult(
        command="mirror_to_db",
        status=PipelineStatus.COMPLETED,
        details={"rows_upserted": len(tuples), "rows_pruned": deleted},
    )


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------


@flow(name="rec-flexibility-commitments-flow")
def rec_flexibility_commitments_flow(config: dict[str, Any] | None = None) -> dict:
    """
    Flexibility commitments mirror pipeline:
      1. Ensure raw table + indexes exist
      2. Fetch all commitments within the 90-day window from flexibility-api
      3. Prune expired rows + upsert fresh rows into raw.flexibility_commitments_mirror
    """
    cfg = PipelineConfig.model_validate(config or {})

    result: dict = {"status": "success"}
    result["ensure_table"] = ensure_table(cfg)
    result["mirror"] = mirror_to_db(cfg)

    return result


if __name__ == "__main__":
    import yaml as _yaml

    config_path = script_dir / "config.yaml"
    with open(config_path) as fh:
        flow_cfg = _yaml.safe_load(fh)

    schedule = flow_cfg["schedule"]

    if DEV_MODE:
        rec_flexibility_commitments_flow.serve(
            name=schedule["name"], cron=schedule["cron"]
        )
