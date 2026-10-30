"""
Flexibility commitments mirror pipeline.

Fetches the flexibility commitments of the last 90 days from the CELINE Flexibility
API (`GET /api/commitments/export?created_after=<now - 90 days>`) via the SDK and
writes them into raw.flexibility_commitments_mirror — a 90-day sliding window table
that dbt pipelines use for settlement analytics, gamification, and acceptance rate
tracking.

Write strategy: the mirror equals the export's window.  Each run deletes every row
and inserts the fetched ones, in one transaction (as rec_registry,
celine-eu/celine-pipelines#7): the window is replaced, so a commitment the export
no longer lists leaves the mirror, and anything older than the window is pruned
with it.  An empty export empties the mirror.  The fetched rows carry the current
status (committed → settled), so nothing is lost by not upserting.

The same transaction upserts this mirror's row in raw.mirror_refresh (mirror,
refreshed_at, rows): last_updated cannot tell a stopped flow from an empty export,
the marker can.  rec_flexibility's test `flexibility_commitments_mirror_is_fresh`
reads it.

A fetch that fails raises before the mirror is touched, and the previous rows and
marker stay: the SDK client raises on any status but 200 and on a body that is not
a list (held by tests/test_a_failed_export_raises.py), and the replace runs in its
own task on the fetched rows only.

Schedule: every 15 minutes.
"""

import asyncio
import logging
import os
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import psycopg2
import psycopg2.extras
from celine.sdk.auth import OidcClientCredentialsProvider, TokenProvider
from celine.sdk.flexibility.client import FlexibilityAdminClient
from celine.utils.pipelines.pipeline import (
    DEV_MODE,
    PipelineConfig,
    PipelineStatus,
    PipelineTaskResult,
)
from prefect import flow, task

logger = logging.getLogger(__name__)

os.environ.setdefault("APP_NAME", "rec_flexibility_commitments")

script_dir = Path(__file__).parent

_WINDOW_DAYS = 90

# This mirror's key in raw.mirror_refresh.
_MIRROR = "flexibility_commitments_mirror"

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

-- One row per mirror: when its last run replaced it, and with how many rows.
-- Shared by the raw mirrors; each flow upserts its own row.
CREATE TABLE IF NOT EXISTS raw.mirror_refresh (
    mirror          text        PRIMARY KEY,
    refreshed_at    timestamptz NOT NULL,
    rows            int         NOT NULL
);
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


async def _export_commitments(
    base_url: str, token_provider: TokenProvider, created_after: datetime
) -> list[dict[str, Any]]:
    """GET /api/commitments/export since created_after, as row dicts.

    A refused or failed request, or a body that is not a list, raises: the SDK
    client raises on any status but 200, so the mirror is left as it was.
    """
    client = FlexibilityAdminClient(base_url=base_url, token_provider=token_provider)
    commitments = await client.export_commitments(created_after=created_after)
    return [c.model_dump() for c in commitments]


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
    return await _export_commitments(_flexibility_url(), provider, created_after)


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
            str(r["status"]),  # generated enum → str
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
    """Create raw.flexibility_commitments_mirror, its indexes and raw.mirror_refresh
    if they don't exist."""
    with _db_conn(cfg) as conn:
        with conn.cursor() as cur:
            cur.execute(_DDL)
        conn.commit()

    logger.info("raw.flexibility_commitments_mirror: DDL applied")
    return PipelineTaskResult(command="ensure_table", status=PipelineStatus.COMPLETED)


@task(name="Fetch commitments", retries=3, retry_delay_seconds=60)
def fetch_commitments(cfg: PipelineConfig) -> list[dict[str, Any]]:
    """Fetch every commitment of the 90-day window from flexibility-api.

    Raises on a failed export, so the replace below never runs on it.
    """
    created_after = datetime.now(UTC) - timedelta(days=_WINDOW_DAYS)
    rows = asyncio.run(_fetch_commitments(cfg, created_after))
    logger.info("Fetched %d commitment rows from flexibility-api", len(rows))
    return rows


@task(name="Mirror to raw table", retries=2, retry_delay_seconds=30)
def mirror_to_db(rows: list[dict[str, Any]], cfg: PipelineConfig) -> PipelineTaskResult:
    """
    Atomically replace raw.flexibility_commitments_mirror with the fetched rows
    and record the run in raw.mirror_refresh: DELETE + bulk INSERT + marker
    upsert in a single transaction.

    **An empty row list still replaces the table**: the export lists every
    commitment of the window, so one it no longer lists is gone upstream.  The
    DELETE has no WHERE: rows in the window are replaced, older ones pruned.
    """
    tuples = _to_tuples(rows)

    with _db_conn(cfg) as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM raw.flexibility_commitments_mirror")
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
                    """,
                    tuples,
                    template=(
                        "(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now())"
                    ),
                    page_size=500,
                )

            cur.execute(
                """
                INSERT INTO raw.mirror_refresh (mirror, refreshed_at, rows)
                VALUES (%s, now(), %s)
                ON CONFLICT (mirror) DO UPDATE SET
                    refreshed_at = EXCLUDED.refreshed_at,
                    rows         = EXCLUDED.rows
                """,
                (_MIRROR, len(tuples)),
            )
        conn.commit()

    logger.info(
        "flexibility_commitments_mirror: %d deleted, %d inserted",
        deleted,
        len(tuples),
    )
    return PipelineTaskResult(
        command="mirror_to_db",
        status=PipelineStatus.COMPLETED,
        details={"rows_inserted": len(tuples)},
    )


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------


@flow(name="rec-flexibility-commitments-flow")
def rec_flexibility_commitments_flow(config: dict[str, Any] | None = None) -> dict:
    """
    Flexibility commitments mirror pipeline:
      1. Ensure raw tables + indexes exist
      2. Fetch all commitments within the 90-day window from flexibility-api
      3. Replace raw.flexibility_commitments_mirror with them and mark the run
         in raw.mirror_refresh, in one transaction
    """
    cfg = PipelineConfig.model_validate(config or {})

    result: dict = {"status": "success"}
    result["ensure_table"] = ensure_table(cfg)
    rows = fetch_commitments(cfg)
    result["mirror"] = mirror_to_db(rows, cfg)

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
