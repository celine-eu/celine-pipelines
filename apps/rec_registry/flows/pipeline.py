"""
REC Registry mirror pipeline.

Fetches all registered RECs from the CELINE REC Registry API via the SDK,
flattens member/sensor data, and writes it into raw.rec_registry_mirror —
a full-replace table (TRUNCATE + INSERT in one transaction) that dbt pipelines
can use as a stable source of truth for community membership and asset metadata.

One row per member (user_id PK).  sensor_ids, delivery_point_ids, and
topology_ids are stored as Postgres text[] arrays.

Schedule: every 5 minutes.
"""

import asyncio
import logging
import os
from pathlib import Path
from typing import Any

import psycopg2
import psycopg2.extras
import yaml
from prefect import flow, task

from celine.sdk.auth import OidcClientCredentialsProvider
from celine.sdk.rec_registry.client import RecRegistryAdminClient
from celine.utils.pipelines.pipeline import (
    DEV_MODE,
    PipelineConfig,
    PipelineTaskResult,
    PipelineStatus,
)

logger = logging.getLogger(__name__)

os.environ.setdefault("APP_NAME", "rec_registry")

script_dir = Path(__file__).parent

_DDL = """
CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.rec_registry_mirror (
    user_id             text        PRIMARY KEY,
    rec_id              text        NOT NULL,
    area                text,
    topology_ids        text[]      NOT NULL DEFAULT '{}',
    delivery_point_ids  text[]      NOT NULL DEFAULT '{}',
    sensor_ids          text[]      NOT NULL DEFAULT '{}',
    last_updated        timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_rec_registry_mirror_rec_id
    ON raw.rec_registry_mirror (rec_id);

CREATE INDEX IF NOT EXISTS ix_rec_registry_mirror_area
    ON raw.rec_registry_mirror (area);

CREATE INDEX IF NOT EXISTS ix_rec_registry_mirror_sensor_ids
    ON raw.rec_registry_mirror USING gin (sensor_ids);

CREATE INDEX IF NOT EXISTS ix_rec_registry_mirror_delivery_point_ids
    ON raw.rec_registry_mirror USING gin (delivery_point_ids);
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


def _registry_url() -> str:
    return os.getenv("CELINE_REC_REGISTRY_URL", "http://host.docker.internal:8004")


async def _fetch_yaml(cfg: PipelineConfig) -> str:
    """Obtain a token via OIDC client credentials and export all RECs as YAML."""
    oidc = cfg.sdk.oidc
    provider = OidcClientCredentialsProvider(
        base_url=oidc.base_url,
        client_id=oidc.client_id,
        client_secret=oidc.client_secret,
        scope=oidc.scope,
        verify_ssl=oidc.verify_ssl,
    )
    client = RecRegistryAdminClient(
        base_url=_registry_url(),
        token_provider=provider,
    )
    return await client.export_communities()  # all communities, multidoc YAML


def _parse_bundles(yaml_text: str) -> list[dict[str, Any]]:
    """Parse multidocument YAML into a list of bundle dicts."""
    docs = [d for d in yaml.safe_load_all(yaml_text) if d]
    if not docs:
        raise ValueError("REC Registry returned empty export")
    return docs


def _flatten_to_rows(bundles: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """
    Flatten community bundles into one row per member.

    Columns produced:
      user_id, rec_id, area, topology_ids, delivery_point_ids, sensor_ids
    """
    rows: list[dict[str, Any]] = []

    for bundle in bundles:
        community = bundle.get("community", {})
        rec_id = community.get("id")
        if not rec_id:
            logger.warning("Bundle has no community.id — skipping")
            continue

        areas: dict[str, Any] = community.get("areas", {})
        members: dict[str, Any] = bundle.get("members", {})

        for _member_key, member in members.items():
            user_id = member.get("user_id")
            if not user_id:
                logger.warning("Member in %s has no user_id — skipping", rec_id)
                continue

            area_key: str | None = member.get("area")
            area_data: dict = areas.get(area_key, {}) if area_key else {}
            topology_ids: list[str] = area_data.get("topology", [])

            delivery_point_ids: list[str] = [
                dp["id"] for dp in member.get("delivery_points", []) if dp.get("id")
            ]

            meter_assets: dict = member.get("assets", {}).get("meter", {})
            sensor_ids: list[str] = [
                m["sensor_id"] for m in meter_assets.values() if m.get("sensor_id")
            ]

            rows.append(
                {
                    "user_id": user_id,
                    "rec_id": rec_id,
                    "area": area_key,
                    "topology_ids": topology_ids,
                    "delivery_point_ids": delivery_point_ids,
                    "sensor_ids": sensor_ids,
                }
            )

    return rows


# ---------------------------------------------------------------------------
# Prefect tasks
# ---------------------------------------------------------------------------


@task(name="Ensure raw table", retries=2, retry_delay_seconds=30)
def ensure_table(cfg: PipelineConfig) -> PipelineTaskResult:
    """Create raw.rec_registry_mirror and its indexes if they don't exist."""
    with _db_conn(cfg) as conn:
        with conn.cursor() as cur:
            cur.execute(_DDL)
        conn.commit()

    logger.info("raw.rec_registry_mirror: DDL applied")
    return PipelineTaskResult(command="ensure_table", status=PipelineStatus.COMPLETED)


@task(name="Fetch REC registry", retries=3, retry_delay_seconds=60)
def fetch_registry(
    cfg: PipelineConfig,
) -> tuple[list[dict[str, Any]], PipelineTaskResult]:
    """
    Export all REC bundles from the registry API and flatten to row dicts.

    Uses OIDC client credentials from PipelineConfig.sdk.oidc.
    """
    yaml_text = asyncio.run(_fetch_yaml(cfg))
    bundles = _parse_bundles(yaml_text)
    rows = _flatten_to_rows(bundles)
    logger.info(
        "Fetched %d bundle(s), %d member rows from REC Registry",
        len(bundles),
        len(rows),
    )
    return rows, PipelineTaskResult(
        command="fetch_registry",
        status=PipelineStatus.COMPLETED,
        details=f"Processed {len(rows)}",
    )


@task(name="Mirror to raw table", retries=2, retry_delay_seconds=30)
def mirror_to_db(rows: list[dict[str, Any]], cfg: PipelineConfig) -> PipelineTaskResult:
    """
    Atomically replace raw.rec_registry_mirror:
    TRUNCATE + bulk INSERT in a single transaction.
    """
    if not rows:
        logger.warning("No rows to insert — skipping mirror (registry may be empty)")
        return PipelineTaskResult(
            command="mirror_to_db",
            status=PipelineStatus.COMPLETED,
            details={"rows_inserted": 0},
        )

    tuples = [
        (
            r["user_id"],
            r["rec_id"],
            r["area"],
            r["topology_ids"],
            r["delivery_point_ids"],
            r["sensor_ids"],
        )
        for r in rows
    ]

    with _db_conn(cfg) as conn:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE raw.rec_registry_mirror")
            psycopg2.extras.execute_values(
                cur,
                """
                INSERT INTO raw.rec_registry_mirror
                    (user_id, rec_id, area, topology_ids, delivery_point_ids, sensor_ids, last_updated)
                VALUES %s
                """,
                tuples,
                template="(%s, %s, %s, %s, %s, %s, now())",
                page_size=500,
            )
        conn.commit()

    logger.info("Mirrored %d rows into raw.rec_registry_mirror", len(tuples))
    return PipelineTaskResult(
        command="mirror_to_db",
        status=PipelineStatus.COMPLETED,
        details={"rows_inserted": len(tuples)},
    )


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------


@flow(name="rec-registry-flow")
def rec_registry_flow(config: dict[str, Any] | None = None) -> dict:
    """
    Full REC Registry mirror pipeline:
      1. Ensure raw table + indexes exist
      2. Fetch all RECs from the registry API
      3. Truncate + re-insert into raw.rec_registry_mirror
    """
    cfg = PipelineConfig.model_validate(config or {})

    result: dict = {"status": "success"}
    result["ensure_table"] = ensure_table(cfg)
    rows, fetch_registry_result = fetch_registry(cfg)
    result["fetch_registry"] = fetch_registry_result
    result["mirror"] = mirror_to_db(rows, cfg)

    return result


if __name__ == "__main__":
    import yaml as _yaml

    config_path = script_dir / "config.yaml"
    with open(config_path) as fh:
        flow_cfg = _yaml.safe_load(fh)

    schedule = flow_cfg["schedule"]

    if DEV_MODE:
        rec_registry_flow.serve(name=schedule["name"], cron=schedule["cron"])
