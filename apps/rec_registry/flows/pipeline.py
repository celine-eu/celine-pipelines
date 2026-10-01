"""
REC Registry mirror pipeline.

Fetches all registered RECs from the CELINE REC Registry API via the SDK,
flattens member/sensor data, and writes it into raw.rec_registry_mirror —
a full-replace table (TRUNCATE + INSERT in one transaction) that dbt pipelines
can use as a stable source of truth for community membership and asset metadata.

One row per active member (user_id PK).  sensor_ids, delivery_point_ids, and
topology_ids are stored as Postgres text[] arrays.  Members with status other
than 'active' are excluded, and delivery_point_ids lists only the points in
service: a delivery point flagged `active: false` is left out.

boundary_id carries the id of the member's area's boundary (registry schema
v0.7: `area.boundary.id`, the area's GSE primary-substation `cod_ac`); it is
null for an area without a boundary.  rec_it takes topology_ids[1] as the
member's substation_id, which is right only when the area lists exactly one
node, equal to boundary_id.  The flow flags every area breaking that (it does
not refuse the export), and rec_it's singular test
`rec_registry_mirror_substation_is_area_boundary` fails on it.

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
    user_id             text        NOT NULL,
    rec_id              text        NOT NULL,
    area                text,
    role                text,
    member_type         text,
    topology_ids        text[]      NOT NULL DEFAULT '{}',
    delivery_point_ids  text[]      NOT NULL DEFAULT '{}',
    sensor_ids          text[]      NOT NULL DEFAULT '{}',
    last_updated        timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (user_id, rec_id)
);

-- Added after the table was first deployed: tables created earlier get it here.
ALTER TABLE raw.rec_registry_mirror ADD COLUMN IF NOT EXISTS boundary_id text;

CREATE INDEX IF NOT EXISTS ix_rec_registry_mirror_rec_id
    ON raw.rec_registry_mirror (rec_id);

CREATE INDEX IF NOT EXISTS ix_rec_registry_mirror_area
    ON raw.rec_registry_mirror (area);

CREATE INDEX IF NOT EXISTS ix_rec_registry_mirror_role
    ON raw.rec_registry_mirror (role);

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


def _area_boundary_id(area_data: Any) -> str | None:
    """The id of an area's boundary, or None when the area has none.

    A boundary is `{source, id}` (registry schema v0.7).  Anything else — no
    boundary, a null one, a malformed one, a blank id — is treated as absent,
    so an area exported before v0.7 mirrors with a null boundary_id.
    """
    if not isinstance(area_data, dict):
        return None
    boundary = area_data.get("boundary")
    if not isinstance(boundary, dict):
        return None
    boundary_id = boundary.get("id")
    if not isinstance(boundary_id, str) or not boundary_id.strip():
        return None
    return boundary_id


def _live_delivery_point_ids(points: Any) -> list[str]:
    """The ids of a member's delivery points that are in service.

    A point with `active: false` is a supply point no longer in service and is
    left out: `rec_it`'s `rec_member_supply_points` unnests these ids into the
    list a community hands its distributor, and a retired POD there asks the
    distributor to release readings against a dead supply point.  The flag
    defaults to true in the registry model, so a point without it is live.
    """
    return [
        dp["id"]
        for dp in points or []
        if dp.get("id") and dp.get("active") is not False
    ]


def _flatten_to_rows(bundles: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """
    Flatten community bundles into one row per active member.

    Members whose status is not 'active' are skipped, and so are delivery
    points flagged `active: false` (the member keeps its row).

    Columns produced:
      user_id, rec_id, area, role, member_type,
      topology_ids, delivery_point_ids, sensor_ids, boundary_id
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

        for member_key, member in members.items():
            status = member.get("status")
            if status != "active":
                logger.debug(
                    "Skipping member %s in %s (status=%s)", member_key, rec_id, status
                )
                continue

            user_id = member.get("user_id")
            if not user_id:
                logger.warning(
                    "Member %s in %s has no user_id — skipping", member_key, rec_id
                )
                continue

            area_key: str | None = member.get("area")
            area_data: dict = areas.get(area_key, {}) if area_key else {}
            topology_ids: list[str] = area_data.get("topology") or []

            delivery_point_ids: list[str] = _live_delivery_point_ids(
                member.get("delivery_points")
            )

            meter_assets: dict = member.get("assets", {}).get("meter", {})
            sensor_ids: list[str] = [
                m["sensor_id"] for m in meter_assets.values() if m.get("sensor_id")
            ]

            rows.append(
                {
                    "user_id": user_id,
                    "rec_id": rec_id,
                    "area": area_key,
                    "role": member.get("role"),
                    "member_type": member.get("type"),
                    "topology_ids": topology_ids,
                    "delivery_point_ids": delivery_point_ids,
                    "sensor_ids": sensor_ids,
                    "boundary_id": _area_boundary_id(area_data),
                }
            )

    return rows


def _substation_mismatches(rows: list[dict[str, Any]]) -> list[tuple[str, str]]:
    """
    The (rec_id, area) pairs whose mirror rows would get the wrong substation.

    rec_it's silver_rec_registry takes topology_ids[1] as a member's
    substation_id.  For a row whose area has a boundary, that is right only when
    the area lists exactly one topology node and that node's id is the
    boundary id.  Rows without a boundary_id (areas exported before registry
    schema v0.7) are not checked.

    Returns each offending pair once, sorted; area keys and community ids only,
    never a member.
    """
    bad: set[tuple[str, str]] = set()
    for row in rows:
        boundary_id = row.get("boundary_id")
        if boundary_id is None:
            continue
        topology_ids = row.get("topology_ids") or []
        if len(topology_ids) != 1 or topology_ids[0] != boundary_id:
            bad.add((row["rec_id"], row.get("area") or ""))
    return sorted(bad)


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
def fetch_registry(cfg: PipelineConfig) -> list[dict[str, Any]]:
    """
    Export all REC bundles from the registry API and flatten to row dicts.

    Only active members are included.
    Uses OIDC client credentials from PipelineConfig.sdk.oidc.
    """
    yaml_text = asyncio.run(_fetch_yaml(cfg))
    bundles = _parse_bundles(yaml_text)
    rows = _flatten_to_rows(bundles)
    logger.info(
        "Fetched %d bundle(s), %d active member rows from REC Registry",
        len(bundles),
        len(rows),
    )
    return rows


@task(name="Mirror to raw table", retries=2, retry_delay_seconds=30)
def mirror_to_db(rows: list[dict[str, Any]], cfg: PipelineConfig) -> PipelineTaskResult:
    """
    Atomically replace raw.rec_registry_mirror:
    TRUNCATE + bulk INSERT in a single transaction.

    **An empty row list still replaces the table** (celine-eu/celine-pipelines#7).
    The rows are the export's *active* members, so none is a legitimate answer —
    the last active member of a community suspended — and skipping the truncate
    kept that member in the mirror, treated as active downstream. A broken or
    empty fetch never reaches this task: `_parse_bundles` raises on it first.
    """
    tuples = [
        (
            r["user_id"],
            r["rec_id"],
            r["area"],
            r["role"],
            r["member_type"],
            r["topology_ids"],
            r["delivery_point_ids"],
            r["sensor_ids"],
            r.get("boundary_id"),
        )
        for r in rows
    ]

    with _db_conn(cfg) as conn:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE raw.rec_registry_mirror")
            if not tuples:
                logger.warning(
                    "The registry export has no active member: raw.rec_registry_mirror "
                    "is now empty"
                )
            else:
                psycopg2.extras.execute_values(
                    cur,
                    """
                    INSERT INTO raw.rec_registry_mirror
                        (user_id, rec_id, area, role, member_type,
                         topology_ids, delivery_point_ids, sensor_ids, boundary_id,
                         last_updated)
                    VALUES %s
                    """,
                    tuples,
                    template="(%s, %s, %s, %s, %s, %s, %s, %s, %s, now())",
                    page_size=500,
                )
        conn.commit()

    logger.info("Mirrored %d rows into raw.rec_registry_mirror", len(tuples))
    return PipelineTaskResult(
        command="mirror_to_db",
        status=PipelineStatus.COMPLETED,
        details={"rows_inserted": len(tuples)},
    )


@task(name="Check substation attribution")
def check_substations(rows: list[dict[str, Any]]) -> PipelineTaskResult:
    """
    Flag areas whose members would be netted under the wrong substation.

    The rows are mirrored either way: refusing the export would leave the
    previous mirror feeding rec_it, which is no better.  The registry refuses
    such areas since schema v0.7, so a hit means data written before that.
    """
    mismatches = _substation_mismatches(rows)
    for rec_id, area in mismatches:
        logger.warning(
            "Area %s in %s: substation_id (topology_ids[1]) is not the area's "
            "boundary id, or the area lists other than one node",
            area,
            rec_id,
        )
    return PipelineTaskResult(
        command="check_substations",
        status=PipelineStatus.COMPLETED,
        details={"areas_mismatched": len(mismatches)},
    )


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------


@flow(name="rec-registry-flow")
def rec_registry_flow(config: dict[str, Any] | None = None) -> dict:
    """
    Full REC Registry mirror pipeline:
      1. Ensure raw table + indexes exist
      2. Fetch all active REC members from the registry API
      3. Flag areas whose substation_id would not be their boundary id
      4. Truncate + re-insert into raw.rec_registry_mirror
    """
    cfg = PipelineConfig.model_validate(config or {})

    result: dict = {"status": "success"}
    result["ensure_table"] = ensure_table(cfg)
    rows = fetch_registry(cfg)
    result["substations"] = check_substations(rows)
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
