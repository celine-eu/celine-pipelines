"""
REC Registry mirror pipeline.

Fetches the member export of every community from the CELINE REC Registry API
(`GET /admin/export/members`, REQ-0100: membership without member identity),
flattens it, and writes it into raw.rec_registry_mirror —
a full-replace table (TRUNCATE + INSERT in one transaction) — then builds this
app's dbt layer on it: the view rec_device_membership, the one place the other
REC apps read membership from, and the tests that fail the run on a sensor listed
twice in one community or an area whose substation is not its boundary.

One row per member and community (PRIMARY KEY (member_key, community_id)),
whatever the member's status: the export lists inactive members with their
status, the mirror carries it, and rec_device_membership keeps the active ones.
community_id is the export's community key, the community's slug.  sensor_ids
and topology_ids are stored as Postgres text[] arrays.  No user id, name, e-mail,
DID or supply point (POD code): the export carries none.

boundary_id carries the id of the member's area's boundary (registry schema
v0.7: the area's GSE primary-substation `cod_ac`); it is null for an area
without a boundary.  rec_device_membership takes
topology_ids[1] as the member's substation_id, which is right only when the area
lists exactly one node, equal to boundary_id.  The flow flags every area breaking
that (it does not refuse the export), and this app's singular test
`rec_registry_mirror_substation_is_area_boundary` fails the dbt build on it.

Schedule: every 5 minutes.
"""

import asyncio
import logging
import os
from pathlib import Path
from typing import Any

import httpx
import psycopg2
import psycopg2.extras
from celine.sdk.auth import OidcClientCredentialsProvider, TokenProvider
from celine.utils.pipelines.pipeline import (
    DEV_MODE,
    PipelineConfig,
    PipelineStatus,
    PipelineTaskResult,
    dbt_run,
)
from prefect import flow, task

logger = logging.getLogger(__name__)

os.environ.setdefault("APP_NAME", "rec_registry")

script_dir = Path(__file__).parent

_DDL = """
CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.rec_registry_mirror (
    member_key          text        NOT NULL,
    community_id        text        NOT NULL,
    area                text,
    role                text,
    member_type         text,
    status              text        NOT NULL,
    topology_ids        text[]      NOT NULL DEFAULT '{}',
    sensor_ids          text[]      NOT NULL DEFAULT '{}',
    last_updated        timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (member_key, community_id)
);

-- Keyed by user_id before the member export: a table created earlier keeps its rows
-- and primary key under the new name until this run's replace rewrites them.
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'raw' AND table_name = 'rec_registry_mirror'
          AND column_name = 'user_id'
    ) THEN
        ALTER TABLE raw.rec_registry_mirror RENAME COLUMN user_id TO member_key;
    END IF;
END $$;

-- The rows of a table created before the status was mirrored were all active
-- members: they get that status, and later inserts must name theirs.
ALTER TABLE raw.rec_registry_mirror
    ADD COLUMN IF NOT EXISTS status text NOT NULL DEFAULT 'active';
ALTER TABLE raw.rec_registry_mirror ALTER COLUMN status DROP DEFAULT;

-- Added after the table was first deployed: tables created earlier get it here.
ALTER TABLE raw.rec_registry_mirror ADD COLUMN IF NOT EXISTS boundary_id text;

-- Supply points are no longer mirrored: tables created earlier lose the column (and
-- its index with it).
ALTER TABLE raw.rec_registry_mirror DROP COLUMN IF EXISTS delivery_point_ids;

CREATE INDEX IF NOT EXISTS ix_rec_registry_mirror_community_id
    ON raw.rec_registry_mirror (community_id);

CREATE INDEX IF NOT EXISTS ix_rec_registry_mirror_area
    ON raw.rec_registry_mirror (area);

CREATE INDEX IF NOT EXISTS ix_rec_registry_mirror_role
    ON raw.rec_registry_mirror (role);

CREATE INDEX IF NOT EXISTS ix_rec_registry_mirror_sensor_ids
    ON raw.rec_registry_mirror USING gin (sensor_ids);
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


async def _get_member_export(
    base_url: str,
    token_provider: TokenProvider,
    *,
    timeout: float = 30.0,
    transport: httpx.AsyncBaseTransport | None = None,
) -> dict[str, Any]:
    """GET /admin/export/members for every community, as the registry answers it.

    No `community` parameter: every community.  A refused or failed request raises
    (httpx.HTTPStatusError), so the mirror is left as it was.  `transport` is for
    tests.
    """
    token = await token_provider.get_token()
    async with httpx.AsyncClient(
        base_url=base_url, timeout=timeout, transport=transport
    ) as client:
        response = await client.get(
            "/admin/export/members",
            headers={"Authorization": f"Bearer {token.access_token}"},
        )
        response.raise_for_status()
        return response.json()


async def _fetch_export(cfg: PipelineConfig) -> dict[str, Any]:
    """Obtain a token via OIDC client credentials and read the member export."""
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
    return await _get_member_export(_registry_url(), provider)


def _parse_export(payload: Any) -> list[dict[str, Any]]:
    """The export's community entries; an empty or malformed export raises."""
    communities = payload.get("communities") if isinstance(payload, dict) else None
    if not isinstance(communities, list) or not communities:
        raise ValueError("REC Registry returned empty export")
    return communities


def _boundary_id(area_data: Any) -> str | None:
    """An area's boundary id, or None when the area has none.

    Anything but a non-blank string (no area, a null id, a malformed one) is
    treated as absent, so an area stored before registry schema v0.7 mirrors with
    a null boundary_id.
    """
    if not isinstance(area_data, dict):
        return None
    boundary_id = area_data.get("boundary_id")
    if not isinstance(boundary_id, str) or not boundary_id.strip():
        return None
    return boundary_id


def _flatten_to_rows(communities: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """
    Flatten the export's communities into one row per member, whatever its status.

    Only the columns below are taken from a member; anything else the export
    might carry is not mirrored.

    Columns produced:
      member_key, community_id, area, role, member_type, status,
      topology_ids, sensor_ids, boundary_id
    """
    rows: list[dict[str, Any]] = []

    for entry in communities:
        community_id = entry.get("community")
        if not community_id:
            logger.warning("Export entry has no community key — skipping")
            continue

        areas: dict[str, Any] = entry.get("areas") or {}

        for member in entry.get("members") or []:
            member_key = member.get("key")
            if not member_key:
                logger.warning("A member in %s has no key — skipping", community_id)
                continue

            area_key: str | None = member.get("area")
            area_data = areas.get(area_key) if area_key else None
            topology_ids: list[str] = (
                (area_data.get("topology_ids") or [])
                if isinstance(area_data, dict)
                else []
            )

            rows.append(
                {
                    "member_key": member_key,
                    "community_id": community_id,
                    "area": area_key,
                    "role": member.get("role"),
                    "member_type": member.get("type"),
                    "status": member.get("status"),
                    "topology_ids": list(topology_ids),
                    "sensor_ids": list(member.get("sensor_ids") or []),
                    "boundary_id": _boundary_id(area_data),
                }
            )

    return rows


def _substation_mismatches(rows: list[dict[str, Any]]) -> list[tuple[str, str]]:
    """
    The (community_id, area) pairs whose mirror rows would get the wrong substation.

    rec_device_membership takes topology_ids[1] as an active member's
    substation_id.  For a row whose area has a boundary, that is right only when
    the area lists exactly one topology node and that node's id is the
    boundary id.  Rows without a boundary_id (areas exported before registry
    schema v0.7) and rows of inactive members are not checked.

    Returns each offending pair once, sorted; area keys and community ids only,
    never a member.
    """
    bad: set[tuple[str, str]] = set()
    for row in rows:
        boundary_id = row.get("boundary_id")
        if boundary_id is None or row.get("status") != "active":
            continue
        topology_ids = row.get("topology_ids") or []
        if len(topology_ids) != 1 or topology_ids[0] != boundary_id:
            bad.add((row["community_id"], row.get("area") or ""))
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
    Read the member export of every community and flatten it to row dicts.

    Every member is included, with its status.
    Uses OIDC client credentials from PipelineConfig.sdk.oidc.
    """
    communities = _parse_export(asyncio.run(_fetch_export(cfg)))
    rows = _flatten_to_rows(communities)
    logger.info(
        "Fetched %d community(ies), %d member rows (%d active) from REC Registry",
        len(communities),
        len(rows),
        sum(1 for r in rows if r["status"] == "active"),
    )
    return rows


@task(name="Mirror to raw table", retries=2, retry_delay_seconds=30)
def mirror_to_db(rows: list[dict[str, Any]], cfg: PipelineConfig) -> PipelineTaskResult:
    """
    Atomically replace raw.rec_registry_mirror:
    TRUNCATE + bulk INSERT in a single transaction.

    **An empty row list still replaces the table** (celine-eu/celine-pipelines#7):
    skipping the truncate once kept a suspended member in the mirror, treated as
    active downstream.  The rows are every member of the export, each with its
    status, and rec_device_membership keeps the active ones.  A broken or empty
    fetch never reaches this task: `_parse_export` raises on it first.
    """
    tuples = [
        (
            r["member_key"],
            r["community_id"],
            r["area"],
            r["role"],
            r["member_type"],
            r["status"],
            r["topology_ids"],
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
                    "The registry export has no member: raw.rec_registry_mirror "
                    "is now empty"
                )
            else:
                psycopg2.extras.execute_values(
                    cur,
                    """
                    INSERT INTO raw.rec_registry_mirror
                        (member_key, community_id, area, role, member_type,
                         status, topology_ids, sensor_ids, boundary_id,
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
    for community_id, area in mismatches:
        logger.warning(
            "Area %s in %s: substation_id (topology_ids[1]) is not the area's "
            "boundary id, or the area lists other than one node",
            area,
            community_id,
        )
    return PipelineTaskResult(
        command="check_substations",
        status=PipelineStatus.COMPLETED,
        details={"areas_mismatched": len(mismatches)},
    )


@task(name="Build membership")
def build_membership(cfg: PipelineConfig) -> PipelineTaskResult:
    """
    Build the dbt layer on the fresh mirror: the view rec_device_membership and
    its tests. A sensor listed twice in one community, or an area whose
    substation is not its boundary, fails the run here; the listing is fixed in
    the registry, never deduplicated downstream.
    """
    return dbt_run("build", cfg)


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------


@flow(name="rec-registry-flow")
def rec_registry_flow(config: dict[str, Any] | None = None) -> dict:
    """
    Full REC Registry mirror pipeline:
      1. Ensure raw table + indexes exist
      2. Read every community's member export from the registry API
      3. Flag areas whose substation_id would not be their boundary id
      4. Truncate + re-insert into raw.rec_registry_mirror
      5. Build rec_device_membership and run its tests
    """
    cfg = PipelineConfig.model_validate(config or {})

    result: dict = {"status": "success"}
    result["ensure_table"] = ensure_table(cfg)
    rows = fetch_registry(cfg)
    result["substations"] = check_substations(rows)
    result["mirror"] = mirror_to_db(rows, cfg)
    result["membership"] = build_membership(cfg)

    return result


if __name__ == "__main__":
    import yaml as _yaml

    config_path = script_dir / "config.yaml"
    with open(config_path) as fh:
        flow_cfg = _yaml.safe_load(fh)

    schedule = flow_cfg["schedule"]

    if DEV_MODE:
        rec_registry_flow.serve(name=schedule["name"], cron=schedule["cron"])
