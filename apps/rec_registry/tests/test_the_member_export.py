"""The flow reads the registry's member export, which names no member (REQ-0100).

`GET /admin/export/members` answers per community its key, each area's boundary id
and topology node ids, and every member, whatever its status, with its key, area,
role, type, status and meter sensor ids. The mirror is keyed by (member_key,
community_id) and carries the status; rec_device_membership keeps the active ones.

A mock HTTP transport stands in for the registry; no database is needed except by
the last test, which runs only when REC_REGISTRY_TEST_PG_DSN names a throwaway
Postgres. Synthetic keys only: no real community or member.
"""

from __future__ import annotations

import asyncio
import os
from pathlib import Path

import httpx
import pytest
from flows import pipeline

APP_DIR = Path(pipeline.__file__).resolve().parent.parent

MIRROR_COLUMNS = {
    "member_key",
    "community_id",
    "area",
    "role",
    "member_type",
    "status",
    "topology_ids",
    "sensor_ids",
    "boundary_id",
}


class _Token:
    access_token = "test-token"


class _Provider:
    async def get_token(self):
        return _Token()


def _payload() -> dict:
    return {
        "communities": [
            {
                "community": "example-rec",
                "areas": {
                    "north": {
                        "boundary_id": "AC000E00001",
                        "topology_ids": ["AC000E00001"],
                    }
                },
                "members": [
                    {
                        "key": "ex-00001",
                        "area": "north",
                        "role": "prosumer",
                        "type": "person",
                        "status": "active",
                        "sensor_ids": ["sensor-ex-00001"],
                    },
                    {
                        "key": "ex-00002",
                        "area": "north",
                        "role": "consumer",
                        "type": "person",
                        "status": "suspended",
                        "sensor_ids": [],
                    },
                ],
            }
        ]
    }


def _get(handler) -> dict:
    return asyncio.run(
        pipeline._get_member_export(
            "http://registry.test",
            _Provider(),
            transport=httpx.MockTransport(handler),
        )
    )


# --- the request ------------------------------------------------------------


def test_the_flow_asks_for_the_member_export_of_every_community_with_its_token():
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, json=_payload())

    assert _get(handler) == _payload()
    assert len(seen) == 1
    assert seen[0].method == "GET"
    assert seen[0].url.path == "/admin/export/members"
    assert "community" not in seen[0].url.params
    assert seen[0].headers["authorization"] == "Bearer test-token"


@pytest.mark.parametrize("status", [401, 403, 404, 500])
def test_a_refused_export_raises_before_the_mirror_is_touched(status):
    with pytest.raises(httpx.HTTPStatusError):
        _get(lambda request: httpx.Response(status, json={"detail": "no"}))


# --- the answer -------------------------------------------------------------


@pytest.mark.parametrize(
    "payload", [{}, {"communities": []}, {"communities": None}, []]
)
def test_an_empty_or_malformed_export_raises(payload):
    with pytest.raises(ValueError):
        pipeline._parse_export(payload)


def test_every_member_is_mirrored_with_its_status_and_nothing_else():
    rows = pipeline._flatten_to_rows(pipeline._parse_export(_payload()))

    assert [(r["member_key"], r["status"]) for r in rows] == [
        ("ex-00001", "active"),
        ("ex-00002", "suspended"),
    ]
    assert all(set(r) == MIRROR_COLUMNS for r in rows)
    assert rows[0] == {
        "member_key": "ex-00001",
        "community_id": "example-rec",
        "area": "north",
        "role": "prosumer",
        "member_type": "person",
        "status": "active",
        "topology_ids": ["AC000E00001"],
        "sensor_ids": ["sensor-ex-00001"],
        "boundary_id": "AC000E00001",
    }


def test_identity_fields_in_an_export_are_not_mirrored():
    payload = _payload()
    payload["communities"][0]["members"][0].update(
        {"user_id": "person@example.test", "name": "Example Person", "did": "did:ex:1"}
    )

    rows = pipeline._flatten_to_rows(pipeline._parse_export(payload))

    assert all(set(r) == MIRROR_COLUMNS for r in rows)
    for value in ("person@example.test", "Example Person", "did:ex:1"):
        assert value not in repr(rows)


def test_a_member_without_a_key_is_skipped():
    payload = _payload()
    payload["communities"][0]["members"][0]["key"] = ""

    rows = pipeline._flatten_to_rows(pipeline._parse_export(payload))

    assert [r["member_key"] for r in rows] == ["ex-00002"]


def test_a_community_without_a_key_is_skipped():
    payload = _payload()
    payload["communities"][0]["community"] = None

    assert pipeline._flatten_to_rows(pipeline._parse_export(payload)) == []


# --- the table --------------------------------------------------------------


def test_the_table_has_no_user_id_and_an_existing_one_is_renamed():
    ddl = " ".join(pipeline._DDL.split())
    create = ddl.split(");", 1)[0]

    assert "user_id" not in create
    assert "member_key text NOT NULL" in create
    assert "RENAME COLUMN user_id TO member_key" in ddl


def test_an_existing_table_gains_the_status_its_rows_already_had():
    """The rows there were all active members: they are backfilled as such."""
    ddl = " ".join(pipeline._DDL.split())

    assert "ADD COLUMN IF NOT EXISTS status text NOT NULL DEFAULT 'active'" in ddl
    assert "ALTER COLUMN status DROP DEFAULT" in ddl


def test_membership_and_its_substation_test_read_active_members_only():
    for rel in (
        "dbt/models/gold/rec_device_membership.sql",
        "dbt/tests/rec_registry_mirror_substation_is_area_boundary.sql",
    ):
        sql = " ".join((APP_DIR / rel).read_text().split())
        assert "status = 'active'" in sql, rel


# --- the migration, on a real Postgres --------------------------------------

_DSN = os.getenv("REC_REGISTRY_TEST_PG_DSN")

_OLD_TABLE = """
DROP SCHEMA IF EXISTS raw CASCADE;
CREATE SCHEMA raw;
CREATE TABLE raw.rec_registry_mirror (
    user_id             text        NOT NULL,
    community_id        text        NOT NULL,
    area                text,
    role                text,
    member_type         text,
    topology_ids        text[]      NOT NULL DEFAULT '{}',
    sensor_ids          text[]      NOT NULL DEFAULT '{}',
    delivery_point_ids  text[]      NOT NULL DEFAULT '{}',
    boundary_id         text,
    last_updated        timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (user_id, community_id)
);
INSERT INTO raw.rec_registry_mirror (user_id, community_id, sensor_ids)
VALUES ('ex-00001', 'example-rec', '{sensor-ex-00001}');
"""


@pytest.mark.skipif(not _DSN, reason="REC_REGISTRY_TEST_PG_DSN not set")
def test_the_ddl_migrates_an_existing_table_in_place_and_twice():
    import psycopg2

    conn = psycopg2.connect(_DSN)
    try:
        with conn.cursor() as cur:
            cur.execute(_OLD_TABLE)
            cur.execute(pipeline._DDL)
            cur.execute(pipeline._DDL)
            cur.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = 'raw' AND table_name = 'rec_registry_mirror'"
            )
            columns = {c for (c,) in cur.fetchall()}
            cur.execute(
                "SELECT a.attname FROM pg_index i "
                "JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) "
                "WHERE i.indrelid = 'raw.rec_registry_mirror'::regclass AND i.indisprimary"
            )
            key = {c for (c,) in cur.fetchall()}
            cur.execute("SELECT member_key, status FROM raw.rec_registry_mirror")
            existing = cur.fetchall()
            cur.execute(
                "SELECT column_default FROM information_schema.columns "
                "WHERE table_schema = 'raw' AND table_name = 'rec_registry_mirror' "
                "AND column_name = 'status'"
            )
            (status_default,) = cur.fetchone()
        conn.rollback()
    finally:
        conn.close()

    assert columns == MIRROR_COLUMNS | {"last_updated"}
    assert key == {"member_key", "community_id"}
    assert existing == [("ex-00001", "active")]
    assert status_default is None
