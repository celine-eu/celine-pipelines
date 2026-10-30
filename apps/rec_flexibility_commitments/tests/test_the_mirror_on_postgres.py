"""The flow on a real Postgres: replace, marker, failed fetch, DDL re-run.

Runs only when REC_FLEXIBILITY_COMMITMENTS_TEST_PG_DSN names a throwaway
Postgres; each test drops and recreates the `raw` schema there. The export comes
from the stub flexibility-api (see conftest). Synthetic ids only.
"""

from __future__ import annotations

import os

import pytest
from celine.sdk.openapi.flexibility.errors import UnexpectedStatus
from flows import pipeline

from tests.conftest import commitment, run_flow

_DSN = os.getenv("REC_FLEXIBILITY_COMMITMENTS_TEST_PG_DSN")

pytestmark = pytest.mark.skipif(
    not _DSN, reason="REC_FLEXIBILITY_COMMITMENTS_TEST_PG_DSN not set"
)

# The table as the upsert flow left it: no raw.mirror_refresh, two rows the export
# no longer lists, last_updated frozen.
_STALE_MIRROR = """
DROP SCHEMA IF EXISTS raw CASCADE;
CREATE SCHEMA raw;
CREATE TABLE raw.flexibility_commitments_mirror (
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
INSERT INTO raw.flexibility_commitments_mirror
    (id, user_id, suggestion_id, suggestion_type, period_start, period_end,
     committed_at, status, reward_points_estimated, last_updated)
VALUES
    ('stale-1', 'user-ex-00001', 'sugg-1', 'shift_load', now(), now(),
     now() - interval '3 days', 'StatusSchema.committed', 5,
     '2026-10-27 10:45+00'),
    ('stale-2', 'user-ex-00002', 'sugg-2', 'shift_load', now(), now(),
     now() - interval '2 days', 'StatusSchema.settled', 5,
     '2026-10-27 10:45+00');
"""


def _connect():
    import psycopg2

    return psycopg2.connect(_DSN)


def _query(sql: str) -> list[tuple]:
    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            return cur.fetchall()
    finally:
        conn.close()


def _execute(sql: str) -> None:
    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()
    finally:
        conn.close()


def _mirror_ids() -> list[str]:
    return [
        i
        for (i,) in _query(
            "SELECT id FROM raw.flexibility_commitments_mirror ORDER BY id"
        )
    ]


def _marker() -> list[tuple]:
    return _query(
        "SELECT rows, now() - refreshed_at < interval '1 minute' "
        "FROM raw.mirror_refresh WHERE mirror = 'flexibility_commitments_mirror'"
    )


@pytest.fixture
def stale_mirror():
    _execute(_STALE_MIRROR)


def test_the_ddl_runs_twice_on_an_existing_table(stale_mirror):
    _execute(pipeline._DDL)
    _execute(pipeline._DDL)

    assert _mirror_ids() == ["stale-1", "stale-2"]
    columns = _query(
        "SELECT column_name, data_type, is_nullable FROM information_schema.columns "
        "WHERE table_schema = 'raw' AND table_name = 'mirror_refresh' "
        "ORDER BY ordinal_position"
    )
    assert columns == [
        ("mirror", "text", "NO"),
        ("refreshed_at", "timestamp with time zone", "NO"),
        ("rows", "integer", "NO"),
    ]


def test_an_empty_export_empties_the_stale_mirror_and_marks_the_run(
    stale_mirror, monkeypatch, stub_export
):
    stub_export.body = []

    run_flow(monkeypatch, pipeline, stub_export, _connect)

    assert _mirror_ids() == []
    assert _marker() == [(0, True)]


def test_the_export_replaces_the_stale_mirror(stale_mirror, monkeypatch, stub_export):
    stub_export.body = [commitment()]

    run_flow(monkeypatch, pipeline, stub_export, _connect)

    assert _mirror_ids() == ["00000000-0000-0000-0000-000000000001"]
    assert _marker() == [(1, True)]


def test_a_failed_fetch_leaves_mirror_and_marker_as_they_were(
    stale_mirror, monkeypatch, stub_export
):
    _execute(pipeline._DDL)
    _execute(
        "INSERT INTO raw.mirror_refresh (mirror, refreshed_at, rows) "
        "VALUES ('flexibility_commitments_mirror', '2026-10-27 10:45+00', 2)"
    )
    stub_export.status = 500

    with pytest.raises(UnexpectedStatus):
        run_flow(monkeypatch, pipeline, stub_export, _connect)

    assert _mirror_ids() == ["stale-1", "stale-2"]
    assert _query(
        "SELECT rows, refreshed_at = '2026-10-27 10:45+00' FROM raw.mirror_refresh"
    ) == [(2, True)]


def test_a_failed_insert_rolls_the_delete_back(stale_mirror, monkeypatch, stub_export):
    """Two export items with one id: the insert fails, the delete is undone."""
    _execute(pipeline._DDL)
    stub_export.body = [commitment(), commitment()]

    import psycopg2.errors

    with pytest.raises(psycopg2.errors.UniqueViolation):
        run_flow(monkeypatch, pipeline, stub_export, _connect)

    assert _mirror_ids() == ["stale-1", "stale-2"]
    assert _marker() == []
