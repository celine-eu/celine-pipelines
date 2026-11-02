"""The mirror carries POD hashes, never a POD code.

A POD leaves prod only as an HMAC hash, and only prod's rec-registry holds the key.
The member export adds, per member and only when the registry has the key, a sorted
`pod_hashes` list (64-char lowercase hex, the member's active delivery points) and
`meter_pod_hashes` ({sensor_id: hash or null}). The mirror stores both; an absent
field is empty, never a failure. A value that is not a hash is dropped before it
reaches a row, and only a count is logged, so a raw POD cannot land even if the
source misbehaves.

Synthetic exports only: placeholder PODs and hashes of placeholder strings, no real
community, member or supply point. The last tests run only when
REC_REGISTRY_TEST_PG_DSN names a throwaway Postgres.
"""

from __future__ import annotations

import hashlib
import logging
import os

import pytest
from flows import pipeline

POD = "IT001E00000001"  # POD-shaped, synthetic
H1 = hashlib.sha256(b"placeholder-1").hexdigest()
H2 = hashlib.sha256(b"placeholder-2").hexdigest()


def _export(**member_fields) -> list[dict]:
    member = {
        "key": "ex-00001",
        "area": None,
        "role": "consumer",
        "type": "person",
        "status": "active",
        "sensor_ids": ["sensor-ex-00001", "sensor-ex-00002"],
    }
    member.update(member_fields)
    return [{"community": "example-rec", "areas": {}, "members": [member]}]


def _row(**member_fields) -> dict:
    (row,) = pipeline._flatten_to_rows(_export(**member_fields))
    return row


# --- the flatten --------------------------------------------------------------


def test_the_hashes_of_a_member_are_mirrored():
    row = _row(
        pod_hashes=[H1, H2],
        meter_pod_hashes={"sensor-ex-00001": H1, "sensor-ex-00002": None},
    )

    assert row["pod_hashes"] == sorted([H1, H2])
    assert row["meter_pod_hashes"] == {"sensor-ex-00001": H1, "sensor-ex-00002": None}


def test_an_export_without_the_fields_mirrors_empty_values():
    """A registry without the key (or older than the change) sends no field."""
    row = _row()

    assert row["pod_hashes"] == []
    assert row["meter_pod_hashes"] == {}


@pytest.mark.parametrize(
    "fields",
    [
        {"pod_hashes": None, "meter_pod_hashes": None},
        {"pod_hashes": "not-a-list", "meter_pod_hashes": ["not", "a", "dict"]},
    ],
)
def test_a_null_or_malformed_field_mirrors_empty_values(fields):
    row = _row(**fields)

    assert row["pod_hashes"] == []
    assert row["meter_pod_hashes"] == {}


def test_a_raw_pod_in_pod_hashes_is_rejected_and_never_mirrored():
    row = _row(pod_hashes=[H1, POD])

    assert row["pod_hashes"] == [H1]
    assert POD not in repr(row)


def test_a_raw_pod_in_meter_pod_hashes_is_rejected_and_never_mirrored():
    row = _row(meter_pod_hashes={"sensor-ex-00001": POD, "sensor-ex-00002": H2})

    assert row["meter_pod_hashes"] == {"sensor-ex-00002": H2}
    assert POD not in repr(row)


@pytest.mark.parametrize(
    "value",
    [
        H1.upper(),  # uppercase hex
        H1[:-1],  # 63 chars
        H1 + "0",  # 65 chars
        "g" * 64,  # not hex
        " " + H1[1:],  # whitespace
        H1 + "\n",  # trailing newline
        "",
        123,
    ],
)
def test_anything_but_64_char_lowercase_hex_is_rejected(value):
    row = _row(pod_hashes=[value], meter_pod_hashes={"sensor-ex-00001": value})

    assert row["pod_hashes"] == []
    assert row["meter_pod_hashes"] == {}


def test_rejections_are_logged_as_a_count_never_as_a_value(caplog):
    with caplog.at_level(logging.WARNING, logger=pipeline.logger.name):
        pipeline._flatten_to_rows(
            _export(
                pod_hashes=[POD, H1],
                meter_pod_hashes={"sensor-ex-00001": POD},
            )
        )

    text = caplog.text
    assert POD not in text
    assert "ex-00001" not in text
    assert "2" in text and "example-rec" in text


def test_a_clean_export_logs_no_rejection(caplog):
    with caplog.at_level(logging.WARNING, logger=pipeline.logger.name):
        pipeline._flatten_to_rows(_export(pod_hashes=[H1]))

    assert "reject" not in caplog.text.lower()


# --- the table ----------------------------------------------------------------


def test_the_table_declares_the_hash_columns_and_an_existing_one_gains_them():
    ddl = " ".join(pipeline._DDL.split())
    create = ddl.split(");", 1)[0]

    assert "pod_hashes text[] NOT NULL DEFAULT '{}'" in create
    assert "meter_pod_hashes jsonb NOT NULL DEFAULT '{}'" in create
    assert "ADD COLUMN IF NOT EXISTS pod_hashes text[] NOT NULL DEFAULT '{}'" in ddl
    assert (
        "ADD COLUMN IF NOT EXISTS meter_pod_hashes jsonb NOT NULL DEFAULT '{}'" in ddl
    )
    assert (
        "CREATE INDEX IF NOT EXISTS ix_rec_registry_mirror_pod_hashes "
        "ON raw.rec_registry_mirror USING gin (pod_hashes)" in ddl
    )


def test_the_insert_writes_the_hashes(monkeypatch):
    sqls: list[str] = []
    values: list[list] = []

    class _Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def execute(self, sql, *args):
            pass

    class _Conn:
        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def cursor(self):
            return _Cursor()

        def commit(self):
            pass

    def _execute_values(cur, sql, vals, **kw):
        sqls.append(" ".join(sql.split()))
        values.extend(vals)

    monkeypatch.setattr(pipeline, "_db_conn", lambda cfg: _Conn())
    monkeypatch.setattr(pipeline.psycopg2.extras, "execute_values", _execute_values)
    rows = pipeline._flatten_to_rows(
        _export(pod_hashes=[H1], meter_pod_hashes={"sensor-ex-00001": H1})
    )

    pipeline.mirror_to_db.fn(rows, cfg=None)

    assert "pod_hashes, meter_pod_hashes" in sqls[0]
    (value,) = values
    assert [H1] in value
    (jsonb,) = [v for v in value if isinstance(v, pipeline.psycopg2.extras.Json)]
    assert jsonb.adapted == {"sensor-ex-00001": H1}


# --- on a real Postgres -------------------------------------------------------

_DSN = os.getenv("REC_REGISTRY_TEST_PG_DSN")

_TABLE_BEFORE_THE_HASHES = """
DROP SCHEMA IF EXISTS raw CASCADE;
CREATE SCHEMA raw;
CREATE TABLE raw.rec_registry_mirror (
    member_key          text        NOT NULL,
    community_id        text        NOT NULL,
    area                text,
    role                text,
    member_type         text,
    status              text        NOT NULL,
    topology_ids        text[]      NOT NULL DEFAULT '{}',
    sensor_ids          text[]      NOT NULL DEFAULT '{}',
    boundary_id         text,
    last_updated        timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (member_key, community_id)
);
INSERT INTO raw.rec_registry_mirror (member_key, community_id, status, sensor_ids)
VALUES ('ex-00001', 'example-rec', 'active', '{sensor-ex-00001}');
"""


class _Keep:
    """A connection whose context manager and commit leave the transaction open."""

    def __init__(self, conn):
        self._conn = conn

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def cursor(self):
        return self._conn.cursor()

    def commit(self):
        pass


@pytest.mark.skipif(not _DSN, reason="REC_REGISTRY_TEST_PG_DSN not set")
def test_an_existing_table_gains_empty_hash_columns_and_takes_the_flow_rows(
    monkeypatch,
):
    import psycopg2

    conn = psycopg2.connect(_DSN)
    try:
        with conn.cursor() as cur:
            cur.execute(_TABLE_BEFORE_THE_HASHES)
            cur.execute(pipeline._DDL)
            cur.execute(pipeline._DDL)
            cur.execute(
                "SELECT pod_hashes, meter_pod_hashes FROM raw.rec_registry_mirror"
            )
            existing = cur.fetchall()
            cur.execute(
                "SELECT indexdef FROM pg_indexes WHERE schemaname = 'raw' "
                "AND indexname = 'ix_rec_registry_mirror_pod_hashes'"
            )
            (indexdef,) = cur.fetchone()

        monkeypatch.setattr(pipeline, "_db_conn", lambda cfg: _Keep(conn))
        rows = pipeline._flatten_to_rows(
            _export(
                pod_hashes=[H2, H1, POD],
                meter_pod_hashes={"sensor-ex-00001": H1, "sensor-ex-00002": None},
            )
        )
        pipeline.mirror_to_db.fn(rows, cfg=None)

        with conn.cursor() as cur:
            cur.execute(
                "SELECT pod_hashes, meter_pod_hashes FROM raw.rec_registry_mirror"
            )
            written = cur.fetchall()
            cur.execute(
                "SELECT count(*) FROM raw.rec_registry_mirror "
                "WHERE pod_hashes @> ARRAY[%s]",
                (H1,),
            )
            (found,) = cur.fetchone()
            cur.execute(
                "SELECT count(*) FROM raw.rec_registry_mirror "
                "WHERE array_to_string(pod_hashes, ',') LIKE %s "
                "OR meter_pod_hashes::text LIKE %s",
                (f"%{POD}%", f"%{POD}%"),
            )
            (raw_pods,) = cur.fetchone()
        conn.rollback()
    finally:
        conn.close()

    assert existing == [([], {})]
    assert "gin (pod_hashes)" in indexdef
    assert written == [
        (sorted([H1, H2]), {"sensor-ex-00001": H1, "sensor-ex-00002": None})
    ]
    assert found == 1
    assert raw_pods == 0
