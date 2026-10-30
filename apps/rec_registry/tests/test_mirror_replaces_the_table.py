"""The mirror always equals the export's members, the empty set included.

celine-eu/celine-pipelines#7: with no active member left (the last one of a
community suspended), `mirror_to_db` returned before its TRUNCATE and the
suspended member stayed in raw.rec_registry_mirror, treated as active downstream.
The member export now lists inactive members with their status, so the suspended
member is mirrored as suspended and rec_device_membership leaves it out; an export
with no member at all still empties the table.

A fake connection records the statements; no database is needed. Synthetic keys
only: no real community or member.
"""

from __future__ import annotations

from flows import pipeline


class _Cursor:
    def __init__(self, log: list[str]):
        self._log = log

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql, *args):
        self._log.append(" ".join(sql.split()))


class _Conn:
    def __init__(self):
        self.statements: list[str] = []
        self.committed = False

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def cursor(self):
        return _Cursor(self.statements)

    def commit(self):
        self.committed = True


def _mirror(monkeypatch, rows):
    conn = _Conn()
    inserted: list[list] = []
    monkeypatch.setattr(pipeline, "_db_conn", lambda cfg: conn)
    monkeypatch.setattr(
        pipeline.psycopg2.extras,
        "execute_values",
        lambda cur, sql, values, **kw: inserted.append(list(values)),
    )
    result = pipeline.mirror_to_db.fn(rows, cfg=None)
    return conn, inserted, result


def test_no_member_still_empties_the_mirror(monkeypatch):
    conn, inserted, result = _mirror(monkeypatch, [])

    assert conn.statements == ["TRUNCATE TABLE raw.rec_registry_mirror"]
    assert inserted == []
    assert conn.committed
    assert result.details == {"rows_inserted": 0}


def test_rows_replace_the_mirror_in_one_transaction(monkeypatch):
    row = {
        "member_key": "ex-00001",
        "community_id": "example-rec",
        "area": "north",
        "role": "consumer",
        "member_type": "person",
        "status": "active",
        "topology_ids": ["AC000E00001"],
        "sensor_ids": ["sensor-ex-00001"],
        "boundary_id": "AC000E00001",
    }

    conn, inserted, result = _mirror(monkeypatch, [row])

    assert conn.statements == ["TRUNCATE TABLE raw.rec_registry_mirror"]
    assert [v[0] for v in inserted[0]] == ["ex-00001"]
    assert "active" in inserted[0][0]
    assert conn.committed
    assert result.details == {"rows_inserted": 1}


def test_a_community_whose_only_member_is_suspended_mirrors_it_as_suspended():
    """The input that used to skip the truncate: the row now says what it is."""
    export = [
        {
            "community": "example-rec",
            "areas": {},
            "members": [
                {
                    "key": "ex-00001",
                    "area": None,
                    "role": "consumer",
                    "type": "person",
                    "status": "suspended",
                    "sensor_ids": ["sensor-ex-00001"],
                }
            ],
        }
    ]

    rows = pipeline._flatten_to_rows(export)

    assert [(r["member_key"], r["status"]) for r in rows] == [("ex-00001", "suspended")]
