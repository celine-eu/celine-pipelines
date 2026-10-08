"""The mirror always equals the export's active members, the empty set included.

celine-eu/celine-pipelines#7: with no active member left (the last one of a
community suspended), `mirror_to_db` returned before its TRUNCATE and the
suspended member stayed in raw.rec_registry_mirror, treated as active downstream.

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


def test_no_active_member_still_empties_the_mirror(monkeypatch):
    conn, inserted, result = _mirror(monkeypatch, [])

    assert conn.statements == ["TRUNCATE TABLE raw.rec_registry_mirror"]
    assert inserted == []
    assert conn.committed
    assert result.details == {"rows_inserted": 0}


def test_rows_replace_the_mirror_in_one_transaction(monkeypatch):
    row = {
        "user_id": "ex-00001",
        "community_id": "example-rec",
        "area": "north",
        "role": "consumer",
        "member_type": "person",
        "topology_ids": ["AC000E00001"],
        "delivery_point_ids": ["dp-ex-00001"],
        "sensor_ids": ["sensor-ex-00001"],
        "boundary_id": "AC000E00001",
    }

    conn, inserted, result = _mirror(monkeypatch, [row])

    assert conn.statements == ["TRUNCATE TABLE raw.rec_registry_mirror"]
    assert [v[0] for v in inserted[0]] == ["ex-00001"]
    assert conn.committed
    assert result.details == {"rows_inserted": 1}


def test_a_community_whose_only_member_is_suspended_flattens_to_no_rows():
    """The input that used to skip the truncate."""
    bundles = [
        {
            "community": {"id": "example-rec", "areas": {}},
            "members": {
                "ex-00001": {
                    "user_id": "ex-00001",
                    "status": "suspended",
                    "role": "consumer",
                    "type": "person",
                }
            },
        }
    ]

    assert pipeline._flatten_to_rows(bundles) == []
