"""The mirror equals the export's 90-day window, the empty window included.

Before, each run upserted the fetched rows and pruned only rows older than the
window, so a commitment the export no longer listed stayed in the mirror for
ever, and `last_updated` froze on it: on 2026-10-30 staging held 228 rows of its
own commitments while the export it reads (prod's) was empty. Now each run
deletes the mirror and inserts what was fetched, in one transaction, and records
the run in raw.mirror_refresh in that same transaction, so freshness is
measurable when the export is legitimately empty.

A fake connection records the statements; no database is needed. A failed fetch
never reaches the replace: the flow raises before it.
"""

from __future__ import annotations

import pytest
from celine.sdk.openapi.flexibility.errors import UnexpectedStatus
from flows import pipeline

from tests.conftest import commitment, run_flow


class _Cursor:
    def __init__(self, log: list[tuple[str, tuple | None]]):
        self._log = log
        self.rowcount = 0

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql, params=None):
        self._log.append((" ".join(sql.split()), params))


class _Conn:
    def __init__(self):
        self.statements: list[tuple[str, tuple | None]] = []
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


def _row() -> dict:
    return {**commitment(), "status": "committed", "suggestion_type": "shift_load"}


def test_an_empty_export_empties_the_mirror_and_records_the_run(monkeypatch):
    conn, inserted, result = _mirror(monkeypatch, [])

    sqls = [sql for sql, _ in conn.statements]
    assert sqls[0] == "DELETE FROM raw.flexibility_commitments_mirror"
    assert inserted == []
    assert sqls[1].startswith("INSERT INTO raw.mirror_refresh")
    assert conn.statements[1][1] == ("flexibility_commitments_mirror", 0)
    assert len(sqls) == 2
    assert conn.committed
    assert result.details == {"rows_inserted": 0}


def test_fetched_rows_replace_the_mirror_in_one_transaction(monkeypatch):
    conn, inserted, result = _mirror(monkeypatch, [_row()])

    sqls = [sql for sql, _ in conn.statements]
    assert sqls[0] == "DELETE FROM raw.flexibility_commitments_mirror"
    assert [v[0] for v in inserted[0]] == ["00000000-0000-0000-0000-000000000001"]
    assert conn.statements[1][1] == ("flexibility_commitments_mirror", 1)
    assert conn.committed
    assert result.details == {"rows_inserted": 1}


def test_the_marker_is_an_upsert_per_mirror(monkeypatch):
    conn, _, _ = _mirror(monkeypatch, [])

    marker = conn.statements[1][0]
    assert "ON CONFLICT (mirror) DO UPDATE" in marker
    assert "now()" in marker


def test_a_failed_fetch_never_reaches_the_replace(monkeypatch, stub_export):
    stub_export.status = 503
    conn = _Conn()

    with pytest.raises(UnexpectedStatus):
        run_flow(monkeypatch, pipeline, stub_export, lambda: conn)

    replaced = [
        sql
        for sql, _ in conn.statements
        if sql.startswith(("DELETE", "INSERT", "TRUNCATE"))
    ]
    assert replaced == []
