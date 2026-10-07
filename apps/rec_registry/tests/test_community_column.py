"""The mirror names the community `community_id`, the column every REC app filters on.

`rec_id` was the old name; one name means one filter rule downstream. Synthetic keys
only: no real community or member.
"""

from __future__ import annotations

import re

from flows import pipeline


def _bundle(community_id, members):
    return {"community": {"id": community_id, "areas": {}}, "members": members}


def _member(user_id, sensors):
    return {
        "user_id": user_id,
        "status": "active",
        "role": "consumer",
        "type": "person",
        "assets": {"meter": {s: {"sensor_id": s} for s in sensors}},
    }


def test_each_row_carries_the_bundle_community_as_community_id():
    rows = pipeline._flatten_to_rows(
        [_bundle("example-rec", {"m1": _member("ex-00001", ["ex-s1"])}),
         _bundle("other-rec", {"m2": _member("ex-00002", ["ex-s2"])})]
    )

    assert [(r["user_id"], r["community_id"]) for r in rows] == [
        ("ex-00001", "example-rec"),
        ("ex-00002", "other-rec"),
    ]
    assert all("rec_id" not in r for r in rows)


def test_the_slug_is_carried_unchanged():
    """No case-folding or mapping: the value is compared byte for byte downstream."""
    rows = pipeline._flatten_to_rows([_bundle("Other-Rec_2", {"m1": _member("ex-00001", [])})])

    assert rows[0]["community_id"] == "Other-Rec_2"


def test_the_table_is_keyed_by_user_and_community_and_has_no_rec_id():
    ddl = " ".join(pipeline._DDL.split())

    assert "PRIMARY KEY (user_id, community_id)" in ddl
    assert "community_id text NOT NULL" in ddl
    assert not re.search(r"\brec_id\b", ddl)


def test_the_insert_names_community_id(monkeypatch):
    statements: list[str] = []

    class _Cur:
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
            return _Cur()

        def commit(self):
            pass

    monkeypatch.setattr(pipeline, "_db_conn", lambda cfg: _Conn())
    monkeypatch.setattr(
        pipeline.psycopg2.extras,
        "execute_values",
        lambda cur, sql, values, **kw: statements.append(" ".join(sql.split())),
    )
    rows = pipeline._flatten_to_rows([_bundle("example-rec", {"m1": _member("ex-00001", ["ex-s1"])})])
    pipeline.mirror_to_db.fn(rows, cfg=None)

    assert "(user_id, community_id, area," in statements[0]
