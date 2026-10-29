"""The mirror holds no supply points (POD codes).

Its only reader, rec_it's `rec_member_supply_points`, was retired: no sharing
offer was bound to it any more, and no service read it. A POD code is a stable
identifier of a household's supply point, so with no reader it is not mirrored,
and a table created before the retirement loses the column.

Synthetic bundles only: placeholder PODs, no real community or member.
"""

from __future__ import annotations

from flows import pipeline


def _bundle(points: list[dict]) -> list[dict]:
    return [
        {
            "community": {"id": "example-rec", "areas": {}},
            "members": {
                "ex-00001": {
                    "user_id": "ex-00001",
                    "status": "active",
                    "role": "consumer",
                    "type": "person",
                    "delivery_points": points,
                    "assets": {"meter": {"m1": {"sensor_id": "sensor-ex-00001"}}},
                }
            },
        }
    ]


def test_a_member_with_supply_points_mirrors_none_of_them():
    rows = pipeline._flatten_to_rows(
        _bundle([{"id": "IT001E00000001", "type": "pod"}])
    )

    assert len(rows) == 1
    assert "delivery_point_ids" not in rows[0]
    assert "IT001E00000001" not in repr(rows)


def test_the_member_keeps_its_row_and_its_sensors():
    rows = pipeline._flatten_to_rows(_bundle([]))

    assert [r["sensor_ids"] for r in rows] == [["sensor-ex-00001"]]


def test_the_insert_names_no_supply_point_column(monkeypatch):
    statements: list[str] = []

    class _Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def execute(self, sql, *args):
            statements.append(sql)

    class _Conn:
        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def cursor(self):
            return _Cursor()

        def commit(self):
            pass

    monkeypatch.setattr(pipeline, "_db_conn", lambda cfg: _Conn())
    monkeypatch.setattr(
        pipeline.psycopg2.extras,
        "execute_values",
        lambda cur, sql, values, **kw: statements.append(sql),
    )
    rows = pipeline._flatten_to_rows(_bundle([{"id": "IT001E00000001"}]))

    pipeline.mirror_to_db.fn(rows, cfg=None)

    assert not any("delivery_point" in s for s in statements)


def test_a_table_created_before_the_retirement_loses_the_column():
    ddl = " ".join(pipeline._DDL.split())

    assert "DROP COLUMN IF EXISTS delivery_point_ids" in ddl
    assert "delivery_point_ids text[]" not in ddl
