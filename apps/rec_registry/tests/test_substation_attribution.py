"""The mirror carries each area's boundary id, and flags areas whose
substation_id (topology_ids[1], as rec_it reads it) is not that boundary id.

Synthetic exports only: AC000E0000x codes, no real community or member.
"""

from __future__ import annotations

import json

from flows import pipeline


def _area(boundary_id: str | None, topology: list[str] | None) -> dict:
    return {"boundary_id": boundary_id, "topology_ids": topology}


def _member(key: str, area: str | None, status: str = "active") -> dict:
    return {
        "key": key,
        "area": area,
        "role": "consumer",
        "type": "person",
        "status": status,
        "sensor_ids": [f"sensor-{key}"],
    }


def _community(community_id: str, areas: dict, members: dict) -> dict:
    return {
        "community": community_id,
        "areas": areas,
        "members": list(members.values()),
    }


def _rows_by_member(communities: list[dict]) -> dict[str, dict]:
    return {r["member_key"]: r for r in pipeline._flatten_to_rows(communities)}


# --- the boundary id is carried into the mirror -----------------------------


def test_row_carries_its_areas_boundary_id():
    rows = _rows_by_member(
        [
            _community(
                "rec-a",
                {"north": _area("AC000E00001", ["AC000E00001"])},
                {"m1": _member("u1", "north")},
            )
        ]
    )
    assert rows["u1"]["boundary_id"] == "AC000E00001"
    assert rows["u1"]["topology_ids"] == ["AC000E00001"]


def test_area_without_boundary_mirrors_a_null_boundary_id():
    rows = _rows_by_member(
        [
            _community(
                "rec-a",
                {"legacy": _area(None, ["n1", "n2"])},
                {"m1": _member("u1", "legacy")},
            )
        ]
    )
    assert rows["u1"]["boundary_id"] is None
    assert rows["u1"]["topology_ids"] == ["n1", "n2"]


def test_member_without_area_or_with_unknown_area_has_no_boundary():
    rows = _rows_by_member(
        [
            _community(
                "rec-a",
                {"north": _area("AC000E00001", ["AC000E00001"])},
                {"m1": _member("u1", None), "m2": _member("u2", "gone")},
            )
        ]
    )
    for member in ("u1", "u2"):
        assert rows[member]["boundary_id"] is None
        assert rows[member]["topology_ids"] == []


def test_malformed_boundary_ids_are_treated_as_absent():
    for boundary_id in (None, "", "  ", 7, [], {"id": "AC000E00001"}):
        area = {"boundary_id": boundary_id, "topology_ids": ["AC000E00001"]}
        rows = _rows_by_member(
            [_community("rec-a", {"x": area}, {"m1": _member("u1", "x")})]
        )
        assert rows["u1"]["boundary_id"] is None, boundary_id


def test_null_topology_mirrors_an_empty_list():
    area = _area("AC000E00001", None)
    rows = _rows_by_member(
        [_community("rec-a", {"x": area}, {"m1": _member("u1", "x")})]
    )
    assert rows["u1"]["topology_ids"] == []


def test_inactive_members_are_mirrored_with_their_status():
    rows = _rows_by_member(
        [
            _community(
                "rec-a",
                {"north": _area("AC000E00001", ["AC000E00001"])},
                {"m1": _member("u1", "north", status="suspended")},
            )
        ]
    )
    assert rows["u1"]["status"] == "suspended"
    assert rows["u1"]["boundary_id"] == "AC000E00001"


def test_member_export_json_round_trips_through_parse_and_flatten():
    payload = json.loads(
        json.dumps(
            {
                "communities": [
                    _community(
                        "rec-a",
                        {"north": _area("AC000E00001", ["AC000E00001"])},
                        {"m1": _member("u1", "north")},
                    ),
                    _community(
                        "rec-b",
                        {"south": _area("AC000E00002", ["AC000E00002"])},
                        {"m1": _member("u2", "south")},
                    ),
                ]
            }
        )
    )
    rows = _rows_by_member(pipeline._parse_export(payload))
    assert {u: r["boundary_id"] for u, r in rows.items()} == {
        "u1": "AC000E00001",
        "u2": "AC000E00002",
    }
    assert pipeline._substation_mismatches(list(rows.values())) == []


# --- substation_id must equal the area's boundary id ------------------------


def _mismatches(areas: dict, members: dict, community_id: str = "rec-a"):
    return pipeline._substation_mismatches(
        pipeline._flatten_to_rows([_community(community_id, areas, members)])
    )


def test_one_node_equal_to_the_boundary_passes():
    assert (
        _mismatches(
            {
                "north": _area("AC000E00001", ["AC000E00001"]),
                "south": _area("AC000E00002", ["AC000E00002"]),
            },
            {"m1": _member("u1", "north"), "m2": _member("u2", "south")},
        )
        == []
    )


def test_node_differing_from_the_boundary_is_flagged():
    assert _mismatches(
        {"north": _area("AC000E00001", ["AC000E00002"])},
        {"m1": _member("u1", "north")},
    ) == [("rec-a", "north")]


def test_two_nodes_are_flagged_even_when_the_first_is_the_boundary():
    assert _mismatches(
        {"north": _area("AC000E00001", ["AC000E00001", "AC000E00002"])},
        {"m1": _member("u1", "north")},
    ) == [("rec-a", "north")]


def test_no_node_is_flagged_when_the_area_has_a_boundary():
    assert _mismatches(
        {"north": _area("AC000E00001", [])},
        {"m1": _member("u1", "north")},
    ) == [("rec-a", "north")]


def test_ids_are_compared_exactly():
    assert _mismatches(
        {"north": _area("AC000E00001", ["ac000e00001 "])},
        {"m1": _member("u1", "north")},
    ) == [("rec-a", "north")]


def test_rows_without_a_boundary_are_not_checked():
    assert (
        _mismatches(
            {"legacy": _area(None, ["n1", "n2"]), "empty": _area(None, None)},
            {"m1": _member("u1", "legacy"), "m2": _member("u2", "empty")},
        )
        == []
    )


def test_an_area_holding_only_inactive_members_is_not_checked():
    """As before the status was mirrored: only active members are netted downstream."""
    assert (
        _mismatches(
            {"north": _area("AC000E00001", ["AC000E00002"])},
            {"m1": _member("u1", "north", status="suspended")},
        )
        == []
    )


def test_each_offending_area_is_reported_once_and_names_no_member():
    result = pipeline._substation_mismatches(
        pipeline._flatten_to_rows(
            [
                _community(
                    "rec-b",
                    {"x": _area("AC000E00003", ["AC000E00004"])},
                    {"m1": _member("u1", "x"), "m2": _member("u2", "x")},
                ),
                _community(
                    "rec-a",
                    {
                        "y": _area("AC000E00001", ["AC000E00001", "AC000E00002"]),
                        "ok": _area("AC000E00002", ["AC000E00002"]),
                    },
                    {"m1": _member("u3", "y"), "m2": _member("u4", "ok")},
                ),
            ]
        )
    )
    assert result == [("rec-a", "y"), ("rec-b", "x")]
    flat = repr(result)
    for member_value in ("u1", "u2", "u3", "u4", "sensor-"):
        assert member_value not in flat


def test_check_task_flags_but_does_not_refuse(caplog):
    rows = pipeline._flatten_to_rows(
        [
            _community(
                "rec-a",
                {
                    "bad": _area("AC000E00001", ["AC000E00002"]),
                    "ok": _area("AC000E00002", ["AC000E00002"]),
                },
                {"m1": _member("u1", "bad"), "m2": _member("u2", "ok")},
            )
        ]
    )
    with caplog.at_level("WARNING", logger=pipeline.logger.name):
        result = pipeline.check_substations.fn(rows)
    assert result.status == pipeline.PipelineStatus.COMPLETED
    assert result.details == {"areas_mismatched": 1}
    logged = caplog.text
    assert "bad" in logged and "rec-a" in logged
    for member_value in ("u1", "u2", "sensor-"):
        assert member_value not in logged


def test_ddl_adds_the_boundary_column_to_existing_tables():
    assert (
        "ALTER TABLE raw.rec_registry_mirror ADD COLUMN IF NOT EXISTS boundary_id text"
        in pipeline._DDL
    )
