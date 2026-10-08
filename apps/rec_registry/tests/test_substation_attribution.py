"""The mirror carries each area's boundary id, and flags areas whose
substation_id (topology_ids[1], as rec_it reads it) is not that boundary id.

Synthetic bundles only: AC000E0000x codes, no real community or member.
"""

from __future__ import annotations

import yaml
from flows import pipeline

SOURCE = "gse_cabine_primarie"


def _area(boundary_id: str | None, topology: list[str] | None) -> dict:
    area: dict = {"name": "Area"}
    if boundary_id is not None:
        area["boundary"] = {"source": SOURCE, "id": boundary_id}
    if topology is not None:
        area["topology"] = topology
    return area


def _member(user_id: str, area: str | None, status: str = "active") -> dict:
    member: dict = {
        "user_id": user_id,
        "status": status,
        "role": "consumer",
        "type": "person",
        "delivery_points": [{"id": f"dp-{user_id}"}],
        "assets": {"meter": {"m1": {"sensor_id": f"sensor-{user_id}"}}},
    }
    if area is not None:
        member["area"] = area
    return member


def _bundle(community_id: str, areas: dict, members: dict) -> dict:
    return {"community": {"id": community_id, "areas": areas}, "members": members}


def _rows_by_user(bundles: list[dict]) -> dict[str, dict]:
    return {r["user_id"]: r for r in pipeline._flatten_to_rows(bundles)}


# --- the boundary id is carried into the mirror -----------------------------


def test_row_carries_its_areas_boundary_id():
    rows = _rows_by_user(
        [
            _bundle(
                "rec-a",
                {"north": _area("AC000E00001", ["AC000E00001"])},
                {"m1": _member("u1", "north")},
            )
        ]
    )
    assert rows["u1"]["boundary_id"] == "AC000E00001"
    assert rows["u1"]["topology_ids"] == ["AC000E00001"]


def test_area_without_boundary_mirrors_a_null_boundary_id():
    rows = _rows_by_user(
        [
            _bundle(
                "rec-a",
                {"legacy": _area(None, ["n1", "n2"])},
                {"m1": _member("u1", "legacy")},
            )
        ]
    )
    assert rows["u1"]["boundary_id"] is None
    assert rows["u1"]["topology_ids"] == ["n1", "n2"]


def test_member_without_area_or_with_unknown_area_has_no_boundary():
    rows = _rows_by_user(
        [
            _bundle(
                "rec-a",
                {"north": _area("AC000E00001", ["AC000E00001"])},
                {"m1": _member("u1", None), "m2": _member("u2", "gone")},
            )
        ]
    )
    for user in ("u1", "u2"):
        assert rows[user]["boundary_id"] is None
        assert rows[user]["topology_ids"] == []


def test_malformed_boundaries_are_treated_as_absent():
    for boundary in (
        None,
        [],
        "AC000E00001",
        {"source": SOURCE},
        {"id": "  "},
        {"id": 7},
    ):
        area = {"name": "x", "boundary": boundary, "topology": ["AC000E00001"]}
        rows = _rows_by_user(
            [_bundle("rec-a", {"x": area}, {"m1": _member("u1", "x")})]
        )
        assert rows["u1"]["boundary_id"] is None, boundary


def test_null_topology_mirrors_an_empty_list():
    area = {
        "name": "x",
        "boundary": {"source": SOURCE, "id": "AC000E00001"},
        "topology": None,
    }
    rows = _rows_by_user([_bundle("rec-a", {"x": area}, {"m1": _member("u1", "x")})])
    assert rows["u1"]["topology_ids"] == []


def test_inactive_members_are_still_skipped():
    rows = _rows_by_user(
        [
            _bundle(
                "rec-a",
                {"north": _area("AC000E00001", ["AC000E00001"])},
                {"m1": _member("u1", "north", status="suspended")},
            )
        ]
    )
    assert rows == {}


def test_v07_export_yaml_round_trips_through_parse_and_flatten():
    text = yaml.safe_dump_all(
        [
            _bundle(
                "rec-a",
                {"north": _area("AC000E00001", ["AC000E00001"])},
                {"m1": _member("u1", "north")},
            ),
            _bundle(
                "rec-b",
                {"south": _area("AC000E00002", ["AC000E00002"])},
                {"m1": _member("u2", "south")},
            ),
        ]
    )
    rows = _rows_by_user(pipeline._parse_bundles(text))
    assert {u: r["boundary_id"] for u, r in rows.items()} == {
        "u1": "AC000E00001",
        "u2": "AC000E00002",
    }
    assert pipeline._substation_mismatches(list(rows.values())) == []


# --- substation_id must equal the area's boundary id ------------------------


def _mismatches(areas: dict, members: dict, community_id: str = "rec-a"):
    return pipeline._substation_mismatches(
        pipeline._flatten_to_rows([_bundle(community_id, areas, members)])
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


def test_each_offending_area_is_reported_once_and_names_no_member():
    result = pipeline._substation_mismatches(
        pipeline._flatten_to_rows(
            [
                _bundle(
                    "rec-b",
                    {"x": _area("AC000E00003", ["AC000E00004"])},
                    {"m1": _member("u1", "x"), "m2": _member("u2", "x")},
                ),
                _bundle(
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
    for member_value in ("u1", "u2", "u3", "u4", "sensor-", "dp-"):
        assert member_value not in flat


def test_check_task_flags_but_does_not_refuse(caplog):
    rows = pipeline._flatten_to_rows(
        [
            _bundle(
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
    for member_value in ("u1", "u2", "sensor-", "dp-"):
        assert member_value not in logged


def test_ddl_adds_the_boundary_column_to_existing_tables():
    assert (
        "ALTER TABLE raw.rec_registry_mirror ADD COLUMN IF NOT EXISTS boundary_id text"
        in pipeline._DDL
    )
