"""The mirror lists only the delivery points in service.

A delivery point flagged `active: false` in the registry is a supply point no
longer in service. rec_it's rec_member_supply_points unnests delivery_point_ids
into the list a community hands its distributor, so a retired point mirrored as
live asks the distributor to release readings against a dead POD.

Synthetic bundles only: placeholder PODs, no real community or member.
"""

from __future__ import annotations

from flows import pipeline


def _bundle(points: list[dict], status: str = "active") -> list[dict]:
    return [
        {
            "community": {"id": "example-rec", "areas": {}},
            "members": {
                "ex-00001": {
                    "user_id": "ex-00001",
                    "status": status,
                    "role": "consumer",
                    "type": "person",
                    "delivery_points": points,
                    "assets": {"meter": {"m1": {"sensor_id": "sensor-ex-00001"}}},
                }
            },
        }
    ]


def _dp(pod: str, **extra) -> dict:
    return {"id": pod, "type": "pod", **extra}


def test_an_inactive_delivery_point_is_not_mirrored():
    rows = pipeline._flatten_to_rows(
        _bundle(
            [_dp("IT001E00000001", active=False), _dp("IT001E00000002", active=True)]
        )
    )

    assert [r["delivery_point_ids"] for r in rows] == [["IT001E00000002"]]


def test_a_point_without_the_flag_is_active():
    """The registry model defaults `active` to true."""
    rows = pipeline._flatten_to_rows(_bundle([_dp("IT001E00000001")]))

    assert rows[0]["delivery_point_ids"] == ["IT001E00000001"]


def test_a_member_whose_every_point_is_inactive_keeps_its_row():
    """The member is still active and its sensors still count; only the
    supply-point list is empty, which rec_member_supply_points already skips."""
    rows = pipeline._flatten_to_rows(_bundle([_dp("IT001E00000001", active=False)]))

    assert len(rows) == 1
    assert rows[0]["delivery_point_ids"] == []
    assert rows[0]["sensor_ids"] == ["sensor-ex-00001"]


def test_a_member_without_delivery_points_mirrors_an_empty_list():
    for points in ([], None):
        rows = pipeline._flatten_to_rows(_bundle(points))
        assert rows[0]["delivery_point_ids"] == []
