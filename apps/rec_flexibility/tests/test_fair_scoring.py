"""Pure fair-gamification formulas (lib.fair_scoring), hand-computed vectors.

These functions are the Python twins of the SQL in rec_fair_weekly_metrics /
rec_fair_weekly_scores; the backtest package in demo3 holds the reference copies.
"""

from __future__ import annotations

import csv
from pathlib import Path

import pytest

from lib import fair_scoring as fs

GOLDEN = Path(__file__).parent / "fixtures" / "fair_golden_week.csv"


def test_hazen_two_distinct():
    assert fs.hazen_percentile([0.2, 0.5]) == [100.0, 0.0]


def test_hazen_three_tied():
    assert fs.hazen_percentile([0.3, 0.3, 0.3]) == [50.0, 50.0, 50.0]


def test_hazen_single_is_none():
    assert fs.hazen_percentile([0.3]) == [None]


def test_hazen_skips_none_and_keeps_positions():
    out = fs.hazen_percentile([0.4, None, 0.1, 0.4])
    assert out == [25.0, None, 100.0, 25.0]


def test_hazen_higher_is_better():
    assert fs.hazen_percentile([0.2, 0.5], lower_is_better=False) == [0.0, 100.0]


def test_level_points_saturation():
    assert [fs.level_points(p, 80) for p in (90.0, 80.0, 60.0, 20.0)] == [100.0, 100.0, 75.0, 25.0]
    assert fs.level_points(None, 80) is None


@pytest.mark.parametrize("k,n", [(1, None), (3, None), (4, 2), (5, 2), (7, 3), (8, 4), (20, 4)])
def test_growth_n(k, n):
    assert fs.growth_n(k) == n


def test_growth_delta_floor_and_value():
    assert fs.growth_delta([0.0, 0.0, 0.5, 0.5], 2) is None
    assert fs.growth_delta([1.0, 1.0, 0.8, 0.8], 2) == pytest.approx(-0.2)
    assert fs.growth_delta([5.0, 1.0, 1.0, 1.2, 1.2], 2) == pytest.approx(0.2)


def test_actions_points_capped():
    assert fs.actions_points(5, 3, points_per_kept=5, surplus_points_per_hour=2, cap=20) == 20
    assert fs.actions_points(2, 1, points_per_kept=5, surplus_points_per_hour=2, cap=20) == 12


def test_import_rise_flag_requires_both_conditions():
    assert fs.import_rise_flag(14.0, 10.0, 0.30, 0.40, 0.30)
    assert not fs.import_rise_flag(14.0, 10.0, 0.45, 0.40, 0.30)
    assert not fs.import_rise_flag(12.0, 10.0, 0.30, 0.40, 0.30)
    assert not fs.import_rise_flag(14.0, None, 0.30, 0.40, 0.30)


def test_golden_week_level_and_growth_points():
    """The demo3 golden week reproduces with the pipeline twins (percentile within group)."""
    rows = list(csv.DictReader(GOLDEN.open(encoding="utf-8")))
    by_group: dict[str, list[dict]] = {}
    for r in rows:
        by_group.setdefault(r["ref_group_effective"], []).append(r)
    for group_rows in by_group.values():
        pct = fs.hazen_percentile([float(r["level_metric"]) for r in group_rows])
        for r, p in zip(group_rows, pct):
            assert p == pytest.approx(float(r["level_pct"]))
            assert fs.level_points(p, 80) == pytest.approx(float(r["level_points"]))
        gpct = fs.hazen_percentile([float(r["growth_delta_adj"]) for r in group_rows])
        for r, p in zip(group_rows, gpct):
            assert p == pytest.approx(float(r["growth_pct"]))
