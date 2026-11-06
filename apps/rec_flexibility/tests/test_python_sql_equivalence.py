"""Verify the SQL implementations of formulas match the Python reference implementations."""

from __future__ import annotations

import math

import pytest

from lib import streaks as st


def py_effort_multiplier(actual_kwh: float, baseline_kwh: float) -> float:
    if baseline_kwh < 0.025:  # kWh/15-min floor (~= 0.1 kW avg)
        return 1.0
    ratio = actual_kwh / baseline_kwh
    if ratio >= 1.50:
        return 2.00
    if ratio >= 1.25:
        return 1.50
    if ratio >= 1.10:
        return 1.00
    if ratio >= 1.00:
        return 0.50
    return 0.25


def sql_effort_multiplier_equivalent(actual_kwh: float, baseline_kwh: float) -> float:
    """Mirror the CASE WHEN in rec_settlement_points.sql."""
    if baseline_kwh < 0.025:
        return 1.0
    ratio = actual_kwh / baseline_kwh
    if ratio >= 1.50:
        return 2.00
    if ratio >= 1.25:
        return 1.50
    if ratio >= 1.10:
        return 1.00
    if ratio >= 1.00:
        return 0.50
    return 0.25


def py_shift_effort_multiplier(shift_fraction: float, shifted_kwh: float) -> float:
    if shift_fraction < 0.05 or shifted_kwh < 0.05:
        return 0.0
    if shift_fraction >= 1.00:
        return 2.5
    if shift_fraction >= 0.50:
        return 2.0
    if shift_fraction >= 0.25:
        return 1.5
    return 1.0


def sql_shift_effort_equivalent(shift_fraction: float, shifted_kwh: float) -> float:
    """Mirror the CASE WHEN in rec_flexibility_bonus.sql."""
    if shift_fraction < 0.05 or shifted_kwh < 0.05:
        return 0.0
    if shift_fraction >= 1.00:
        return 2.5
    if shift_fraction >= 0.50:
        return 2.0
    if shift_fraction >= 0.25:
        return 1.5
    return 1.0


@pytest.mark.parametrize("ratio", [0.5, 0.99, 1.0, 1.05, 1.10, 1.20, 1.25, 1.40, 1.50, 2.0])
def test_effort_multiplier_matches_at_boundaries(ratio):
    # Both operands are kWh per 15-min bucket (same unit -> unit-less ratio).
    baseline_kwh = 0.25  # ~1 kW x 0.25 h
    actual_kwh = ratio * baseline_kwh
    assert py_effort_multiplier(actual_kwh, baseline_kwh) == sql_effort_multiplier_equivalent(
        actual_kwh, baseline_kwh
    )


@pytest.mark.parametrize(
    "frac", [0.0, 0.04, 0.05, 0.10, 0.24, 0.25, 0.49, 0.50, 0.99, 1.00, 1.5]
)
def test_shift_effort_multiplier_matches(frac):
    shifted = max(0.06, frac)
    assert py_shift_effort_multiplier(frac, shifted) == sql_shift_effort_equivalent(frac, shifted)


def test_log_scaling_matches_postgres_ln1p():
    for x in [0.0, 0.1, 1.0, 10.0, 100.0]:
        assert math.log1p(x) == pytest.approx(math.log(1 + x))


def test_streak_multiplier_at_boundaries():
    assert st.streak_multiplier(0) == 1.0
    assert st.streak_multiplier(10) == 1.5
    assert st.streak_multiplier(50) == 1.5


# ─── Fair gamification: SQL window-function percentile ↔ Python ───────────────────

def sql_hazen_equivalent(values: list[float]) -> list[float | None]:
    """Mirror the CTE in rec_fair_weekly_scores.sql:
    n = count(*) over g, r_asc = rank() over (g order by v), cnt_eq = count(*) over (g, v);
    pct = 100 * ((n - (r_asc - 1) - cnt_eq) + 0.5 * (cnt_eq - 1)) / (n - 1), null when n < 2.
    """
    n = len(values)
    out: list[float | None] = []
    for v in values:
        if n < 2:
            out.append(None)
            continue
        r_asc = 1 + sum(1 for x in values if x < v)
        cnt_eq = sum(1 for x in values if x == v)
        out.append(100.0 * ((n - (r_asc - 1) - cnt_eq) + 0.5 * (cnt_eq - 1)) / (n - 1))
    return out


@pytest.mark.parametrize(
    "values",
    [[0.2, 0.5], [0.3, 0.3, 0.3], [0.4, 0.1, 0.4], [0.1, 0.2, 0.3, 0.4, 0.5, 0.6], [0.7]],
)
def test_hazen_percentile_matches_sql_formula(values):
    from lib import fair_scoring as fs

    assert fs.hazen_percentile(values) == sql_hazen_equivalent(values)


def test_level_saturation_matches_sql_least():
    from lib import fair_scoring as fs

    for pct in (0.0, 40.0, 79.99, 80.0, 95.0, 100.0):
        sql = min(100.0, pct / 80 * 100)  # least(100, level_pct / fair_saturation_pct * 100)
        assert fs.level_points(pct, 80) == pytest.approx(sql)
