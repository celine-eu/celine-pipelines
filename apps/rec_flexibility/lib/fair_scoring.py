"""Pure formulas of the fair gamification (spec 2026-10-07, design 2026-10-09).

Python twins of the SQL in rec_fair_weekly_metrics / rec_fair_weekly_scores, kept so the
formulas can be unit-tested and checked for equivalence (tests/test_python_sql_equivalence.py).
The reference implementation lives in demo3 ``src/analysis/fair_gamification/scoring.py``.
"""

from __future__ import annotations


def hazen_percentile(values: list[float | None], lower_is_better: bool = True) -> list[float | None]:
    """Percentile of each value within the list: worse members plus half the ties.

    ``pct = 100 × (worse + 0.5 × tied) / (n − 1)``; None when fewer than two values.
    None inputs stay None and do not count.
    """
    present = [v for v in values if v is not None]
    n = len(present)
    out: list[float | None] = []
    for v in values:
        if v is None or n < 2:
            out.append(None)
            continue
        worse = sum(1 for x in present if (x > v if lower_is_better else x < v))
        tied = sum(1 for x in present if x == v) - 1
        out.append(100.0 * (worse + 0.5 * tied) / (n - 1))
    return out


def level_points(pct: float | None, saturation_pct: int) -> float | None:
    """Level points: the percentile rescaled so ``saturation_pct`` and above give 100."""
    if pct is None:
        return None
    return min(100.0, pct / saturation_pct * 100.0)


def growth_n(valid_weeks_so_far: int, min_n: int = 2, max_n: int = 4) -> int | None:
    """Growth window length for the valid weeks available (None below ``2 × min_n``)."""
    if valid_weeks_so_far < 2 * min_n:
        return None
    return max(min_n, min(max_n, valid_weeks_so_far // 2))


def growth_delta(night_kwh_h: list[float], n: int, prev_floor: float = 0.02) -> float | None:
    """Relative change of the last ``n`` values against the ``n`` before (None below the floor)."""
    if len(night_kwh_h) < 2 * n:
        return None
    recent = sum(night_kwh_h[-n:]) / n
    prev = sum(night_kwh_h[-2 * n : -n]) / n
    if prev < prev_floor:
        return None
    return (recent - prev) / prev


def actions_points(kept: int, surplus_hours: int, *, points_per_kept: int, surplus_points_per_hour: int, cap: int) -> int:
    """Capped weekly Actions points."""
    return min(cap, kept * points_per_kept + surplus_hours * surplus_points_per_hour)


def import_rise_flag(
    import_kwh: float, median_8w: float | None, night_share: float, night_share_prev: float | None, pct: float
) -> bool:
    """Daytime-waste flag: total import above ``(1 + pct) ×`` median while the night share fell."""
    if median_8w is None or night_share_prev is None:
        return False
    return import_kwh > (1.0 + pct) * median_8w and night_share < night_share_prev
