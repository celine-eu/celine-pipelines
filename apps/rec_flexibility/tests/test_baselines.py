"""Tests for baseline computation."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from lib import baselines as bl


def test_high_4_of_7_with_full_window():
    daily = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0]
    # top 4: 4, 5, 6, 7 -> mean = 5.5
    assert bl.compute_high_x_of_y(daily, select=4, candidates=7) == pytest.approx(5.5)


def test_high_4_of_7_with_fewer_days():
    daily = [1.0, 2.0, 3.0]
    assert bl.compute_high_x_of_y(daily, select=4, candidates=7) == pytest.approx(2.0)


def test_high_4_of_7_empty_returns_zero():
    assert bl.compute_high_x_of_y([], select=4, candidates=7) == 0.0


def test_winsorized_baseline_returns_dict_per_slot_weekday():
    rng = pd.date_range("2026-03-01", "2026-03-31 23:45", freq="15min", tz="UTC")
    df = pd.DataFrame(
        {
            "ts": rng,
            "consumption_kwh": np.random.RandomState(42).uniform(0.1, 0.5, len(rng)),
        }
    )
    df["slot"] = df["ts"].dt.hour * 4 + df["ts"].dt.minute // 15
    df["is_weekday"] = df["ts"].dt.dayofweek < 5
    df["date"] = df["ts"].dt.date
    out = bl.compute_winsorized_reference_baseline(df, select=4, candidates=7, min_readings=1)
    assert len(out) == 192  # 96 slots x 2 day-types
    assert all(isinstance(k, tuple) and len(k) == 2 for k in out.keys())
    assert all(v >= 0 for v in out.values())


def test_winsorized_not_systematically_higher_than_settlement():
    rng = pd.date_range("2026-03-01", "2026-03-31 23:45", freq="15min", tz="UTC")
    np.random.seed(7)
    df = pd.DataFrame(
        {
            "ts": rng,
            "consumption_kwh": np.random.uniform(0.1, 1.25, len(rng)),
        }
    )
    df["slot"] = df["ts"].dt.hour * 4 + df["ts"].dt.minute // 15
    df["is_weekday"] = df["ts"].dt.dayofweek < 5
    df["date"] = df["ts"].dt.date
    settlement = bl.compute_settlement_baseline(df, select=4, candidates=7, min_readings=1)
    reference = bl.compute_winsorized_reference_baseline(
        df, select=4, candidates=7, min_readings=1
    )
    overlapping = set(settlement.keys()) & set(reference.keys())
    assert len(overlapping) > 0
    deltas = [reference[k] - settlement[k] for k in overlapping]
    # winsorization must not systematically inflate the baseline
    assert sum(d for d in deltas if d > 0.01) <= sum(abs(d) for d in deltas if d < -0.01) + 5


# --- v2: M1-only consumption proxy ---------------------------------------------


def test_consumption_proxy_credits_export_reduction():
    # actual export below baseline -> credit the reduction as self-consumption
    proxy = bl.compute_m1_only_consumption_proxy(
        grid_import=0.4, grid_export_actual=0.1, grid_export_baseline=0.5
    )
    assert proxy == pytest.approx(0.4 + (0.5 - 0.1))


def test_consumption_proxy_no_credit_when_export_above_baseline():
    # actual export at/above baseline -> no credit, proxy == grid_import
    proxy = bl.compute_m1_only_consumption_proxy(
        grid_import=0.4, grid_export_actual=0.9, grid_export_baseline=0.5
    )
    assert proxy == pytest.approx(0.4)


def test_consumption_proxy_import_only_collapses_to_import():
    # no PV (baseline 0, actual 0) -> proxy == grid_import
    proxy = bl.compute_m1_only_consumption_proxy(
        grid_import=0.7, grid_export_actual=0.0, grid_export_baseline=0.0
    )
    assert proxy == pytest.approx(0.7)


def test_consumption_proxy_vectorised():
    gi = np.array([0.4, 0.4, 0.7])
    ga = np.array([0.1, 0.9, 0.0])
    gb = np.array([0.5, 0.5, 0.0])
    out = bl.compute_m1_only_consumption_proxy(gi, ga, gb)
    np.testing.assert_allclose(out, [0.8, 0.4, 0.7])


# --- v2: median grid-export baseline -------------------------------------------


def test_median_baseline_per_slot_weekday():
    rng = pd.date_range("2026-03-01", "2026-03-31 23:45", freq="15min", tz="UTC")
    df = pd.DataFrame({"ts": rng, "grid_export_kwh": np.arange(len(rng), dtype=float) % 7})
    df["slot"] = df["ts"].dt.hour * 4 + df["ts"].dt.minute // 15
    df["is_weekday"] = df["ts"].dt.dayofweek < 5
    out = bl.compute_median_baseline(df, value_col="grid_export_kwh")
    assert len(out) == 192  # 96 slots x 2 day-types
    assert all(isinstance(k, tuple) and len(k) == 2 for k in out)
    # spot-check: median equals pandas median for one bucket
    k = next(iter(out))
    sub = df[(df["slot"] == k[0]) & (df["is_weekday"] == k[1])]["grid_export_kwh"]
    assert out[k] == pytest.approx(float(sub.median()))


def test_median_baseline_empty():
    empty = pd.DataFrame(columns=["slot", "is_weekday", "grid_export_kwh"])
    assert bl.compute_median_baseline(empty) == {}


# --- v3: upward spread (window-promise potential) -------------------------------


class TestComputeUpwardSpread:
    """v3 window-promise spread: p_hi − p_lo of daily per-slot values, clipped at 0."""

    @staticmethod
    def _frame(rows: list[dict]) -> pd.DataFrame:
        return pd.DataFrame(rows)

    def test_empty_frame_returns_empty_dict(self) -> None:
        empty = pd.DataFrame(columns=["slot", "is_weekday", "date", "consumption_kwh"])
        assert bl.compute_upward_spread(empty) == {}

    def test_constant_series_has_zero_spread(self) -> None:
        rows = [
            {"slot": 40, "is_weekday": True, "date": f"2026-06-{d:02d}", "consumption_kwh": 0.2}
            for d in range(1, 11)
        ]
        result = bl.compute_upward_spread(self._frame(rows))
        assert result == {(40, True): 0.0}

    def test_spread_is_quantile_difference(self) -> None:
        # Daily values 0.1..1.0 → p75 = 0.775, p50 = 0.55 → spread 0.225
        rows = [
            {"slot": 40, "is_weekday": True, "date": f"2026-06-{d:02d}", "consumption_kwh": d / 10}
            for d in range(1, 11)
        ]
        result = bl.compute_upward_spread(self._frame(rows), q_hi=0.75, q_lo=0.5)
        assert result[(40, True)] == pytest.approx(0.225)

    def test_clear_top_restricts_to_highest_export_days(self) -> None:
        # 4 days: consumption 0.1/0.2/0.3/0.4, export high on the 0.3 & 0.4 days.
        # clear_top=2 keeps only those two days → for a 2-point sorted array,
        # quantile(q) = min + q*(max-min), so spread = (q_hi-q_lo)*(max-min)
        # = (0.75-0.5)*(0.4-0.3) = 0.025 (p75=0.375, p50=0.35 of [0.3, 0.4]).
        rows = []
        for d, (cons, exp) in enumerate(
            [(0.1, 0.0), (0.2, 0.0), (0.3, 5.0), (0.4, 6.0)], start=1
        ):
            rows.append(
                {
                    "slot": 40,
                    "is_weekday": True,
                    "date": f"2026-06-{d:02d}",
                    "consumption_kwh": cons,
                    "grid_export_kwh": exp,
                }
            )
        full = bl.compute_upward_spread(self._frame(rows), clear_top=None)
        clear = bl.compute_upward_spread(self._frame(rows), clear_top=2)
        assert clear[(40, True)] == pytest.approx(0.025)
        assert clear[(40, True)] != full[(40, True)]

    def test_spread_never_negative(self) -> None:
        rows = [
            {"slot": 8, "is_weekday": False, "date": f"2026-06-{d:02d}", "consumption_kwh": v}
            for d, v in enumerate([1.0, 0.0, 0.0, 0.0, 0.0], start=1)
        ]
        result = bl.compute_upward_spread(self._frame(rows), q_hi=0.25, q_lo=0.75)
        assert result[(8, False)] == 0.0
