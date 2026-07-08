"""Baseline computation for the rec_flexibility points system.

Two baseline types are computed:
- Settlement baseline: rolling High 4 of 7 (used for effort multiplier).
- Reference baseline: winsorized High 4 of 7 over 90-day lookback (used for bonus).

Input DataFrames carry ``consumption_kwh`` = energy consumed in that 15-min bucket,
read as-is from ``rec_meters_15m`` (over ``ds_dev_gold.meters_data_15m``); the whole
chain is kWh end-to-end with no unit conversion.
"""

from __future__ import annotations

import numpy as np
import pandas as pd


def compute_high_x_of_y(
    daily_values: list[float] | np.ndarray,
    select: int = 4,
    candidates: int = 7,
) -> float:
    """Return the mean of the top ``select`` values from the last ``candidates`` entries.

    Args:
        daily_values: Sequence of daily aggregate values (most recent last).
        select: Number of top values to average.
        candidates: Number of recent entries to consider.

    Returns:
        Mean of top ``select`` values; if fewer than ``select`` values exist,
        averages whatever is available; returns 0.0 for empty input.
    """
    arr = np.asarray(list(daily_values)[-candidates:], dtype=float)
    if arr.size == 0:
        return 0.0
    k = min(select, arr.size)
    top_k = np.sort(arr)[-k:]
    return float(top_k.mean())


def _baseline_per_slot_weekday(
    device_data: pd.DataFrame,
    select: int,
    candidates: int,
    min_readings: int,
    winsorize_pct: float | None,
) -> dict[tuple[int, bool], float]:
    """Build ``{(slot, is_weekday): baseline_kwh_per_bucket}`` from a per-device frame.

    For each (slot, is_weekday) bucket, aggregates daily values (mean across the day's
    occurrences of that slot), optionally winsorizes extremes, and applies High X/Y.
    """
    out: dict[tuple[int, bool], float] = {}
    if device_data.empty:
        return out

    grp = (
        device_data.groupby(["slot", "is_weekday", "date"], sort=True)
        .agg(kwh=("consumption_kwh", "mean"), readings=("consumption_kwh", "size"))
        .reset_index()
    )

    grp = grp[grp["readings"] >= max(1, min_readings // 96)]

    for (slot, is_wkday), bucket in grp.groupby(["slot", "is_weekday"], sort=False):
        daily = bucket["kwh"].to_numpy(dtype=float)
        if daily.size == 0:
            continue
        if winsorize_pct and 0.0 < winsorize_pct < 0.5 and daily.size >= 4:
            lo = np.quantile(daily, winsorize_pct)
            hi = np.quantile(daily, 1.0 - winsorize_pct)
            daily = daily[(daily >= lo) & (daily <= hi)]
            if daily.size == 0:
                continue
        out[(int(slot), bool(is_wkday))] = compute_high_x_of_y(daily, select, candidates)
    return out


def compute_settlement_baseline(
    device_data: pd.DataFrame,
    select: int = 4,
    candidates: int = 7,
    min_readings: int = 90,
) -> dict[tuple[int, bool], float]:
    """Return rolling High 4 of 7 baseline per (slot, is_weekday), kWh per 15-min bucket."""
    return _baseline_per_slot_weekday(
        device_data, select, candidates, min_readings, winsorize_pct=None
    )


def compute_winsorized_reference_baseline(
    device_data: pd.DataFrame,
    select: int = 4,
    candidates: int = 7,
    min_readings: int = 90,
    winsorize_pct: float = 0.05,
) -> dict[tuple[int, bool], float]:
    """Return High 4 of 7 baseline after trimming top/bottom ``winsorize_pct`` of daily values."""
    return _baseline_per_slot_weekday(
        device_data, select, candidates, min_readings, winsorize_pct=winsorize_pct
    )


def compute_median_baseline(
    device_data: pd.DataFrame,
    value_col: str = "grid_export_kwh",
) -> dict[tuple[int, bool], float]:
    """Return the per (slot, is_weekday) median of ``value_col``.

    Used as the v2 grid-export reference for the M1-only consumption proxy. Unlike
    the High X/Y settlement baselines, the export reference is a robust central
    tendency (median) of what the device historically exported in that slot — the
    proxy credits *reductions* below this typical export as self-consumption.

    Args:
        device_data: Per-device frame with ``slot``, ``is_weekday`` and ``value_col``.
        value_col: Column to take the median of (default ``grid_export_kwh``).

    Returns:
        ``{(slot, is_weekday): median_value}``; empty dict for an empty frame.
    """
    out: dict[tuple[int, bool], float] = {}
    if device_data.empty:
        return out
    grp = device_data.groupby(["slot", "is_weekday"])[value_col].median()
    for (slot, is_wkday), val in grp.items():
        out[(int(slot), bool(is_wkday))] = float(val)
    return out


def compute_m1_only_consumption_proxy(
    grid_import: float | np.ndarray,
    grid_export_actual: float | np.ndarray,
    grid_export_baseline: float | np.ndarray,
) -> float | np.ndarray:
    """Proxy consumption for M1-only devices (no behind-meter PV decomposition).

    M1-only devices report only grid import/export, so true ``total_consumption`` is
    unobservable. The proxy treats any *reduction* of export below the device's
    typical (baseline) export as self-consumption that should be rewarded::

        proxy = grid_import + max(0, grid_export_baseline - grid_export_actual)

    When the device exports at/above baseline there is no credit (proxy == import);
    an import-only device (baseline == actual == 0) collapses to ``grid_import``.

    Args:
        grid_import: Grid import kWh per bucket (scalar or array).
        grid_export_actual: Observed grid export kWh per bucket.
        grid_export_baseline: Typical/median grid export kWh per bucket.

    Returns:
        Proxy consumption, same shape as the inputs (float for scalars).
    """
    export_reduction = np.clip(
        np.asarray(grid_export_baseline, dtype=float) - np.asarray(grid_export_actual, dtype=float),
        a_min=0.0,
        a_max=None,
    )
    result = np.asarray(grid_import, dtype=float) + export_reduction
    return float(result) if np.ndim(result) == 0 else result


def baselines_to_dataframe(
    baselines: dict[tuple[int, bool], float],
    device_id: str,
    baseline_type: str,
    computed_at: pd.Timestamp,
) -> pd.DataFrame:
    """Flatten ``{(slot, is_weekday): kwh_per_15min}`` into a tidy DataFrame for DB insert."""
    rows = [
        {
            "device_id": device_id,
            "baseline_type": baseline_type,
            "slot": slot,
            "is_weekday": is_wkday,
            "baseline_kwh": kwh,
            "computed_at": computed_at,
        }
        for (slot, is_wkday), kwh in baselines.items()
    ]
    return pd.DataFrame(rows)
