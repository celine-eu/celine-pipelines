"""Streak state management for the flexibility bonus.

State persists in ``{CELINE_GOLD_SCHEMA}._rec_device_streaks_raw`` (one row per device,
rewritten weekly). Each call to :func:`update_streaks` reads previous state and
produces the next state given the week's response signal.
"""

from __future__ import annotations

import pandas as pd

StreakState = dict[str, dict[str, int]]


def update_streaks(
    prev_levels: StreakState,
    responses: dict[str, bool],
    increment: int = 1,
    decay_per_period: int = 1,
    max_level: int = 10,
    floor_fraction: float = 0.5,
    devices: list[str] | None = None,
) -> StreakState:
    """Update streak levels for one decay period (weekly).

    Args:
        prev_levels: Previous state ``{device_id: {"level": int, "peak": int}}``.
        responses: Current period response signal ``{device_id: bool}``.
        increment: Levels gained on a qualifying response.
        decay_per_period: Levels lost per period without response.
        max_level: Hard cap (corresponds to the configured ``max_multiplier``).
        floor_fraction: Decay floor as fraction of peak (e.g. 0.5 = never drop
            below half peak).
        devices: Optional active-fleet universe to always seed. Any listed device
            absent from both ``prev_levels`` and ``responses`` is initialised at
            level 0. Without this, a cold start (empty state + no bonus events)
            yields an empty result and the streak table can never bootstrap.

    Returns:
        New state ``{device_id: {"level": int, "peak": int}}``.
    """
    new: StreakState = {}
    all_devices = set(prev_levels.keys()) | set(responses.keys()) | set(devices or [])
    for device_id in all_devices:
        prev = prev_levels.get(device_id, {"level": 0, "peak": 0})
        responded = responses.get(device_id, False)
        if responded:
            new_level = min(prev["level"] + increment, max_level)
            new_peak = max(prev["peak"], new_level)
        else:
            floor = int(round(prev["peak"] * floor_fraction))
            new_level = max(prev["level"] - decay_per_period, floor)
            new_peak = prev["peak"]
        new[device_id] = {"level": new_level, "peak": new_peak}
    return new


def streak_multiplier(level: int, increment: float = 0.05, max_multiplier: float = 1.5) -> float:
    """Convert a streak level to a multiplier: ``1.0 + level * increment``, capped."""
    return float(min(1.0 + level * increment, max_multiplier))


def state_to_dataframe(state: StreakState, computed_at: pd.Timestamp) -> pd.DataFrame:
    """Flatten streak state to a DataFrame for DB write."""
    rows = [
        {
            "device_id": device_id,
            "level": int(s["level"]),
            "peak": int(s["peak"]),
            "multiplier": streak_multiplier(s["level"]),
            "computed_at": computed_at,
        }
        for device_id, s in state.items()
    ]
    return pd.DataFrame(rows)


def state_from_dataframe(df: pd.DataFrame) -> StreakState:
    """Load streak state from a DataFrame (e.g. after reading from DB)."""
    if df.empty:
        return {}
    return {
        row["device_id"]: {"level": int(row["level"]), "peak": int(row["peak"])}
        for _, row in df.iterrows()
    }
