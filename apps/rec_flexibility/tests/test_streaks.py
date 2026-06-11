"""Tests for streak update logic."""

from __future__ import annotations

import pytest

from lib import streaks as st


def test_first_response_initializes_level_to_one():
    new = st.update_streaks(
        prev_levels={},
        responses={"dev-A": True},
        increment=1,
        decay_per_period=1,
        max_level=10,
        floor_fraction=0.5,
    )
    assert new["dev-A"]["level"] == 1
    assert new["dev-A"]["peak"] == 1


def test_response_increments_level():
    new = st.update_streaks(
        prev_levels={"dev-A": {"level": 3, "peak": 3}},
        responses={"dev-A": True},
        increment=1,
        decay_per_period=1,
        max_level=10,
        floor_fraction=0.5,
    )
    assert new["dev-A"]["level"] == 4
    assert new["dev-A"]["peak"] == 4


def test_no_response_decays_level():
    new = st.update_streaks(
        prev_levels={"dev-A": {"level": 5, "peak": 8}},
        responses={"dev-A": False},
        increment=1,
        decay_per_period=1,
        max_level=10,
        floor_fraction=0.5,
    )
    assert new["dev-A"]["level"] == 4
    assert new["dev-A"]["peak"] == 8


def test_decay_does_not_drop_below_floor():
    new = st.update_streaks(
        prev_levels={"dev-A": {"level": 5, "peak": 10}},  # floor = 5
        responses={"dev-A": False},
        increment=1,
        decay_per_period=1,
        max_level=10,
        floor_fraction=0.5,
    )
    assert new["dev-A"]["level"] == 5


def test_level_caps_at_max():
    new = st.update_streaks(
        prev_levels={"dev-A": {"level": 10, "peak": 10}},
        responses={"dev-A": True},
        increment=1,
        decay_per_period=1,
        max_level=10,
        floor_fraction=0.5,
    )
    assert new["dev-A"]["level"] == 10


def test_streak_multiplier_formula():
    assert st.streak_multiplier(0, increment=0.05, max_multiplier=1.5) == 1.0
    assert st.streak_multiplier(5, increment=0.05, max_multiplier=1.5) == pytest.approx(1.25)
    assert st.streak_multiplier(10, increment=0.05, max_multiplier=1.5) == 1.5
    assert st.streak_multiplier(20, increment=0.05, max_multiplier=1.5) == 1.5


def test_unknown_device_with_no_response_returns_baseline():
    new = st.update_streaks(
        prev_levels={},
        responses={"dev-A": False},
        increment=1,
        decay_per_period=1,
        max_level=10,
        floor_fraction=0.5,
    )
    assert new["dev-A"]["level"] == 0
    assert new["dev-A"]["peak"] == 0


def test_cold_start_seeds_active_fleet_at_zero():
    """With no prior state and no responses, the active fleet is still seeded.

    Guards the cold-start deadlock: previously the device universe was
    ``set(prev) | set(responses)``, so an empty table with no bonus events
    produced an empty table forever (DELETE + insert nothing).
    """
    new = st.update_streaks(
        prev_levels={},
        responses={},
        increment=1,
        decay_per_period=1,
        max_level=10,
        floor_fraction=0.5,
        devices=["dev-A", "dev-B", "dev-C"],
    )
    assert set(new) == {"dev-A", "dev-B", "dev-C"}
    for state in new.values():
        assert state == {"level": 0, "peak": 0}


def test_seeded_active_device_that_responds_still_increments():
    """A device passed via ``devices`` that also responded gains a level."""
    new = st.update_streaks(
        prev_levels={},
        responses={"dev-A": True},
        increment=1,
        decay_per_period=1,
        max_level=10,
        floor_fraction=0.5,
        devices=["dev-A", "dev-B"],
    )
    assert new["dev-A"]["level"] == 1
    assert new["dev-B"]["level"] == 0


def test_devices_param_defaults_to_no_seeding():
    """Omitting ``devices`` preserves the prior union-of-prev-and-responses behaviour."""
    new = st.update_streaks(
        prev_levels={},
        responses={"dev-A": True},
        increment=1,
        decay_per_period=1,
        max_level=10,
        floor_fraction=0.5,
    )
    assert set(new) == {"dev-A"}
