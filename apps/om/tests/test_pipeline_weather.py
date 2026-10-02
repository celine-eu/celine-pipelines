"""Tests for the om-flow (weather) gold recompute window, schedule and dbt scoping.

No database and no network: the clock, the engine and every SQL call are
replaced by in-memory fakes so the bounds the tasks compute can be checked
directly.
"""

from datetime import datetime
from itertools import pairwise
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
import pipeline
import pytest
import yaml

APP_DIR = Path(__file__).resolve().parents[1]
TZ = "Europe/Rome"
HORIZON_HOURS = 72


# ---------------------------------------------------------------------------
# Clock
# ---------------------------------------------------------------------------


class TestNowLocal:
    """The clock is the current wall-clock hour, naive, in the table's tz."""

    def test_is_naive_and_floored_to_the_hour(self):
        now = pipeline._now_local(TZ)
        assert now.tzinfo is None
        assert now == now.floor("h")

    def test_matches_wall_clock_in_timezone(self):
        before = pd.Timestamp(datetime.now(ZoneInfo(TZ))).tz_localize(None).floor("h")
        now = pipeline._now_local(TZ)
        after = pd.Timestamp(datetime.now(ZoneInfo(TZ))).tz_localize(None).floor("h")
        assert before <= now <= after


# ---------------------------------------------------------------------------
# Recompute bounds (pure helper)
# ---------------------------------------------------------------------------


class TestRecomputeBounds:
    def test_anchored_on_clock_when_table_extends_into_the_future(self):
        now = pd.Timestamp("2026-10-02 08:00")
        max_processed = now + pd.Timedelta(hours=60)
        recompute_from, cutoff = pipeline._recompute_bounds(max_processed, now)
        assert recompute_from == now - pd.Timedelta(
            hours=pipeline._RECOMPUTE_WINDOW_HOURS
        )
        assert cutoff == recompute_from - pd.Timedelta(
            hours=pipeline._ROLLING_BUFFER_HOURS
        )

    def test_anchored_on_table_end_when_table_is_stale(self):
        now = pd.Timestamp("2026-10-02 08:00")
        max_processed = now - pd.Timedelta(days=5)
        recompute_from, _ = pipeline._recompute_bounds(max_processed, now)
        assert recompute_from == max_processed - pd.Timedelta(
            hours=pipeline._RECOMPUTE_WINDOW_HOURS
        )

    def test_rolling_buffer_covers_longest_rolling_window(self):
        # cumulative_hdd_48h is a 48-row rolling sum; the buffer must hold at
        # least that much history before the first recomputed row.
        assert pipeline._ROLLING_BUFFER_HOURS >= 48


def _simulate(run_times_utc, horizon_hours=HORIZON_HOURS):
    """Replay a sequence of om-flow runs against an in-memory gold table.

    Each run sees silver up to ``now + horizon_hours`` (local naive), reads
    ``max_processed`` from the gold table, computes the recompute bounds with
    the pipeline helper and records which hours it rewrites.

    Returns:
        (runs, last_rewrite) where runs is a list of
        (now, rewritten_hours) and last_rewrite maps hour -> now of the last
        run that rewrote it.
    """
    max_processed = None
    runs = []
    last_rewrite: dict[pd.Timestamp, pd.Timestamp] = {}
    for run_utc in run_times_utc:
        now = (
            pd.Timestamp(run_utc, tz="UTC").tz_convert(TZ).tz_localize(None).floor("h")
        )
        data_end = now + pd.Timedelta(hours=horizon_hours)
        if max_processed is None:
            start = now - pd.Timedelta(hours=120)
        else:
            start, _ = pipeline._recompute_bounds(max_processed, now)
        rewritten = set(pd.date_range(start, data_end, freq="h"))
        for hour in rewritten:
            last_rewrite[hour] = now
        runs.append((now, rewritten))
        max_processed = data_end if max_processed is None else max(max_processed, data_end)
    return runs, last_rewrite


def _twice_daily(start: str, days: int):
    first = pd.Timestamp(start)
    return [
        first + pd.Timedelta(days=d, hours=h) for d in range(days) for h in (6, 18)
    ]


# Spans the end of CEST (2026-10-25) so the 11 h / 13 h local gaps are covered.
_RUNS = _twice_daily("2026-10-20", 10)


class TestRecomputeSimulation:
    def test_every_hour_since_previous_run_is_rewritten(self):
        runs, _ = _simulate(_RUNS)
        for (prev_now, _), (now, rewritten) in pairwise(runs):
            expected = pd.date_range(
                prev_now, now + pd.Timedelta(hours=HORIZON_HOURS), freq="h"
            )
            missing = [h for h in expected if h not in rewritten]
            assert not missing, f"run at {now} skipped {missing[:3]}..."

    def test_last_rewrite_happens_after_the_hour_has_passed(self):
        runs, last_rewrite = _simulate(_RUNS)
        final_now = runs[-1][0]
        # Hours that a later run would still revisit are not final yet.
        settled = [
            h
            for h in last_rewrite
            if h < final_now - pd.Timedelta(hours=pipeline._RECOMPUTE_WINDOW_HOURS)
        ]
        assert settled, "simulation too short to settle any hour"
        early = [h for h in settled if last_rewrite[h] <= h]
        assert not early, f"hours finalised from a forecast: {sorted(early)[:3]}"

    def test_old_table_end_anchor_would_fail(self):
        # Guard on the premise: anchoring on max_processed alone with a 72 h
        # horizon leaves near-term hours unrefreshed.
        now = pd.Timestamp("2026-10-20 20:00")
        prev_now = now - pd.Timedelta(hours=12)
        max_processed = prev_now + pd.Timedelta(hours=HORIZON_HOURS)
        old_from = max_processed - pd.Timedelta(hours=pipeline._RECOMPUTE_WINDOW_HOURS)
        new_from, _ = pipeline._recompute_bounds(max_processed, now)
        assert old_from > now
        assert new_from <= prev_now


# ---------------------------------------------------------------------------
# Task wiring (both gold tasks use the helper and the configured timezone)
# ---------------------------------------------------------------------------


class _Logger:
    def info(self, *a, **k):
        pass

    warning = info


def _silver_frame(start, end):
    idx = pd.date_range(start, end, freq="h")
    n = len(idx)
    return pd.DataFrame(
        {
            "datetime": idx,
            "temperature_2m": [10.0] * n,
            "shortwave_radiation": [100.0] * n,
            "direct_radiation": [60.0] * n,
            "diffuse_radiation": [40.0] * n,
            "global_tilted_irradiance": [110.0] * n,
            "cloud_cover": [50.0] * n,
            "precipitation": [0.0] * n,
        }
    )


@pytest.mark.parametrize(
    "task, table_key",
    [
        (pipeline.compute_gold_features_task, "gold_raw"),
        (pipeline.compute_gold_features_meters_task, "gold_raw_meters"),
    ],
)
def test_task_recomputes_from_clock_anchor(monkeypatch, task, table_key):
    now = pd.Timestamp("2026-10-02 08:00")
    max_processed = now + pd.Timedelta(hours=60)
    expected_from, expected_cutoff = pipeline._recompute_bounds(max_processed, now)
    seen = {}

    def fake_now(tz):
        seen["tz"] = tz
        return now

    def fake_read_sql(sql, engine, params=None):
        seen["cutoff"] = params["cutoff"]
        return _silver_frame(params["cutoff"], now + pd.Timedelta(hours=72))

    def fake_upsert(df, engine, schema, table, recompute_from, recompute_to):
        seen["upsert"] = (recompute_from, recompute_to, df["datetime"].min())
        return len(df)

    def fake_load(df, cfg, table_name, schema, if_exists="append"):
        seen["append_min"] = df["datetime"].min()
        return len(df)

    monkeypatch.setattr(pipeline, "_now_local", fake_now)
    monkeypatch.setattr(pipeline, "_get_pg_engine", lambda cfg: object())
    monkeypatch.setattr(
        pipeline, "_get_max_processed_datetime", lambda *a: max_processed
    )
    monkeypatch.setattr(pipeline.pd, "read_sql", fake_read_sql)
    monkeypatch.setattr(pipeline, "_upsert_gold_rows", fake_upsert)
    monkeypatch.setattr(pipeline, "_load_to_postgres", fake_load)
    monkeypatch.setattr(pipeline, "get_run_logger", lambda: _Logger())

    result = task.fn(cfg=None)

    assert result.status == "success"
    assert seen["tz"] == pipeline._load_config()["timezone"]
    assert seen["cutoff"] == expected_cutoff
    assert seen["upsert"] == (expected_from, max_processed, expected_from)
    assert seen["append_min"] == max_processed + pd.Timedelta(hours=1)


# ---------------------------------------------------------------------------
# dbt scoping
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "task, spec",
    [
        (pipeline.transform_staging_task, "-s staging,tag:weather"),
        (pipeline.transform_silver_task, "-s silver,tag:weather"),
        (pipeline.transform_gold_task, "-s gold,tag:weather"),
        (pipeline.run_dbt_tests_task, "test -s tag:weather"),
    ],
)
def test_dbt_tasks_are_scoped_to_weather(monkeypatch, task, spec):
    calls = []
    monkeypatch.setattr(pipeline, "dbt_run", lambda s, cfg: calls.append(s))
    task.fn(cfg=None)
    assert calls == [spec]


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------


def _tap_config():
    meltano = yaml.safe_load((APP_DIR / "meltano" / "meltano.yml").read_text())
    (tap,) = [
        e for e in meltano["plugins"]["extractors"] if e["name"] == "tap-openmeteo"
    ]
    return tap["config"]


def test_schedule_is_twice_daily():
    cfg = pipeline._load_config()
    assert cfg["schedule"]["cron"] == "0 6,18 * * *"
    assert cfg["schedule"]["name"] == "om-weather-daily"


def test_timezone_matches_tap_timezone():
    assert pipeline._load_config()["timezone"] == _tap_config()["timezone"] == TZ


def test_tap_uses_icon_seamless_with_72h_horizon():
    tap = _tap_config()
    assert tap["models"] == ["icon_seamless"]
    assert tap["forecast_hours"] == HORIZON_HOURS
