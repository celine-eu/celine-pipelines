"""Tests for the meter loader (rec_meters_15m -> analysis-ready 15-min frame)."""

from __future__ import annotations

import pandas as pd
import pytest

from lib import meters as m

_METER_COLUMNS = [
    "device_id",
    "ts",
    "consumption_kwh",
    "production_kwh",
    "pv_production_kwh",
    "self_consumed_kwh",
    "total_consumption_kwh",
]


class _Conn:
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class _Engine:
    def connect(self):
        return _Conn()


def _view_frame() -> pd.DataFrame:
    """Rows shaped like rec_meters_15m (kWh per 15-min bucket, already merged)."""
    base = pd.Timestamp("2026-04-01 12:00", tz="UTC")
    rows = [
        # consumption, grid export, gross pv, self consumed (clipped), total
        ("dev-A", base, 1.0, 0.2, 0.9, 0.7, 1.7),
        ("dev-A", base + pd.Timedelta(minutes=15), 2.0, 0.0, 0.5, 0.5, 2.5),
    ]
    return pd.DataFrame(rows, columns=_METER_COLUMNS)


def test_load_meters_returns_kwh_columns_unconverted(monkeypatch):
    """load_meters must pass values through untouched — no x0.25, no renaming."""
    captured: dict[str, object] = {}

    def fake_read_sql(sql, conn, params=None):
        captured["sql"] = str(sql)
        captured["params"] = params
        return _view_frame()

    monkeypatch.setattr(pd, "read_sql", fake_read_sql)

    df = m.load_meters(_Engine(), lookback_days=7, devices=["dev-A"])

    assert list(df.columns) == _METER_COLUMNS
    assert df["consumption_kwh"].tolist() == [1.0, 2.0]
    assert df["total_consumption_kwh"].tolist() == [1.7, 2.5]
    assert str(df["ts"].dt.tz) == "UTC"
    # query targets the view, scoped by lookback + devices, no unit conversion
    assert m.METERS_VIEW in captured["sql"]
    assert "0.25" not in captured["sql"]
    assert captured["params"] == {"lookback": 7, "devices": ["dev-A"]}


def test_load_meters_omits_device_filter_when_none(monkeypatch):
    captured: dict[str, object] = {}

    def fake_read_sql(sql, conn, params=None):
        captured["sql"] = str(sql)
        captured["params"] = params
        return _view_frame()

    monkeypatch.setattr(pd, "read_sql", fake_read_sql)

    m.load_meters(_Engine(), lookback_days=30)
    assert "any(" not in str(captured["sql"])
    assert captured["params"] == {"lookback": 30}


def test_add_time_features():
    df = _view_frame().iloc[:1].copy()
    df["ts"] = pd.Timestamp("2026-04-01 12:30", tz="UTC")
    df = m.add_time_features(df)
    assert df.loc[0, "slot"] == 50  # 12*4 + 30//15 = 50
    assert df.loc[0, "is_weekday"]  # 2026-04-01 is Wednesday
    assert df.loc[0, "date"] == pd.Timestamp("2026-04-01").date()
    assert df.loc[0, "hour"] == 12


def test_view_frame_invariants():
    """The contract rec_meters_15m guarantees (enforced by dbt tests upstream)."""
    df = _view_frame()
    assert (df["self_consumed_kwh"] >= 0).all()
    expected_total = df["consumption_kwh"] + df["self_consumed_kwh"]
    assert df["total_consumption_kwh"].tolist() == pytest.approx(expected_total.tolist())
