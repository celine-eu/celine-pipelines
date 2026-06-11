"""Tests for the meter loader (silver -> analysis-ready 15-min frame)."""

from __future__ import annotations

import pandas as pd
import pytest

from lib import meters as m


def _synthetic_silver_rows() -> pd.DataFrame:
    """3 meter_types x 2 timestamps x 1 device."""
    base = pd.Timestamp("2026-04-01 12:00", tz="UTC")
    rows = [
        # (device, ts, meter_type, consumption_kw, production_kw)
        ("dev-A", base, "M1", 1.0, 0.2),  # 0.2 kW exported to grid
        ("dev-A", base, "M2", 0.0, 0.8),  # 0.8 kW PV gross
        ("dev-A", base, "M2_2", 0.0, 0.1),  # +0.1 kW PV (second meter)
        ("dev-A", base + pd.Timedelta(minutes=15), "M1", 2.0, 0.0),
        ("dev-A", base + pd.Timedelta(minutes=15), "M2", 0.0, 0.5),
    ]
    return pd.DataFrame(
        rows, columns=["device_id", "ts", "meter_type", "consumption_kw", "production_kw"]
    )


def test_merge_m1_m2_produces_one_row_per_device_ts():
    df = m.merge_meters(_synthetic_silver_rows())
    assert len(df) == 2
    assert set(df.columns) >= {
        "device_id",
        "ts",
        "consumption_kw",
        "production_kw",
        "pv_production_kw",
        "self_consumed_kw",
        "total_consumption_kw",
    }


def test_self_consumed_kw_matches_gamification_formula():
    df = m.merge_meters(_synthetic_silver_rows()).set_index("ts")
    base = pd.Timestamp("2026-04-01 12:00", tz="UTC")
    # pv = 0.8 + 0.1 = 0.9; grid_export (M1.production_kw) = 0.2; self = 0.9 - 0.2 = 0.7
    assert df.loc[base, "self_consumed_kw"] == pytest.approx(0.7)
    # total_cons = M1.consumption_kw + self = 1.0 + 0.7 = 1.7
    assert df.loc[base, "total_consumption_kw"] == pytest.approx(1.7)


def test_self_consumed_never_negative():
    rows = pd.DataFrame(
        [
            ("dev-A", pd.Timestamp("2026-04-01 12:00", tz="UTC"), "M1", 0.5, 1.5),
            ("dev-A", pd.Timestamp("2026-04-01 12:00", tz="UTC"), "M2", 0.0, 0.3),
        ],
        columns=["device_id", "ts", "meter_type", "consumption_kw", "production_kw"],
    )
    df = m.merge_meters(rows)
    assert (df["self_consumed_kw"] >= 0).all()


def test_missing_m2_defaults_pv_to_zero():
    rows = pd.DataFrame(
        [("dev-B", pd.Timestamp("2026-04-01 12:00", tz="UTC"), "M1", 0.8, 0.0)],
        columns=["device_id", "ts", "meter_type", "consumption_kw", "production_kw"],
    )
    df = m.merge_meters(rows)
    assert df.loc[0, "pv_production_kw"] == 0.0
    assert df.loc[0, "self_consumed_kw"] == 0.0
    assert df.loc[0, "total_consumption_kw"] == 0.8


def test_add_time_features():
    rows = pd.DataFrame(
        [("dev-A", pd.Timestamp("2026-04-01 12:30", tz="UTC"), "M1", 0.5, 0.0)],
        columns=["device_id", "ts", "meter_type", "consumption_kw", "production_kw"],
    )
    df = m.merge_meters(rows)
    df = m.add_time_features(df)
    assert df.loc[0, "slot"] == 50  # 12*4 + 30//15 = 50
    assert df.loc[0, "is_weekday"]  # 2026-04-01 is Wednesday
    assert df.loc[0, "date"] == pd.Timestamp("2026-04-01").date()


def test_to_kwh_per_bucket_applies_quarter_hour_factor():
    rows = pd.DataFrame(
        [("dev-A", pd.Timestamp("2026-04-01 12:00", tz="UTC"), "M1", 4.0, 0.0)],
        columns=["device_id", "ts", "meter_type", "consumption_kw", "production_kw"],
    )
    df = m.merge_meters(rows)
    df = m.to_kwh_per_bucket(df, ["total_consumption_kw"])
    # 4.0 kW * 0.25 h = 1.0 kWh
    assert df.loc[0, "total_consumption_kwh"] == pytest.approx(1.0)
