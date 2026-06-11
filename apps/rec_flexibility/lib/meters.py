"""Silver -> analysis-ready 15-min meter DataFrame.

All kW columns remain in kW; downstream callers that
need kWh per 15-min bucket multiply by 0.25.
"""

from __future__ import annotations

import os

import pandas as pd
from sqlalchemy import Engine, text

_SILVER_SCHEMA = os.environ.get("CELINE_SILVER_SCHEMA", "ds_dev_silver")


def load_silver(
    engine: Engine,
    lookback_days: int,
    devices: list[str] | None = None,
) -> pd.DataFrame:
    """Read the last ``lookback_days`` of raw silver rows for all meter types.

    Args:
        engine: SQLAlchemy engine.
        lookback_days: How many days back to read.
        devices: Optional fleet scope. When provided, rows are restricted to these
            device_ids (v2: the ``fleet.active_devices`` set). ``None`` reads all.

    Returns columns: ``device_id, ts, meter_type, consumption_kw, production_kw``.
    """
    where_device = ""
    params: dict[str, object] = {"lookback": lookback_days}
    if devices:
        where_device = "and device_id = any(:devices)"
        params["devices"] = list(devices)
    sql = text(
        f"""
        select device_id, ts, meter_type, consumption_kw, production_kw
        from {_SILVER_SCHEMA}.meters_data
        where ts >= now() - make_interval(days => :lookback)
        {where_device}
        """
    )
    with engine.connect() as conn:
        df = pd.read_sql(sql, conn, params=params)
    df["ts"] = pd.to_datetime(df["ts"], utc=True)
    return df


def merge_meters(silver_df: pd.DataFrame) -> pd.DataFrame:
    """Pivot M1 + combined M2/M2_2 per ``(device, ts)``; derive ``self_consumed_kw``.

    Mirrors gamification/data_loader.py::load_meters exactly::

        pv_production_kw = M2.production_kw + M2_2.production_kw
        grid_export_kw   = M1.production_kw
        self_consumed_kw = clip(pv_production_kw - grid_export_kw, >= 0)
        total_cons_kw    = M1.consumption_kw + self_consumed_kw

    Args:
        silver_df: raw rows from the silver meters_data table.

    Returns:
        One row per ``(device_id, ts)`` with derived columns ``consumption_kw,
        production_kw (= grid_export_kw), pv_production_kw, self_consumed_kw,
        total_consumption_kw``.
    """
    m1 = silver_df[silver_df["meter_type"] == "M1"].copy()
    m1 = m1.rename(columns={"production_kw": "grid_export_kw"})[
        ["device_id", "ts", "consumption_kw", "grid_export_kw"]
    ]

    m2 = silver_df[silver_df["meter_type"].isin(["M2", "M2_2"])]
    pv = (
        m2.groupby(["device_id", "ts"])["production_kw"]
        .sum()
        .reset_index()
        .rename(columns={"production_kw": "pv_production_kw"})
    )

    out = m1.merge(pv, on=["device_id", "ts"], how="left")
    out["pv_production_kw"] = out["pv_production_kw"].fillna(0.0)
    out["self_consumed_kw"] = (out["pv_production_kw"] - out["grid_export_kw"]).clip(lower=0.0)
    out["production_kw"] = out["grid_export_kw"]  # keep original name for SQL parity
    out["total_consumption_kw"] = out["consumption_kw"] + out["self_consumed_kw"]
    return out[
        [
            "device_id",
            "ts",
            "consumption_kw",
            "production_kw",
            "pv_production_kw",
            "self_consumed_kw",
            "total_consumption_kw",
        ]
    ].reset_index(drop=True)


def add_time_features(df: pd.DataFrame) -> pd.DataFrame:
    """Add ``slot`` (0..95), ``is_weekday`` (bool), ``date``, ``hour`` derived from ``ts``."""
    df = df.copy()
    df["slot"] = df["ts"].dt.hour * 4 + df["ts"].dt.minute // 15
    df["is_weekday"] = df["ts"].dt.dayofweek < 5
    df["date"] = df["ts"].dt.date
    df["hour"] = df["ts"].dt.hour
    return df


def to_kwh_per_bucket(df: pd.DataFrame, kw_cols: list[str]) -> pd.DataFrame:
    """Convert named kW columns to kWh per 15-min bucket (``x 0.25``).

    The returned DataFrame renames each column to the ``_kwh`` suffix.
    """
    df = df.copy()
    for col in kw_cols:
        kwh_col = col.replace("_kw", "_kwh")
        df[kwh_col] = df[col] * 0.25
    return df
