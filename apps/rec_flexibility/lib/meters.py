"""Analysis-ready 15-min meter DataFrame from ``rec_meters_15m``.

``rec_meters_15m`` is the dbt view over ``ds_dev_gold.meters_data_15m`` (the
sanctioned rec_metering interface) that reconstructs ``pv_production_kwh``,
clips ``self_consumed_kwh`` at zero and scopes to the active fleet. All energy
columns are kWh per 15-min bucket and are read as-is — no kW/kWh conversion
happens anywhere in this app.
"""

from __future__ import annotations

import os

import pandas as pd
from sqlalchemy import Engine, text

_SILVER_SCHEMA = os.environ.get("CELINE_SILVER_SCHEMA", "ds_dev_silver")
METERS_VIEW = "rec_meters_15m"


def load_meters(
    engine: Engine,
    lookback_days: int,
    devices: list[str] | None = None,
) -> pd.DataFrame:
    """Read the last ``lookback_days`` of 15-min meter rows (kWh per bucket).

    Args:
        engine: SQLAlchemy engine.
        lookback_days: How many days back to read.
        devices: Optional extra fleet scope. The view is already restricted to the
            active fleet; pass a subset to narrow further. ``None`` reads all.

    Returns:
        One row per ``(device_id, ts)`` with columns ``consumption_kwh``
        (grid import), ``production_kwh`` (grid export), ``pv_production_kwh``,
        ``self_consumed_kwh``, ``total_consumption_kwh``.
    """
    where_device = ""
    params: dict[str, object] = {"lookback": lookback_days}
    if devices:
        where_device = "and device_id = any(:devices)"
        params["devices"] = list(devices)
    sql = text(
        f"""
        select device_id, ts, consumption_kwh, production_kwh,
               pv_production_kwh, self_consumed_kwh, total_consumption_kwh
        from {_SILVER_SCHEMA}.{METERS_VIEW}
        where ts >= now() - make_interval(days => :lookback)
        {where_device}
        """
    )
    with engine.connect() as conn:
        df = pd.read_sql(sql, conn, params=params)
    df["ts"] = pd.to_datetime(df["ts"], utc=True)
    return df


def add_time_features(df: pd.DataFrame) -> pd.DataFrame:
    """Add ``slot`` (0..95), ``is_weekday`` (bool), ``date``, ``hour`` derived from ``ts``."""
    df = df.copy()
    df["slot"] = df["ts"].dt.hour * 4 + df["ts"].dt.minute // 15
    df["is_weekday"] = df["ts"].dt.dayofweek < 5
    df["date"] = df["ts"].dt.date
    df["hour"] = df["ts"].dt.hour
    return df
