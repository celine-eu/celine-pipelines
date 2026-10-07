"""Analysis-ready 15-min meter DataFrame from ``rec_meters_15m``.

``rec_meters_15m`` is the dbt view over ``ds_dev_gold.meters_data_15m`` (the
sanctioned rec_metering interface) that reconstructs ``pv_production_kwh``,
clips ``self_consumed_kwh`` at zero and scopes to the fleet: the registry's
membership (``rec_device_membership``), matched on ``(device_id, community_id)``. All energy
columns are kWh per 15-min bucket and are read as-is — no kW/kWh conversion
happens anywhere in this app.
"""

from __future__ import annotations

import os

import pandas as pd
from sqlalchemy import Engine, text

_SILVER_SCHEMA = os.environ.get("CELINE_SILVER_SCHEMA", "ds_dev_silver")
_GOLD_SCHEMA = os.environ.get("CELINE_GOLD_SCHEMA", "ds_dev_gold")
METERS_VIEW = "rec_meters_15m"
MEMBERSHIP_VIEW = "rec_device_membership"


def load_fleet(engine: Engine) -> dict[str, str]:
    """The fleet, as ``{device_id: community_id}``.

    Every device of the registry's membership (every role). A device's community is
    the one its latest reading carries; a member device with no reading yet takes the
    community the membership lists it under (the first, by name, if it is listed under
    several). The Python tasks write this ``community_id`` on every row.
    """
    sql = text(
        f"""
        select distinct on (device_id) device_id, community_id
        from (
            select device_id, community_id, ts
            from {_SILVER_SCHEMA}.{METERS_VIEW}
            union all
            select device_id, community_id, null::timestamptz as ts
            from {_GOLD_SCHEMA}.{MEMBERSHIP_VIEW}
        ) rows
        order by device_id, ts desc nulls last, community_id
        """
    )
    with engine.connect() as conn:
        return {device_id: community_id for device_id, community_id in conn.execute(sql)}


def load_meters(
    engine: Engine,
    lookback_days: int,
    devices: list[str] | None = None,
) -> pd.DataFrame:
    """Read the last ``lookback_days`` of 15-min meter rows (kWh per bucket).

    Args:
        engine: SQLAlchemy engine.
        lookback_days: How many days back to read.
        devices: Optional extra scope. The view is already restricted to the fleet
            (the registry's membership); pass a subset to narrow further. ``None``
            reads the whole fleet.

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
