"""Shared pytest fixtures for rec_flexibility tests."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import pytest


@pytest.fixture
def config_path() -> Path:
    """Path to the production flexibility_config.yaml."""
    return Path(__file__).parents[1] / "flexibility_config.yaml"


@pytest.fixture
def sample_meter_15m() -> pd.DataFrame:
    """Synthetic 15-min meter data: 3 devices x 7 days x 96 slots.

    ``consumption_kw`` is average kW over the bucket (NOT kWh). Tests should
    multiply by 0.25 when they need a 15-min kWh figure.
    """
    rng = pd.date_range("2026-04-01 00:00", "2026-04-07 23:45", freq="15min", tz="UTC")
    rows = []
    rng_state = np.random.RandomState(42)
    for device_id in ["dev-A", "dev-B", "dev-C"]:
        for ts in rng:
            slot = ts.hour * 4 + ts.minute // 15
            rows.append(
                {
                    "device_id": device_id,
                    "ts": ts,
                    "consumption_kw": 0.5 + 0.1 * (slot % 5) + rng_state.uniform(-0.05, 0.05),
                }
            )
    return pd.DataFrame(rows)
