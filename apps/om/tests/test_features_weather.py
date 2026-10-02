"""Tests for the om-flow gold feature builders (flows/features.py).

Covers two properties the incremental recompute relies on:
- trailing rows beyond the provider's horizon (all inputs null) are dropped
  rather than invented by forward-fill;
- rows inside the recompute window get the same rolling/shift features as
  they would with the full silver history, given the pipeline's buffer.
"""

import numpy as np
import pandas as pd
import pipeline
import pytest
from features import (
    METERS_WEATHER_COLS,
    REQUIRED_WEATHER_COLS,
    _drop_trailing_all_null_rows,
    build_gold_features,
    build_gold_features_meters,
)

ALL_INPUT_COLS = sorted(set(REQUIRED_WEATHER_COLS) | set(METERS_WEATHER_COLS))


def _silver(hours: int, start: str = "2026-01-10 00:00", seed: int = 0) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    idx = pd.date_range(start, periods=hours, freq="h")
    t = np.arange(hours)
    temp = 2 + 6 * np.sin(2 * np.pi * (t - 9) / 24) + rng.normal(0, 1.5, hours)
    temp += np.linspace(-4, 4, hours)  # slow drift so long windows matter
    sw = np.clip(500 * np.sin(np.pi * ((t % 24) - 6) / 12), 0, None)
    return pd.DataFrame(
        {
            "datetime": idx,
            "temperature_2m": temp,
            "shortwave_radiation": sw,
            "direct_radiation": 0.6 * sw,
            "diffuse_radiation": 0.4 * sw,
            "global_tilted_irradiance": 1.1 * sw,
            "cloud_cover": rng.uniform(0, 100, hours),
            "precipitation": rng.exponential(0.2, hours),
        }
    )


# ---------------------------------------------------------------------------
# Trailing all-null rows
# ---------------------------------------------------------------------------


class TestDropTrailingAllNullRows:
    def test_drops_only_trailing_rows_where_all_columns_are_null(self):
        df = _silver(10)
        cols = ["temperature_2m", "cloud_cover"]
        df.loc[7:, cols] = np.nan
        out = _drop_trailing_all_null_rows(df, cols)
        assert list(out["datetime"]) == list(df["datetime"].iloc[:7])

    def test_keeps_interior_all_null_rows(self):
        df = _silver(10)
        cols = ["temperature_2m", "cloud_cover"]
        df.loc[3:4, cols] = np.nan
        out = _drop_trailing_all_null_rows(df, cols)
        assert len(out) == 10

    def test_keeps_partially_null_trailing_rows(self):
        df = _silver(10)
        df.loc[7:, "temperature_2m"] = np.nan
        out = _drop_trailing_all_null_rows(df, ["temperature_2m", "cloud_cover"])
        assert len(out) == 10

    def test_ignores_columns_outside_the_given_set(self):
        df = _silver(10)
        cols = ["temperature_2m", "cloud_cover"]
        df.loc[7:, cols] = np.nan
        # precipitation still present on the tail: irrelevant to this builder
        out = _drop_trailing_all_null_rows(df, cols)
        assert len(out) == 7

    def test_all_null_frame_becomes_empty(self):
        df = _silver(5)
        df[ALL_INPUT_COLS] = np.nan
        out = _drop_trailing_all_null_rows(df, ALL_INPUT_COLS)
        assert out.empty

    def test_uses_datetime_order_not_row_order(self):
        df = _silver(10)
        cols = ["temperature_2m", "cloud_cover"]
        df.loc[8:, cols] = np.nan
        shuffled = df.iloc[::-1].reset_index(drop=True)
        out = _drop_trailing_all_null_rows(shuffled, cols)
        assert out["datetime"].max() == df["datetime"].iloc[7]
        assert len(out) == 8


@pytest.mark.parametrize(
    "builder, cols",
    [
        (build_gold_features, REQUIRED_WEATHER_COLS),
        (build_gold_features_meters, METERS_WEATHER_COLS),
    ],
)
class TestBuildersDoNotInventTail:
    def test_trailing_all_null_rows_are_dropped(self, builder, cols):
        df = _silver(72)
        df.loc[70:, ALL_INPUT_COLS] = np.nan
        out = builder(df, impute_missing=True)
        assert out["datetime"].max() == df["datetime"].iloc[69]
        assert len(out) == 70

    def test_tail_null_only_in_builder_cols_is_dropped(self, builder, cols):
        df = _silver(72)
        df.loc[70:, cols] = np.nan
        out = builder(df, impute_missing=True)
        assert len(out) == 70

    def test_interior_gap_is_still_imputed(self, builder, cols):
        df = _silver(72)
        df.loc[30:32, list(cols)] = np.nan
        out = builder(df, impute_missing=True)
        assert len(out) == 72
        assert not out.isna().any().any()

    def test_partially_null_tail_is_still_imputed(self, builder, cols):
        df = _silver(72)
        df.loc[70:, "temperature_2m"] = np.nan
        out = builder(df, impute_missing=True)
        assert len(out) == 72
        assert not out["temperature_2m"].isna().any()


# ---------------------------------------------------------------------------
# Rolling buffer: recompute-window rows match full-history values
# ---------------------------------------------------------------------------

EXACT_FEATURES = [
    "temp_rolling_mean_24h",
    "temp_rolling_std_24h",
    "radiation_rolling_mean_24h",
    "cloud_cover_rolling_mean_24h",
    "heating_degree_rolling_mean_24h",
    "cumulative_hdd_48h",
    "temp_change_rate_3h",
    "temp_gradient_24h",
]
EXACT_METERS_FEATURES = ["cloud_cover_diff", "ghi_ramp"]


def _window_split():
    full = _silver(24 * 14)
    recompute_from = full["datetime"].iloc[24 * 8]
    cutoff = recompute_from - pd.Timedelta(hours=pipeline._ROLLING_BUFFER_HOURS)
    partial = full[full["datetime"] >= cutoff].reset_index(drop=True)
    return full, partial, recompute_from


def _in_window(df, recompute_from):
    return df[df["datetime"] >= recompute_from].set_index("datetime")


def test_recompute_window_matches_full_history_features():
    full, partial, recompute_from = _window_split()
    ref = _in_window(build_gold_features(full), recompute_from)
    got = _in_window(build_gold_features(partial), recompute_from)
    assert len(got) == len(ref) > 0
    for col in EXACT_FEATURES:
        np.testing.assert_allclose(got[col], ref[col], rtol=1e-9, atol=1e-9, err_msg=col)
    # EWM (halflife 12 h) only converges: 0.5 ** (72 / 12) of weight is lost.
    np.testing.assert_allclose(
        got["thermal_inertia_12h"], ref["thermal_inertia_12h"], atol=0.5
    )


def test_recompute_window_matches_full_history_meters_features():
    full, partial, recompute_from = _window_split()
    ref = _in_window(build_gold_features_meters(full), recompute_from)
    got = _in_window(build_gold_features_meters(partial), recompute_from)
    assert len(got) == len(ref) > 0
    for col in EXACT_METERS_FEATURES:
        np.testing.assert_allclose(got[col], ref[col], rtol=1e-9, atol=1e-9, err_msg=col)
