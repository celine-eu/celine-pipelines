"""Tests for the pure functions in pipeline_soil.py.

No network calls: _fetch_soil_data (which does POST) is intentionally left
untested here; these tests cover the pure transformation functions that
sit between the parsed API response and the raw table.
"""

import pandas as pd
import pytest

from pipeline_soil import _soil_1m, _soil_rows_from_response


class TestSoil1m:
    """Depth-weighted composite soil temperature at 1 m (100 cm) depth."""

    def test_weighted_composite(self):
        assert _soil_1m(20.0, 10.0) == pytest.approx(0.683 * 20.0 + 0.317 * 10.0)

    def test_equal_layers_returns_the_same_value(self):
        # If weights sum to 1, equal inputs must return the same value.
        assert _soil_1m(15.0, 15.0) == pytest.approx(15.0)

    def test_upper_layer_weighted_more_heavily(self):
        # Weight 0.683 on the upper (28-100cm) layer vs 0.317 on the lower
        # (100-255cm) layer: the composite must sit closer to the upper
        # layer's value than a plain 50/50 average would.
        plain_average = (10.0 + 20.0) / 2
        composite = _soil_1m(10.0, 20.0)
        assert composite < plain_average

    def test_works_on_pandas_series(self):
        upper = pd.Series([10.0, 12.0])
        lower = pd.Series([8.0, 9.0])
        result = _soil_1m(upper, lower)
        assert result.iloc[0] == pytest.approx(0.683 * 10.0 + 0.317 * 8.0)
        assert result.iloc[1] == pytest.approx(0.683 * 12.0 + 0.317 * 9.0)


def _sample_response():
    """Two locations, three days each, matching the archive API shape."""
    return [
        {
            "latitude": 45.45,  # snapped to nearest 0.1deg ERA5-Land cell
            "longitude": 10.45,
            "elevation": 500.0,
            "daily": {
                "time": ["2026-09-01", "2026-09-02", "2026-09-03"],
                "soil_temperature_28_to_100cm_mean": [18.1, None, 18.3],
                "soil_temperature_100_to_255cm_mean": [16.0, None, 16.2],
            },
        },
        {
            "latitude": 45.49,
            "longitude": 10.49,
            "elevation": 800.0,
            "daily": {
                "time": ["2026-09-01", "2026-09-02", "2026-09-03"],
                "soil_temperature_28_to_100cm_mean": [14.0, 14.1, 14.2],
                "soil_temperature_100_to_255cm_mean": [12.0, 12.1, 12.2],
            },
        },
    ]


class TestSoilRowsFromResponse:
    def test_stores_requested_coordinates_as_lat_lon(self):
        batch = [(45.44, 10.44), (45.48, 10.48)]
        result = _soil_rows_from_response(batch, _sample_response())

        loc1 = result[result["lat"] == 45.44]
        assert (loc1["lon"] == 10.44).all()

        loc2 = result[result["lat"] == 45.48]
        assert (loc2["lon"] == 10.48).all()

    def test_stores_api_returned_coordinates_for_provenance(self):
        batch = [(45.44, 10.44), (45.48, 10.48)]
        result = _soil_rows_from_response(batch, _sample_response())

        loc1 = result[result["lat"] == 45.44]
        assert (loc1["api_lat"] == 45.45).all()
        assert (loc1["api_lon"] == 10.45).all()

    def test_drops_rows_where_both_soil_values_are_null(self):
        batch = [(45.44, 10.44), (45.48, 10.48)]
        result = _soil_rows_from_response(batch, _sample_response())

        loc1 = result[result["lat"] == 45.44]
        assert len(loc1) == 2
        assert set(loc1["date"].astype(str)) == {"2026-09-01", "2026-09-03"}

        loc2 = result[result["lat"] == 45.48]
        assert len(loc2) == 3

    def test_keeps_row_when_only_one_soil_value_is_null(self):
        data = _sample_response()
        data[0]["daily"]["soil_temperature_28_to_100cm_mean"][1] = 17.5

        batch = [(45.44, 10.44), (45.48, 10.48)]
        result = _soil_rows_from_response(batch, data)

        loc1 = result[result["lat"] == 45.44]
        assert len(loc1) == 3
        middle_row = loc1[loc1["date"].astype(str) == "2026-09-02"].iloc[0]
        assert middle_row["soil_temperature_28_to_100cm_mean"] == 17.5
        assert pd.isna(middle_row["soil_temperature_100_to_255cm_mean"])

    def test_raises_on_batch_response_length_mismatch(self):
        batch = [(45.44, 10.44), (45.48, 10.48), (45.52, 10.52)]

        with pytest.raises(ValueError):
            _soil_rows_from_response(batch, _sample_response())

    def test_output_columns(self):
        batch = [(45.44, 10.44), (45.48, 10.48)]
        result = _soil_rows_from_response(batch, _sample_response())

        assert set(result.columns) == {
            "date",
            "lat",
            "lon",
            "soil_temperature_28_to_100cm_mean",
            "soil_temperature_100_to_255cm_mean",
            "api_lat",
            "api_lon",
        }
