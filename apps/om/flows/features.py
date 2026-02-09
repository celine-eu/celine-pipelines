"""Gold-layer feature engineering for Open-Meteo weather pipeline.

Computes 29 ML features from the 4 natural weather variables
(shortwave_radiation, cloud_cover, temperature_2m, precipitation)
plus datetime.

Features are designed for energy consumption forecasting and are
identical for both historical and forecast data, ensuring
train/test consistency.

All datetime operations assume Italian local time (Europe/Rome),
which matches the Open-Meteo API timezone parameter.
"""

import logging
from datetime import date, timedelta
from typing import Optional

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


# =============================================================================
# Constants
# =============================================================================

DATETIME_COL: str = "datetime"

REQUIRED_WEATHER_COLS: list[str] = [
    "temperature_2m",
    "shortwave_radiation",
    "cloud_cover",
    "precipitation",
]

SELECTED_FEATURES: list[str] = [
    # Temporal / Fourier (11)
    "hour_sin",
    "hour_cos",
    "dow_sin",
    "dow_cos",
    "annual_sin",
    "annual_cos",
    "semi_annual_sin",
    "semi_annual_cos",
    "is_weekend",
    "is_holiday",
    "is_daylight",
    # Temperature-derived (11)
    "temperature_2m",
    "heating_degree_hour",
    "temp_rolling_mean_24h",
    "temp_rolling_std_24h",
    "temp_change_rate_3h",
    "thermal_inertia_12h",
    "temp_gradient_24h",
    "heating_degree_rolling_mean_24h",
    "cumulative_hdd_48h",
    "temp_x_hour_sin",
    "heating_x_night",
    # Radiation-derived (3)
    "shortwave_radiation",
    "radiation_rolling_mean_24h",
    "radiation_x_daytime",
    # Cloud-derived (2)
    "cloud_cover",
    "cloud_cover_rolling_mean_24h",
    # Precipitation (1)
    "precipitation",
    # Interaction (1)
    "weekend_x_hour_cos",
]

FOURIER_PERIODS: dict[str, int] = {
    "annual": 8766,      # hours in a year (365.25 * 24)
    "semi_annual": 4383,  # hours in half a year
}

# Monthly climate defaults for Folgaria (Trentino, ~1200m altitude)
MONTHLY_CLIMATE_DEFAULTS: dict[str, dict[int, float]] = {
    "temperature_2m": {
        1: -2.0, 2: -1.0, 3: 3.0, 4: 7.0, 5: 11.0, 6: 15.0,
        7: 17.0, 8: 17.0, 9: 13.0, 10: 8.0, 11: 3.0, 12: -1.0,
    },
    "shortwave_radiation": {
        1: 150, 2: 200, 3: 300, 4: 400, 5: 500, 6: 550,
        7: 550, 8: 500, 9: 400, 10: 280, 11: 180, 12: 130,
    },
    "cloud_cover": {
        1: 55, 2: 50, 3: 50, 4: 55, 5: 55, 6: 50,
        7: 40, 8: 45, 9: 45, 10: 50, 11: 60, 12: 60,
    },
    "precipitation": {
        1: 0.0, 2: 0.0, 3: 0.0, 4: 0.1, 5: 0.1, 6: 0.1,
        7: 0.1, 8: 0.1, 9: 0.1, 10: 0.0, 11: 0.0, 12: 0.0,
    },
}


# =============================================================================
# Italian holidays
# =============================================================================

def _easter_date(year: int) -> date:
    """Compute Easter Sunday using the Anonymous Gregorian algorithm."""
    a = year % 19
    b = year // 100
    c = year % 100
    d = b // 4
    e = b % 4
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i = c // 4
    k = c % 4
    l_val = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l_val) // 451
    month = (h + l_val - 7 * m + 114) // 31
    day = ((h + l_val - 7 * m + 114) % 31) + 1
    return date(year, month, day)


def _generate_italian_holidays(
    start_year: int = 2020,
    end_year: int = 2035,
) -> list[str]:
    """Generate Italian public holiday dates for a range of years.

    Args:
        start_year: First year to generate holidays for.
        end_year: Last year (inclusive) to generate holidays for.

    Returns:
        Sorted list of date strings in 'YYYY-MM-DD' format.
    """
    fixed_dates = [
        (1, 1),    # Capodanno
        (1, 6),    # Epifania
        (4, 25),   # Liberazione
        (5, 1),    # Festa del Lavoro
        (6, 2),    # Festa della Repubblica
        (8, 15),   # Ferragosto
        (11, 1),   # Ognissanti
        (12, 8),   # Immacolata Concezione
        (12, 25),  # Natale
        (12, 26),  # Santo Stefano
    ]
    holidays: list[str] = []
    for year in range(start_year, end_year + 1):
        for month, day in fixed_dates:
            holidays.append(date(year, month, day).isoformat())
        easter_monday = _easter_date(year) + timedelta(days=1)
        holidays.append(easter_monday.isoformat())
    return sorted(holidays)


ITALIAN_HOLIDAYS: list[str] = _generate_italian_holidays()


# =============================================================================
# Imputation
# =============================================================================

def _find_gap_lengths(series: pd.Series) -> list[tuple[int, int, int]]:
    """Find all NaN gaps in a series.

    Args:
        series: Pandas Series to analyze.

    Returns:
        List of (start_idx, end_idx, length) tuples for each gap.
    """
    is_nan = series.isna()
    if not is_nan.any():
        return []

    gaps: list[tuple[int, int, int]] = []
    in_gap = False
    gap_start = 0

    for idx, is_missing in enumerate(is_nan):
        if is_missing and not in_gap:
            in_gap = True
            gap_start = idx
        elif not is_missing and in_gap:
            in_gap = False
            gaps.append((gap_start, idx - 1, idx - gap_start))

    if in_gap:
        gaps.append((gap_start, len(series) - 1, len(series) - gap_start))

    return gaps


def impute_weather_column(
    df: pd.DataFrame,
    column: str,
    small_gap_threshold: int = 6,
) -> pd.DataFrame:
    """Impute missing values for a single weather column.

    Strategy (applied in order):
    1. Small gaps (<=threshold hours): linear interpolation
    2. Larger gaps: copy from same hour 24h/48h/72h back
    3. Last resort: monthly climate default

    Args:
        df: DataFrame with the column to impute.
        column: Column name to impute.
        small_gap_threshold: Max consecutive NaN hours to interpolate.

    Returns:
        DataFrame with imputed values.
    """
    df = df.copy()

    if column not in df.columns or not df[column].isna().any():
        return df

    n_missing_before = df[column].isna().sum()
    dt_series = pd.to_datetime(df[DATETIME_COL])
    gaps = _find_gap_lengths(df[column])

    if not gaps:
        return df

    logger.info(
        "Imputing %s: %d missing values in %d gap(s)",
        column, n_missing_before, len(gaps),
    )

    for gap_start, gap_end, gap_length in gaps:
        if gap_length <= small_gap_threshold:
            continue  # will be handled by interpolation below

        for idx in range(gap_start, gap_end + 1):
            if not pd.isna(df.iloc[idx][column]):
                continue

            filled = False
            for days_back in [1, 2, 3]:
                source_idx = idx - (24 * days_back)
                if source_idx >= 0 and not pd.isna(df.iloc[source_idx][column]):
                    df.iloc[idx, df.columns.get_loc(column)] = (
                        df.iloc[source_idx][column]
                    )
                    filled = True
                    break

            if not filled and column in MONTHLY_CLIMATE_DEFAULTS:
                month = dt_series.iloc[idx].month
                df.iloc[idx, df.columns.get_loc(column)] = (
                    MONTHLY_CLIMATE_DEFAULTS[column][month]
                )

    # Interpolate small gaps
    if df[column].isna().any():
        df[column] = df[column].interpolate(
            method="linear", limit=small_gap_threshold,
        )

    # Final safety: forward/backward fill
    if df[column].isna().any():
        remaining = df[column].isna().sum()
        df[column] = df[column].ffill().bfill()
        logger.warning("Forward/backward filled %d remaining NaN in %s", remaining, column)

    n_filled = n_missing_before - df[column].isna().sum()
    logger.info("Imputed %d values in %s", n_filled, column)
    return df


def impute_missing_weather(
    df: pd.DataFrame,
    small_gap_threshold: int = 6,
) -> pd.DataFrame:
    """Impute missing values for all required weather columns.

    Only handles the 4 original weather columns. Derived features
    are computed from these and don't need separate imputation.

    Args:
        df: DataFrame with weather columns.
        small_gap_threshold: Max consecutive NaN hours to interpolate.

    Returns:
        DataFrame with imputed weather values.
    """
    df = df.copy()

    columns_with_nan = [
        col for col in REQUIRED_WEATHER_COLS
        if col in df.columns and df[col].isna().any()
    ]

    if not columns_with_nan:
        logger.info("No missing values in weather columns")
        return df

    logger.info("Imputing %d columns with missing values", len(columns_with_nan))

    for col in columns_with_nan:
        df = impute_weather_column(df, col, small_gap_threshold=small_gap_threshold)

    return df


# =============================================================================
# Temporal features
# =============================================================================

def add_temporal_features(df: pd.DataFrame) -> pd.DataFrame:
    """Add basic temporal features from datetime column.

    All features use Italian local time directly (Europe/Rome).

    Args:
        df: DataFrame with datetime column.

    Returns:
        DataFrame with hour, day_of_week, is_weekend, is_holiday, is_daylight.
    """
    df = df.copy()
    dt_series = pd.to_datetime(df[DATETIME_COL])

    df["hour"] = dt_series.dt.hour
    df["day_of_week"] = dt_series.dt.dayofweek

    df["is_weekend"] = df["day_of_week"].isin([5, 6]).astype(int)

    holiday_dates = pd.to_datetime(ITALIAN_HOLIDAYS)
    df["is_holiday"] = dt_series.dt.normalize().isin(holiday_dates).astype(int)

    df["is_daylight"] = df["hour"].between(6, 20).astype(int)

    return df


# =============================================================================
# Fourier features
# =============================================================================

def add_fourier_features(df: pd.DataFrame) -> pd.DataFrame:
    """Add Fourier encoding for cyclical temporal patterns.

    Creates: hour_sin, hour_cos, dow_sin, dow_cos,
             annual_sin, annual_cos, semi_annual_sin, semi_annual_cos.

    Args:
        df: DataFrame with datetime and temporal columns (hour, day_of_week).

    Returns:
        DataFrame with added Fourier features.
    """
    df = df.copy()
    dt_series = pd.to_datetime(df[DATETIME_COL])

    hours_since_epoch = (
        (dt_series - pd.Timestamp("2020-01-01")).dt.total_seconds() / 3600
    )

    for name in ["annual", "semi_annual"]:
        period = FOURIER_PERIODS[name]
        angle = 2 * np.pi * hours_since_epoch / period
        df[f"{name}_sin"] = np.sin(angle)
        df[f"{name}_cos"] = np.cos(angle)

    df["hour_sin"] = np.sin(2 * np.pi * df["hour"] / 24)
    df["hour_cos"] = np.cos(2 * np.pi * df["hour"] / 24)

    df["dow_sin"] = np.sin(2 * np.pi * df["day_of_week"] / 7)
    df["dow_cos"] = np.cos(2 * np.pi * df["day_of_week"] / 7)

    return df


# =============================================================================
# Weather-derived features
# =============================================================================

def add_weather_features(df: pd.DataFrame) -> pd.DataFrame:
    """Add weather-derived features used by the model.

    Creates: heating_degree_hour, temp_rolling_mean_24h, temp_rolling_std_24h,
             radiation_rolling_mean_24h, cloud_cover_rolling_mean_24h,
             heating_degree_rolling_mean_24h.

    Args:
        df: DataFrame with the 4 required weather columns.

    Returns:
        DataFrame with added weather features.
    """
    df = df.copy()

    missing = [col for col in REQUIRED_WEATHER_COLS if col not in df.columns]
    if missing:
        raise ValueError(f"Missing required weather columns: {missing}")

    df["heating_degree_hour"] = np.maximum(0, 18 - df["temperature_2m"])

    df["temp_rolling_mean_24h"] = (
        df["temperature_2m"].rolling(window=24, min_periods=1).mean()
    )
    df["temp_rolling_std_24h"] = (
        df["temperature_2m"].rolling(window=24, min_periods=1).std()
    )

    df["radiation_rolling_mean_24h"] = (
        df["shortwave_radiation"].rolling(window=24, min_periods=1).mean()
    )

    df["cloud_cover_rolling_mean_24h"] = (
        df["cloud_cover"].rolling(window=24, min_periods=1).mean()
    )

    df["heating_degree_rolling_mean_24h"] = (
        df["heating_degree_hour"].rolling(window=24, min_periods=1).mean()
    )

    return df


# =============================================================================
# Thermal dynamics features
# =============================================================================

def add_thermal_dynamics(df: pd.DataFrame) -> pd.DataFrame:
    """Add thermal dynamics features.

    Creates: temp_change_rate_3h, temp_gradient_24h,
             thermal_inertia_12h, cumulative_hdd_48h.

    Args:
        df: DataFrame with temperature_2m and heating_degree_hour columns.

    Returns:
        DataFrame with added thermal dynamics features.
    """
    df = df.copy()

    df["temp_change_rate_3h"] = (
        (df["temperature_2m"] - df["temperature_2m"].shift(3)) / 3
    )

    df["temp_gradient_24h"] = (
        df["temperature_2m"] - df["temperature_2m"].shift(24)
    )

    df["thermal_inertia_12h"] = (
        df["temperature_2m"].ewm(halflife=12, min_periods=1).mean()
    )

    df["cumulative_hdd_48h"] = (
        df["heating_degree_hour"].rolling(window=48, min_periods=1).sum()
    )

    return df


# =============================================================================
# Interaction features
# =============================================================================

def add_interactions(df: pd.DataFrame) -> pd.DataFrame:
    """Add interaction features used by the model.

    Creates: temp_x_hour_sin, radiation_x_daytime,
             weekend_x_hour_cos, heating_x_night.

    Args:
        df: DataFrame with base features already computed.

    Returns:
        DataFrame with added interaction features.
    """
    df = df.copy()

    df["temp_x_hour_sin"] = df["temperature_2m"] * df["hour_sin"]

    df["radiation_x_daytime"] = df["shortwave_radiation"] * df["is_daylight"]

    df["weekend_x_hour_cos"] = df["is_weekend"] * df["hour_cos"]

    is_night = ((df["hour"] >= 20) | (df["hour"] <= 6)).astype(int)
    df["heating_x_night"] = df["heating_degree_hour"] * is_night

    return df


# =============================================================================
# Edge-case NaN handling
# =============================================================================

def _fill_edge_nans(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    """Fill NaN values caused by rolling/shift warmup at series edges.

    Args:
        df: DataFrame with potential edge-case NaN.
        columns: Feature columns to check and fill.

    Returns:
        DataFrame with edge NaN filled via bfill/ffill.
    """
    df = df.copy()

    for col in columns:
        if col not in df.columns:
            continue
        if not df[col].isna().any():
            continue

        n_nan = df[col].isna().sum()
        if n_nan <= 48:
            df[col] = df[col].bfill().ffill()
            if df[col].isna().any():
                median_val = df[col].median()
                fill_val = median_val if not pd.isna(median_val) else 0.0
                df[col] = df[col].fillna(fill_val)
            logger.debug("Filled %d edge-case NaN in '%s'", n_nan, col)
        else:
            logger.warning(
                "Feature '%s' has %d NaN values (more than warmup period)", col, n_nan,
            )

    return df


# =============================================================================
# Main entry point
# =============================================================================

def build_gold_features(
    df: pd.DataFrame,
    impute_missing: bool = True,
) -> pd.DataFrame:
    """Build the 29 gold ML features from silver weather data.

    Pipeline order:
    1. Impute missing weather values (optional)
    2. Add temporal features (hour, day_of_week, is_weekend, is_holiday, is_daylight)
    3. Add Fourier features (cyclical encodings)
    4. Add weather-derived features (rolling stats, heating degree)
    5. Add thermal dynamics (change rate, gradient, inertia, cumulative HDD)
    6. Add interaction features (cross-feature products)
    7. Fill edge-case NaN from rolling/shift warmup
    8. Select final 29 features + datetime

    Args:
        df: DataFrame with columns: datetime, temperature_2m,
            shortwave_radiation, cloud_cover, precipitation.
        impute_missing: Whether to impute missing weather values first.

    Returns:
        DataFrame with datetime + 29 feature columns.

    Raises:
        ValueError: If required weather columns are missing.
    """
    logger.info("Building gold features (%d input rows)...", len(df))

    required_cols = [DATETIME_COL] + REQUIRED_WEATHER_COLS
    missing = [col for col in required_cols if col not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    df = df.copy()
    df = df.sort_values(DATETIME_COL).reset_index(drop=True)

    if impute_missing:
        df = impute_missing_weather(df)

    df = add_temporal_features(df)
    df = add_fourier_features(df)
    df = add_weather_features(df)
    df = add_thermal_dynamics(df)
    df = add_interactions(df)

    available = [col for col in SELECTED_FEATURES if col in df.columns]
    missing_features = set(SELECTED_FEATURES) - set(available)
    if missing_features:
        logger.warning("Missing features from SELECTED_FEATURES: %s", missing_features)

    df = _fill_edge_nans(df, available)

    output_cols = [DATETIME_COL] + available
    result = df[output_cols]

    logger.info("Built %d gold features (%d rows)", len(available), len(result))
    return result
