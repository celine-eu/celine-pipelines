-- Copernicus weather features reshaped to the meter-forecasting contract.
--
-- Combines CAMS solar radiation (hourly) with ERA5 temperature (6-hourly,
-- interpolated to hourly). Output columns match the forecasting pipeline's
-- WeatherDataContract so this table can be used as a weather source directly.
--
-- Missing columns vs the full contract (global_tilted_irradiance,
-- effective_solar_pv, cloud_cover, cloud_cover_diff, pv_temp_factor,
-- solar_elevation) are omitted — the forecasting pipeline handles absent
-- weather columns gracefully (drops with warning, falls back to calendar + lag).

{{ config(
    materialized='incremental',
    unique_key='datetime'
) }}

with solar as (
    select
        datetime_utc                        as datetime,
        ghi                                 as shortwave_radiation,
        coalesce(ghi_clearness_index, 0)    as clearsky_index,
        case when ghi > 0 then 1 else 0 end as is_daylight
    from {{ ref('copernicus_solar_radiation') }}
    where datetime_utc is not null
    {% if is_incremental() %}
        and datetime_utc > (select max(datetime) from {{ this }})
    {% endif %}
),

-- ERA5 temperature: 6-hourly, 2 grid points → average per timestamp
era5_temp as (
    select
        datetime_utc,
        avg(value) - 273.15  as temperature_2m  -- Kelvin → Celsius
    from {{ ref('copernicus_era5_single_levels') }}
    where variable_name = '2t'
    group by datetime_utc
),

-- Interpolate ERA5 6-hourly temperature to hourly via LOCF, but only within
-- 12 hours of an actual reading to avoid carrying stale values across months
temp_hourly as (
    select
        s.datetime,
        (
            select t.temperature_2m
            from era5_temp t
            where t.datetime_utc <= s.datetime
              and t.datetime_utc >= s.datetime - interval '12 hours'
            order by t.datetime_utc desc
            limit 1
        ) as temperature_2m
    from solar s
),

joined as (
    select
        s.datetime,
        s.shortwave_radiation,
        t.temperature_2m,
        s.clearsky_index,
        s.is_daylight,
        -- Derived: null when temperature is unavailable (ERA5 lag)
        case when t.temperature_2m is not null
            then greatest(0, 18.0 - t.temperature_2m)
        end as heating_degree,
        case when t.temperature_2m is not null
            then greatest(0, t.temperature_2m - 24.0)
        end as cooling_degree
    from solar s
    left join temp_hourly t on s.datetime = t.datetime
)

select * from joined
