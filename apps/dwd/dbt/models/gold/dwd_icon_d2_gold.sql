{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = [
        'run_time_utc',
        'forecast_hour',
        'lat',
        'lon'
    ],
    on_schema_change = 'sync_all_columns'
) }}

with hourly_values as (

    select
        run_time_utc,
        forecast_hour,
        interval_end_utc,
        valid_datetime_rome,
        lat,
        lon,
        name,
        step_type,

        -- de-accumulate / convert to hourly values (raw units)
        case step_type
            when 'avg' then
                value * forecast_hour
                - coalesce(prev_value * prev_forecast_hour, 0)
            when 'accum' then
                value - coalesce(prev_value, 0)
            when 'instant' then
                value
        end as hourly_value

    from {{ ref('dwd_icon_d2_silver') }}
    where forecast_hour > 0  -- step 0 has no meaningful hourly delta

),

unit_converted as (

    select
        run_time_utc,
        forecast_hour,
        interval_end_utc,
        valid_datetime_rome,
        lat,
        lon,
        name,

        case name
            -- K → °C
            when '2t'         then hourly_value - 273.15
            -- W/m² already, clamp negatives (nighttime float noise)
            when 'avg_snswrf' then greatest(hourly_value, 0)
            -- % already, no conversion
            when 'CLCT'       then hourly_value
            -- kg/m² = mm already, clamp negatives
            when 'tp'         then greatest(hourly_value, 0)
        end as value

    from hourly_values

),

pivoted as (

    select
        run_time_utc,
        forecast_hour,
        interval_end_utc                       as valid_datetime_utc,
        valid_datetime_rome,
        lat,
        lon,

        max(case when name = '2t'         then value end) as temperature_2m,
        max(case when name = 'avg_snswrf' then value end) as shortwave_radiation,
        max(case when name = 'CLCT'       then value end) as cloud_cover,
        max(case when name = 'tp'         then value end) as precipitation

    from unit_converted
    group by
        run_time_utc,
        forecast_hour,
        interval_end_utc,
        valid_datetime_rome,
        lat,
        lon

)

select
    run_time_utc,
    forecast_hour,
    valid_datetime_utc,
    valid_datetime_rome,
    lat,
    lon,

    -- Open-Meteo compatible columns & units
    round(temperature_2m::numeric,     1) as temperature_2m,        -- °C
    round(shortwave_radiation::numeric, 1) as shortwave_radiation,  -- W/m² (hourly avg)
    round(cloud_cover::numeric,         0) as cloud_cover,          -- % (0-100)
    round(precipitation::numeric,       2) as precipitation         -- mm (hourly sum)

from pivoted
where temperature_2m        is not null
  and shortwave_radiation   is not null
  and cloud_cover           is not null
  and precipitation         is not null
  -- keep only 48h window in local time
  and valid_datetime_rome < run_time_utc at time zone 'Europe/Rome'
                            + interval '48 hours'

{% if is_incremental() %}
  and run_time_utc >= (
      select coalesce(max(run_time_utc), '1970-01-01')
      from {{ this }}
  )
{% endif %}