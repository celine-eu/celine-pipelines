{{ config(
    materialized         = 'incremental',
    unique_key           = ['date', 'lat', 'lon'],
    incremental_strategy = 'merge',
    alias                = 'heat_daily_obs'
) }}

{#
    Daily max observed temperature per active weather station.

    Produces the heat_daily_obs proxy table consumed by downstream heat risk
    pipelines (e.g. grid nowcasting).  The contract is:

        date, lat, lon, elevation_m, altitude_band, temp_max_c, latest_obs

    Source-specific details (station codes, network) stay internal.
    Altitude band classification: low (<500m), mid (500-1000m), high (>1000m).
#}

with daily_max as (

    select
        station_code,
        (observed_at at time zone 'Europe/Rome')::date as date,
        max(air_temperature_c) as temp_max_c,
        max(observed_at)       as latest_obs
    from {{ ref('mt_station_observations') }}
    where air_temperature_c is not null

    {% if is_incremental() %}
    and (observed_at at time zone 'Europe/Rome')::date >= (
        select coalesce(max(date) - interval '3 days', '1970-01-01'::date)
        from {{ this }}
    )
    {% endif %}

    group by 1, 2

)

select
    d.date,
    s.latitude                          as lat,
    s.longitude                         as lon,
    s.elevation_m,
    case
        when s.elevation_m < 500  then 'low'
        when s.elevation_m < 1000 then 'mid'
        else 'high'
    end                                 as altitude_band,
    d.temp_max_c,
    d.latest_obs
from daily_max d
join {{ ref('mt_meteo_stations') }} s
    on s.code = d.station_code
where s.active_until is null
   or s.active_until >= current_date
