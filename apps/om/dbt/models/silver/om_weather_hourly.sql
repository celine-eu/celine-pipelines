{{ config(
    materialized = 'incremental',
    unique_key   = ['datetime'],
    incremental_strategy = 'merge'
) }}

{#
    Hourly weather features for energy forecasting.
    Adds physics-informed derived features for solar production,
    cloud impact, temperature-driven consumption, and snow risks.

    All constants are parameterized via dbt vars (om_* prefix).
#}

with base as (

    select *
    from {{ ref('stg_om_weather') }}
    {% if is_incremental() %}
    where _sdc_extracted_at > (
        select coalesce(max(_sdc_extracted_at), '1900-01-01'::timestamp)
        from {{ this }}
    )
    {% endif %}

),

-- Step 1: Compute solar position (declination, hour angle, elevation)
solar_position as (

    select
        *,

        -- Solar declination (degrees): axial tilt effect over the year
        23.45 * sin(radians(
            360.0 / 365 * (extract(doy from datetime)::int - 81)
        )) as declination_deg,

        -- Hour angle (degrees): sun position relative to solar noon
        15.0 * (extract(hour from datetime)::int - 12) as hour_angle_deg

    from base

),

solar_elevation as (

    select
        *,

        -- Solar elevation angle (degrees)
        degrees(asin(least(1.0, greatest(-1.0,
            sin(radians({{ var('om_latitude') }})) * sin(radians(declination_deg))
            + cos(radians({{ var('om_latitude') }})) * cos(radians(declination_deg))
              * cos(radians(hour_angle_deg))
        )))) as solar_elevation_deg,

        -- Clear-sky GHI approximation (Ineichen-Perez simplified)
        greatest(0, 1000.0 * greatest(0,
            sin(radians({{ var('om_latitude') }})) * sin(radians(declination_deg))
            + cos(radians({{ var('om_latitude') }})) * cos(radians(declination_deg))
              * cos(radians(hour_angle_deg))
        )) as clearsky_ghi_approx

    from solar_position

),

-- Step 2: Add all derived features
with_features as (

    select
        -- === RAW COLUMNS ===
        datetime,
        direct_radiation,
        diffuse_radiation,
        direct_normal_irradiance,
        shortwave_radiation,
        cloud_cover,
        cloud_cover_low,
        cloud_cover_mid,
        cloud_cover_high,
        temperature_2m,
        apparent_temperature,
        precipitation,
        rain,
        snowfall,
        relative_humidity_2m,
        _sdc_extracted_at,

        -- === SOLAR FEATURES ===
        solar_elevation_deg                            as solar_elevation,
        clearsky_ghi_approx,

        -- Clear-sky index: actual / theoretical (cloud attenuation metric)
        case
            when clearsky_ghi_approx > {{ var('om_clearsky_ghi_threshold') }}
            then least({{ var('om_clearsky_index_cap') }}, greatest(0,
                shortwave_radiation / clearsky_ghi_approx
            ))
            else 0
        end                                            as clearsky_index,

        -- Effective solar for PV (direct + bifacial gain * diffuse)
        coalesce(direct_radiation, 0)
            + {{ var('om_bifacial_gain_factor') }} * coalesce(diffuse_radiation, 0)
                                                       as effective_solar_pv,

        -- === CLOUD FEATURES ===

        -- Weighted cloud cover: low clouds impact solar most
        {{ var('om_cloud_weight_low') }} * coalesce(cloud_cover_low, 0)
            + {{ var('om_cloud_weight_mid') }} * coalesce(cloud_cover_mid, 0)
            + {{ var('om_cloud_weight_high') }} * coalesce(cloud_cover_high, 0)
                                                       as cloud_cover_weighted,

        -- Cloud variability (high = harder to forecast)
        abs(cloud_cover - lag(cloud_cover) over (
            order by datetime
        ))                                              as cloud_cover_diff,

        -- === TEMPERATURE FEATURES ===

        -- Heating degree hours (consumption driver in winter)
        greatest(0, {{ var('om_heating_base_temp') }} - coalesce(temperature_2m, 0))
                                                       as heating_degree,

        -- Cooling degree hours (consumption driver in summer)
        greatest(0, coalesce(temperature_2m, 0) - {{ var('om_cooling_base_temp') }})
                                                       as cooling_degree,

        -- PV efficiency correction (loss per C above reference temp)
        1 - {{ var('om_pv_temp_loss_rate') }} * greatest(0,
            coalesce(temperature_2m, 0) - {{ var('om_pv_ref_temp') }}
        )                                               as pv_temp_factor,

        -- === SNOW FEATURES (critical for Alps) ===

        -- Rolling snowfall accumulation
        sum(coalesce(snowfall, 0)) over (
            order by datetime
            rows between {{ var('om_snowfall_rolling_hours') - 1 }} preceding and current row
        )                                               as recent_snowfall,

        -- === TIME FEATURES ===
        extract(hour from datetime)::int                as hour,
        extract(dow from datetime)::int                 as day_of_week,
        extract(month from datetime)::int               as month,
        case
            when extract(hour from datetime) between {{ var('om_daylight_start_hour') }} and {{ var('om_daylight_end_hour') }}
            then true else false
        end                                             as is_daylight

    from solar_elevation

)

select * from with_features
