{{ config(materialized='table', schema='gold') }}

{#
    Two-axis heat status for today, from observations: the nowcast twin of
    grid_heat_status.

    Air axis: heat_daily_obs (provider-agnostic daily max temperature per
    observation point), against the altitude-band P90 with the gaps-and-islands
    heat-day streak of grid_heat_risks_now, restricted to today. The streak is
    translated into the same GREEN / ORANGE / RED vocabulary as the forecast air
    tier, so the matrix reads one language on both sides:

        no heat stress          -> GREEN
        heat stress, streak < 3 -> ORANGE
        heat stress, streak >= 3 -> RED

    Soil axis: the latest complete om_soil_heat_risk row on or before today at
    the nearest soil point (10 km), the soil temperature being a slow signal
    with no useful sub-daily observation. Same as-of behaviour and same GREEN
    fallback as the forecast model.

    Grain: one row per observation point, date = current_date.
#}

{% set obs  = source('heat_obs', 'heat_daily_obs') %}
{% set soil = source('om_soil', 'om_soil_heat_risk') %}

with daily_obs as (

    select
        date,
        lat,
        lon,
        ST_SetSRID(
            ST_MakePoint(lon::double precision, lat::double precision),
            4326
        )::geography as geoposition,
        elevation_m,
        altitude_band,
        temp_max_c,
        latest_obs
    from {{ obs }}
    where date >= current_date - interval '7 days'

),

with_threshold as (

    select
        *,
        {{ grid_heat_p90_threshold('altitude_band', 'date') }} as p90_threshold,
        case
            when extract(month from date) between 5 and 9
                 and temp_max_c > {{ grid_heat_p90_threshold('altitude_band', 'date') }}
            then true
            else false
        end as is_heat_stress
    from daily_obs

),

islands as (

    select
        *,
        date - (row_number() over (
            partition by lat, lon, is_heat_stress
            order by date
        ))::int as island_id
    from with_threshold

),

streaks as (

    select
        *,
        case
            when is_heat_stress then
                row_number() over (
                    partition by lat, lon, island_id, is_heat_stress
                    order by date
                )
            else 0
        end as consecutive_heat_days
    from islands

),

air_today as (

    select
        date,
        lat,
        lon,
        geoposition,
        ST_Transform(geoposition::geometry, 32632)  as geom_utm,
        temp_max_c,
        p90_threshold,
        consecutive_heat_days,
        elevation_m,
        altitude_band,
        latest_obs                                  as observed_at,
        case
            when not is_heat_stress          then 'GREEN'
            when consecutive_heat_days >= 3  then 'RED'
            else 'ORANGE'
        end                                         as air_heat_tier
    from streaks
    where date = current_date

),

soil_asof as materialized (

    {#- Latest complete soil row per point, on or before today.
        MATERIALIZED on purpose: referenced once from inside the LATERAL below,
        so Postgres would otherwise inline it and re-run the whole as-of scan
        for every outer observation point. -#}
    select distinct on (s.lat, s.lon)
        s.lat                                       as soil_lat,
        s.lon                                       as soil_lon,
        ST_Transform(s.geoposition::geometry, 32632) as geom_utm,
        s.date                                      as soil_asof_date,
        s.soil_status,
        s.soil_data_complete,
        s.soil_1m_c,
        s.soil7_mean_c,
        s.soil7_p90_c,
        s.soil7_n_days
    from {{ soil }} s
    where s.soil_data_complete
      and s.date <= current_date
      and s.date >= current_date - 10
    order by s.lat, s.lon, s.date desc

),

matched as (

    select
        a.*,
        sa.soil_asof_date,
        sa.soil_status                              as soil_status_raw,
        sa.soil_data_complete                       as soil_data_complete_raw,
        sa.soil_1m_c,
        sa.soil7_mean_c,
        sa.soil7_p90_c,
        sa.soil7_n_days,
        sa.dist_m                                   as soil_dist_m
    from air_today a
    left join lateral (
        select
            s.*,
            ST_Distance(s.geom_utm, a.geom_utm)     as dist_m
        from soil_asof s
        where ST_DWithin(s.geom_utm, a.geom_utm, 10000)
        order by s.geom_utm <-> a.geom_utm
        limit 1
    ) sa on true

),

statused as (

    select
        *,
        coalesce(soil_status_raw, 'GREEN')          as soil_status,
        coalesce(soil_data_complete_raw, false)     as soil_data_complete
    from matched

)

select
    md5(date::text || '|' || lat::text || '|' || lon::text) as status_key,
    date,
    lat,
    lon,
    geoposition,

    air_heat_tier,
    temp_max_c,
    p90_threshold,
    consecutive_heat_days,
    elevation_m,
    altitude_band,
    observed_at,

    soil_status,
    soil_data_complete,
    soil_asof_date,
    soil_dist_m,
    soil_1m_c,
    soil7_mean_c,
    soil7_p90_c,
    soil7_n_days,

    case
        when soil_status = 'ORANGE' and air_heat_tier = 'RED' then 'RED'
        when soil_status = 'ORANGE'                           then 'ORANGE'
        else 'GREEN'
    end                                             as heat_status

from statused
