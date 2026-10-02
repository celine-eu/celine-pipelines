{{ config(materialized='table', schema='gold') }}

{#
    Two-axis heat status per Open-Meteo point and forecast date.

    The air axis alone (om_heat_risk: Tmax over the altitude-band P90, with a
    heat-day streak) fires on every hot afternoon and says nothing about the
    ground a cable is buried in. The soil axis (om_soil_heat_risk: 1 m soil
    temperature against its own 7-day P90) is the slow one: it stays warm for
    days and is what actually derates a cable.

        soil ORANGE + air RED -> RED     (both axes hot)
        soil ORANGE           -> ORANGE  (the ground is warm)
        otherwise             -> GREEN

    The two Open-Meteo tables do NOT share coordinates: om_heat_risk stores the
    lat/lon the API snapped the request to, om_soil_heat_risk the requested grid
    point. They are matched by nearest geoposition (10 km), never by equality.

    The soil row is taken as-of: the most recent complete row at that point on
    or before the date, up to 10 days back, exposed as soil_asof_date so a
    consumer sees how stale the soil axis is. No soil row in reach means the
    soil axis reads GREEN (soil_asof_date NULL), and per the matrix below GREEN
    maps to NORMAL regardless of tier, so a soil-feed outage switches the whole
    heat vector off rather than falling back to an air-only assessment. Only
    grid_heat_status_has_soil (warn) and om's om_soil_is_fresh (warn) signal
    this has happened.

    Date range: today + 2 days ahead (same idiom as grid_heat_risks).
    Grain: date x heat point, one row per om_heat_risk row.
#}

{% set heat = source('om_heat', 'om_heat_risk') %}
{% set soil = source('om_soil', 'om_soil_heat_risk') %}

with date_range as (

    select generate_series(
        {% if var('start_date', none) is not none %}
            '{{ var("start_date") }}'::date,
        {% else %}
            CURRENT_DATE,
        {% endif %}
        CURRENT_DATE + interval '2 days',
        interval '1 day'
    )::date as date

),

heat_points as (

    select
        h.date,
        h.lat,
        h.lon,
        h.geoposition,
        ST_Transform(h.geoposition::geometry, 32632)    as geom_utm,
        h.heat_risk_tier                                as air_heat_tier,
        h.temp_max_c,
        h.p90_threshold,
        h.consecutive_heat_days,
        h.elevation_m,
        h.altitude_band,
        h.forecast_model
    from {{ heat }} h
    join date_range d on h.date = d.date

),

soil_asof as materialized (

    {#- Most recent complete soil row per point for each date of the range.
        MATERIALIZED on purpose: this CTE is referenced once, from inside the
        LATERAL below, so Postgres would otherwise inline it and re-run the
        whole as-of scan for every outer heat point. -#}
    select distinct on (s.lat, s.lon, d.date)
        d.date                                          as date,
        s.lat                                           as soil_lat,
        s.lon                                           as soil_lon,
        ST_Transform(s.geoposition::geometry, 32632)    as geom_utm,
        s.date                                          as soil_asof_date,
        s.soil_status,
        s.soil_data_complete,
        s.soil_1m_c,
        s.soil7_mean_c,
        s.soil7_p90_c,
        s.soil7_n_days
    from {{ soil }} s
    join date_range d
        on s.date <= d.date
       and s.date >= d.date - 10
    where s.soil_data_complete
    order by s.lat, s.lon, d.date, s.date desc

),

matched as (

    select
        h.*,
        sa.soil_lat,
        sa.soil_lon,
        sa.soil_asof_date,
        sa.soil_status                                  as soil_status_raw,
        sa.soil_data_complete                           as soil_data_complete_raw,
        sa.soil_1m_c,
        sa.soil7_mean_c,
        sa.soil7_p90_c,
        sa.soil7_n_days,
        sa.dist_m                                       as soil_dist_m
    from heat_points h
    left join lateral (
        {#- Nearest soil point within 10 km; ties inside a point resolve on the
            as-of ordering already applied in soil_asof. -#}
        select
            s.*,
            ST_Distance(s.geom_utm, h.geom_utm)         as dist_m
        from soil_asof s
        where s.date = h.date
          and ST_DWithin(s.geom_utm, h.geom_utm, 10000)
        order by s.geom_utm <-> h.geom_utm
        limit 1
    ) sa on true

),

statused as (

    select
        *,
        coalesce(soil_status_raw, 'GREEN')              as soil_status,
        coalesce(soil_data_complete_raw, false)         as soil_data_complete
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
    forecast_model,

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
    end                                                 as heat_status

from statused
