{{ config(
    materialized         = 'incremental',
    unique_key           = ['date', 'lat', 'lon'],
    incremental_strategy = 'merge'
) }}

{#
    Gold layer: daily heat risk per grid point.

    Computes heat stress using altitude-band P90 thresholds (Crespi 1981-2010).
    Heat stress is only evaluated May-Sep; outside warm season -> GREEN.

    Consecutive-day logic (gaps-and-islands):
      RED:    >= 3 consecutive heat stress days (heatwave)
      ORANGE: 1-2 heat stress days
      GREEN:  no heat stress

    P90 thresholds (daily Tmax, deg C):
      Band    | May | Jun | Jul | Aug | Sep
      <500m   |  22 |  26 |  27 |  27 |  23
      500-1km |  18 |  22 |  23 |  23 |  19
      >1km    |  14 |  18 |  19 |  19 |  15
#}

with base as (

    select *
    from {{ ref('om_heat_daily') }}

    {% if is_incremental() %}
    where date >= (
        select coalesce(max(date) - interval '5 days', '1970-01-01'::date)
        from {{ this }}
    )
    {% endif %}

),

-- Assign P90 threshold based on altitude band + month
with_threshold as (

    select
        *,
        case
            -- Low altitude (<500m)
            when altitude_band = 'low' then
                case extract(month from date)
                    when 5 then 22.0
                    when 6 then 26.0
                    when 7 then 27.0
                    when 8 then 27.0
                    when 9 then 23.0
                    else null
                end
            -- Mid altitude (500-1000m)
            when altitude_band = 'mid' then
                case extract(month from date)
                    when 5 then 18.0
                    when 6 then 22.0
                    when 7 then 23.0
                    when 8 then 23.0
                    when 9 then 19.0
                    else null
                end
            -- High altitude (>1000m)
            when altitude_band = 'high' then
                case extract(month from date)
                    when 5 then 14.0
                    when 6 then 18.0
                    when 7 then 19.0
                    when 8 then 19.0
                    when 9 then 15.0
                    else null
                end
        end as p90_threshold,

        -- Heat stress: temp exceeds P90 AND within warm season (May-Sep)
        case
            when extract(month from date) between 5 and 9
                 and temp_max_c > (
                     case
                         when altitude_band = 'low' then
                             case extract(month from date)
                                 when 5 then 22.0 when 6 then 26.0
                                 when 7 then 27.0 when 8 then 27.0
                                 when 9 then 23.0 end
                         when altitude_band = 'mid' then
                             case extract(month from date)
                                 when 5 then 18.0 when 6 then 22.0
                                 when 7 then 23.0 when 8 then 23.0
                                 when 9 then 19.0 end
                         when altitude_band = 'high' then
                             case extract(month from date)
                                 when 5 then 14.0 when 6 then 18.0
                                 when 7 then 19.0 when 8 then 19.0
                                 when 9 then 15.0 end
                     end
                 )
            then true
            else false
        end as is_heat_stress

    from base

),

-- Gaps-and-islands: identify consecutive heat stress streaks per grid point
islands as (

    select
        *,
        -- Subtract row_number from date to get an island identifier
        -- Consecutive TRUE days share the same island_id
        date - (row_number() over (
            partition by lat, lon, is_heat_stress
            order by date
        ))::int as island_id
    from with_threshold

),

-- Count consecutive days within each island
streaks as (

    select
        *,
        case
            when is_heat_stress then
                count(*) over (
                    partition by lat, lon, island_id, is_heat_stress
                )
            else 0
        end as streak_length,
        case
            when is_heat_stress then
                row_number() over (
                    partition by lat, lon, island_id, is_heat_stress
                    order by date
                )
            else 0
        end as day_in_streak
    from islands

),

final as (

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
        p90_threshold,
        is_heat_stress,
        day_in_streak as consecutive_heat_days,

        case
            when not is_heat_stress then 'GREEN'
            when day_in_streak >= 3  then 'RED'
            else 'ORANGE'
        end as heat_risk_tier,

        forecast_model,
        _sdc_extracted_at
    from streaks

)

select * from final
