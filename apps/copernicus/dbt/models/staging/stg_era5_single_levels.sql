{{ config(
    materialized='view'
) }}

with source as (

    select * 
    from {{ source('reanalysis', 'reanalysis_era5_single_levels') }}

)

, cleaned as (
    select
        cast(datetime as timestamp)                   as datetime_utc,
        nullif(lat, null)::float                     as latitude,
        nullif(lon, null)::float                     as longitude,
        nullif(level_type, '')                        as level_type,
        nullif(level, null)::int                     as level,
        nullif(name, '')                              as variable_name,
        nullif(value, null)::float                   as value,
        nullif(forecast_step, null)::int             as forecast_step,
        nullif(data_type, '')                         as data_type
    from source
)

select * from cleaned
