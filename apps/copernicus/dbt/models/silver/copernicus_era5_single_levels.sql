{{ config(
    materialized='incremental',
    unique_key='record_id'
) }}

with base as (
    select
        {{ dbt_utils.generate_surrogate_key([
            'datetime_utc', 
            'latitude', 
            'longitude', 
            'level_type', 
            'level', 
            'variable_name', 
            'forecast_step', 
            'data_type'
        ]) }} as record_id,

        datetime_utc,
        latitude,
        longitude,
        level_type,
        level,
        variable_name,
        value,
        forecast_step,
        data_type
    from {{ ref('stg_era5_single_levels') }}
)

, enriched as (
    select
        record_id,
        datetime_utc,
        latitude,
        longitude,
        level_type,
        level,
        variable_name,
        value,
        forecast_step,
        data_type,

        -- useful breakdowns
        date_trunc('day', datetime_utc)       as date_day,
        extract(hour from datetime_utc)       as hour_utc
    from base
)

select * from enriched
