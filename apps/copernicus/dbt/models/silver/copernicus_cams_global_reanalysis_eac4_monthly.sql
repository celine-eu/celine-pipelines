{{ config(
    materialized='incremental',
    unique_key='record_id'
) }}

with base as (
    select
        -- surrogate key from important dimensions
        md5(concat_ws('||',
            coalesce(cast(datetime_utc as text), ''),
            coalesce(cast(latitude as text), ''),
            coalesce(cast(longitude as text), ''),
            coalesce(level_type, ''),
            coalesce(cast(level as text), ''),
            coalesce(variable_name, ''),
            coalesce(cast(forecast_step as text), ''),
            coalesce(data_type, '')
        )) as record_id,

        datetime_utc,
        latitude,
        longitude,
        level_type,
        level,
        variable_name,
        value,
        forecast_step,
        data_type
    from {{ ref('stg_cams_global_reanalysis_eac4_monthly') }}
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

        -- add time breakdowns
        date_trunc('month', datetime_utc) as date_month,
        extract(hour from datetime_utc)   as hour_utc
    from base
)

select * from enriched
