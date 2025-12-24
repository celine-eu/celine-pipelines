{{ config(
    materialized='view'
) }}

{% set rel = source('reanalysis','cams_global_reanalysis_eac4_monthly') %}

{% if adapter.get_relation(
      database=rel.database,
      schema=rel.schema,
      identifier=rel.identifier
   ) %}

    select
        cast(datetime as timestamp) as datetime_utc,
        lat::float as latitude,
        lon::float as longitude,
        level_type,
        level::int,
        name as variable_name,
        value::float,
        forecast_step::int,
        data_type
    from {{ rel }}

{% else %}

    -- empty but schema-compatible
    select
        null::timestamp as datetime_utc,
        null::float as latitude,
        null::float as longitude,
        null::text as level_type,
        null::int as level,
        null::text as variable_name,
        null::float as value,
        null::int as forecast_step,
        null::text as data_type
    where false

{% endif %}