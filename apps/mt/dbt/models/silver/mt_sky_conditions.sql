{{ config(materialized='table') }}

select
    id,
    icon_day,
    icon_night,
    name_deu,
    name_eng,
    name_ita,
    name_lld
from {{ ref('stg_mt_sky_conditions') }}
