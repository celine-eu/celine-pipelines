{{ config(materialized='table', schema='gold') }}

{#
    Distinct topology entities for UI filter autocomplete.

    Source: silver_grid_ac_line_segment (the CIM interface).
    Updated on the monthly topology cadence — no weather join.

    Powers the /filters DT endpoint, providing the complete set of
    available filter values (substations, lines, units, municipalities)
    independent of the daily weather-risk tables.

    Using the silver source directly (rather than the gold risk tables)
    ensures filter options are always complete — a line with no weather
    station nearby still appears in the filter, it just has no risk rows.
#}

select distinct
    dso_id,
    line_name,
    parent_substation_name,
    operational_unit,
    municipality,
    conductor_type
from {{ source('grid_silver', 'silver_grid_ac_line_segment') }}
order by parent_substation_name, line_name
