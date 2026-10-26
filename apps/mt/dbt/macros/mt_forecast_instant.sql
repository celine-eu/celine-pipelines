{#
  Forecast timestamp text → timestamptz.

  tap-meteotrentino up to 0.5.5 emits a naive Europe/Rome wall clock; later
  versions emit an explicit offset. Raw keeps both forms side by side, so an
  offset is authoritative when present and a naive value is read as Europe/Rome.
#}
{% macro mt_forecast_instant(column) -%}
    case
        when {{ column }} ~ 'T[0-9:.]+([+-]\d{2}(:?\d{2})?|Z)$'
            then cast({{ column }} as timestamptz)
        else cast({{ column }} as timestamp) at time zone 'Europe/Rome'
    end
{%- endmacro %}
