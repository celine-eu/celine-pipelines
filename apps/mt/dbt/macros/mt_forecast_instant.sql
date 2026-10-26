{#
  Forecast timestamp text → timestamptz.

  tap-meteotrentino 0.5.6+ emits an explicit offset, which is authoritative.

  Up to 0.5.5 it emitted a naive Europe/Rome wall clock: the API's start (00:00Z
  of the extraction day) stepped by elapsed minutes on the wall clock, so after a
  clock change every value is an hour off if read as Europe/Rome. The instant is
  that midnight plus the wall-clock distance from it. Raw keeps both forms side
  by side.
#}
{% macro mt_forecast_instant(column, extracted_at) -%}
    case
        when {{ column }} ~ 'T[0-9:.]+([+-]\d{2}(:?\d{2})?|Z)$'
            then cast({{ column }} as timestamptz)
        else (
            date_trunc('day', {{ extracted_at }})
            + (
                cast({{ column }} as timestamp)
                - (date_trunc('day', {{ extracted_at }}) at time zone 'UTC' at time zone 'Europe/Rome')
            )
        ) at time zone 'UTC'
    end
{%- endmacro %}
