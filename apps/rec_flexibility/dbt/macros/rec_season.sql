{#
  rec_season_start(date_expr)

  Map a date/timestamp expression to the start date of its season. Seasons are
  calendar-aligned blocks of `season_months` months anchored at `season_anchor_date`
  (both set in dbt_project.yml vars; mirrored in flexibility_config.yaml `season`).

  Example with anchor 2025-09-01, season_months 2:
    2025-09-15 -> 2025-09-01   (Sep-Oct)
    2025-10-31 -> 2025-09-01
    2025-11-01 -> 2025-11-01   (Nov-Dec)
    2026-05-28 -> 2026-05-01   (May-Jun 2026)

  Used to bucket points into resettable seasons. The displayed leaderboard total is the
  sum of points within the current season; it returns to 0 at each season boundary.
#}
{% macro rec_season_start(date_expr) %}
(
    date_trunc('month', cast('{{ var("season_anchor_date") }}' as date))
    + (
        (
            floor(
                (
                    (extract(year  from {{ date_expr }})
                        - extract(year  from cast('{{ var("season_anchor_date") }}' as date))) * 12
                  + (extract(month from {{ date_expr }})
                        - extract(month from cast('{{ var("season_anchor_date") }}' as date)))
                ) / {{ var("season_months") }}::numeric
            )::int * {{ var("season_months") }}::int
        ) * interval '1 month'
    )
)::date
{% endmacro %}
