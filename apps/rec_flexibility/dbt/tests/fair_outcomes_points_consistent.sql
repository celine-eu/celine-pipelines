-- Fair gamification: points follow is_kept exactly (fixed points per kept commitment,
-- zero otherwise) and surplus points are 2 per surplus hour; unmatched rows earn nothing.
select commitment_id
from {{ ref('rec_fair_commitment_outcomes') }}
where points <> case when is_kept then {{ var('fair_points_per_kept') }} else 0 end
   or surplus_points <> surplus_hours_kept * {{ var('fair_surplus_points_per_hour') }}
   or (not matched and (points <> 0 or surplus_points <> 0))
   or (reason = 'kept' and not is_kept) or (is_kept and reason <> 'kept')
