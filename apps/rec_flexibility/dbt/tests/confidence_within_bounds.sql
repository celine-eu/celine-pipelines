-- confidence is either null (no scored history) or a clamped 2-decimal rate
select *
from {{ ref('rec_flexibility_windows_community') }}
where confidence is not null
  and (confidence < 0.30 or confidence > 0.95)
