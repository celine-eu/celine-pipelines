-- An alert cannot expire before it starts.
--
-- The facade normalises alert windows from providers that express them
-- differently — absolute instants, local wall-clock, or a start plus a
-- duration. A timezone or parsing mistake in that normalisation inverts the
-- window, and an inverted window reads as "already expired" to every consumer
-- filtering on `expires_at > now()`. The alert then disappears from the webapp
-- silently, which is the worst possible failure for this dataset.
--
-- Rows with a null expiry are open-ended alerts and are not violations.

select
    location_id,
    provider,
    alert_id,
    event,
    starts_at,
    expires_at
from {{ ref('weather_alerts_active') }}
where expires_at < starts_at
