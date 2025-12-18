-- staging layer for raw.forecast_stream
select
    synced_at,
    lat,
    lon,
    location_id,
    timezone,
    timezone_offset,
    current,
    minutely,
    hourly,
    daily,
    alerts,
    _sdc_extracted_at,
    _sdc_received_at,
    _sdc_batched_at,
    _sdc_deleted_at,
    _sdc_sequence,
    _sdc_table_version,
    _sdc_sync_started_at
from {{ source('raw', 'forecast_stream') }}
