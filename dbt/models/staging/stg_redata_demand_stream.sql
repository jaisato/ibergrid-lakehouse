-- Latest revision per (series_id, ts_utc) in the append-only stream table (ADR 0006):
-- highest (coalesce(stream_epoch, 0), kafka_offset) wins; each `stream reset` increments epoch.
select
    series_id,
    series_title,
    electric_system,
    ts_utc,
    cast(ts_utc at time zone 'Europe/Madrid' as date) as madrid_date,
    value,
    series_last_update,
    content_sha256,
    coalesce(stream_epoch, 0) as stream_epoch,
    kafka_offset,
    ingested_at
from {{ source('bronze', 'redata_demand_stream') }}
qualify row_number() over (
    partition by series_id, ts_utc
    order by coalesce(stream_epoch, 0) desc, kafka_offset desc
) = 1
