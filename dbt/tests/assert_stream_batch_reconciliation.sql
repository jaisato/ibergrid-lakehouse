-- Closed Madrid days present in batch demand_realtime (all 276/288/300 Real points) must
-- always resolve to source_lane = 'batch' in silver.demand_national_5min, even when the stream
-- table also holds events for those days (ADR 0006).
with batch_closed_days as (
    select madrid_date
    from {{ ref('stg_redata_observations') }}
    where dataset = 'demand_realtime'
      and electric_system = 'national'
      and series_id = '2037'
    group by madrid_date
    having count(*) >= cast(
        extract(
            epoch from (
                (cast(madrid_date + interval 1 day as timestamp) at time zone 'Europe/Madrid')
                - (cast(madrid_date as timestamp) at time zone 'Europe/Madrid')
            )
        ) / 300
        as bigint
    )
)

select
    d.ts_utc,
    d.madrid_date,
    d.source_lane
from {{ ref('demand_national_5min') }} d
inner join batch_closed_days c
    on d.madrid_date = c.madrid_date
where d.source_lane <> 'batch'
