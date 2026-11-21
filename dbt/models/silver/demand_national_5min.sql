-- Reconciled 5-minute national demand (ADR 0003, ADR 0006):
-- Batch is authoritative for closed Madrid days (all 276/288/300 Real points present in batch);
-- deduplicated stream supplies open/partial days and beats partial batch rows when both exist.
with batch_points as (
    select
        series_id,
        ts_utc,
        madrid_date,
        value,
        series_last_update
    from {{ ref('stg_redata_observations') }}
    where dataset = 'demand_realtime'
      and electric_system = 'national'
),

batch_closed_days as (
    select
        madrid_date
    from batch_points
    where series_id = '2037'
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
),

stream_points as (
    select
        series_id,
        ts_utc,
        madrid_date,
        value,
        series_last_update
    from {{ ref('stg_redata_demand_stream') }}
    where electric_system = 'national'
      and madrid_date not in (select madrid_date from batch_closed_days)
),

combined as (
    select
        series_id,
        ts_utc,
        madrid_date,
        value,
        series_last_update,
        'stream' as source_lane,
        1 as lane_priority
    from stream_points

    union all

    select
        series_id,
        ts_utc,
        madrid_date,
        value,
        series_last_update,
        'batch' as source_lane,
        2 as lane_priority
    from batch_points
),

reconciled as (
    select
        series_id,
        ts_utc,
        madrid_date,
        value,
        series_last_update,
        source_lane
    from combined
    qualify row_number() over (
        partition by series_id, ts_utc
        order by lane_priority asc
    ) = 1
)

select
    ts_utc,
    madrid_date,
    max(case when series_id = '2037' then value end) as demand_real_mw,
    max(case when series_id = '2052' then value end) as demand_forecast_mw,
    max(case when series_id = '2053' then value end) as demand_scheduled_mw,
    max(case when series_id = '2054' then value end) as demand_total_scheduled_mw,
    case when bool_or(source_lane = 'stream') then 'stream' else 'batch' end as source_lane,
    max(series_last_update) as series_last_update_utc
from reconciled
group by ts_utc, madrid_date
order by ts_utc
