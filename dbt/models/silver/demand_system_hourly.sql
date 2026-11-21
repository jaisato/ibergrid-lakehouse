-- Hourly demand per electric system (ADR 0003): strictly separates peninsular from
-- non-peninsular systems (baleares, canarias, ceuta, melilla) and flags Canarias timestamps as
-- unverified pending confirmation of Canarian vs peninsular wall-clock stamps.
select
    ts_utc,
    madrid_date,
    madrid_hour,
    electric_system,
    case
        when electric_system = 'peninsular' then 'peninsular'
        else 'non_peninsular'
    end as system_scope,
    (electric_system <> 'canarias') as ts_utc_verified,
    value as demand_mw,
    series_last_update as series_last_update_utc
from {{ ref('stg_redata_observations') }}
where dataset = 'demand_hourly'
  and series_id = '10297'
order by electric_system, ts_utc
