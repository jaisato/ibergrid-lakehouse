-- Standardized hourly MIBEL price series across both contract eras (v1 60 min 1:1;
-- v2 15 min arithmetic mean of the 4 quarter-hours per UTC hour), enriched with REData peninsular
-- PVPC and spot price for cross-source reconciliation (ADR 0004, ADR 0007).
with omie_hourly as (
    select
        time_bucket(interval '1 hour', interval_start_utc) as hour_utc,
        delivery_date,
        cast(
            extract(
                hour from (
                    time_bucket(interval '1 hour', interval_start_utc)
                    at time zone 'Europe/Madrid'
                )
            ) as integer
        ) as madrid_hour,
        cast(max(contract_version) as integer) as contract_version,
        cast(count(*) as integer) as periods_in_hour,
        round(avg(price_es_eur_mwh), 6) as price_es_eur_mwh,
        min(price_es_eur_mwh) as price_es_min_eur_mwh,
        max(price_es_eur_mwh) as price_es_max_eur_mwh,
        round(avg(price_pt_eur_mwh), 6) as price_pt_eur_mwh,
        round(avg(spread_es_minus_pt_eur_mwh), 6) as spread_es_minus_pt_eur_mwh,
        bool_or(is_decoupled) as is_decoupled,
        avg(price_es_eur_mwh) <= 0.0 as is_zero_or_negative_es
    from {{ ref('mibel_prices_period') }}
    group by 1, 2, 3
),

redata_market_hourly as (
    select
        time_bucket(interval '1 hour', ts_utc) as hour_utc,
        avg(case when series_id = '1001' then value end) as pvpc_eur_mwh,
        round(avg(case when series_id = '600' then value end), 6) as redata_spot_eur_mwh
    from {{ ref('stg_redata_observations') }}
    where dataset = 'market_prices'
      and electric_system = 'peninsular'
    group by 1
)

select
    o.hour_utc,
    o.delivery_date,
    o.madrid_hour,
    o.contract_version,
    o.periods_in_hour,
    o.price_es_eur_mwh,
    o.price_es_min_eur_mwh,
    o.price_es_max_eur_mwh,
    o.price_pt_eur_mwh,
    o.spread_es_minus_pt_eur_mwh,
    o.is_decoupled,
    o.is_zero_or_negative_es,
    r.pvpc_eur_mwh,
    r.redata_spot_eur_mwh,
    case
        when r.redata_spot_eur_mwh is not null
            then round(o.price_es_eur_mwh - r.redata_spot_eur_mwh, 6)
    end as omie_redata_diff_eur_mwh
from omie_hourly o
left join redata_market_hourly r
    on o.hour_utc = r.hour_utc
order by o.hour_utc
