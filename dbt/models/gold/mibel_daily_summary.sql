-- Daily MIBEL executive summary (Madrid day, ADR 0007):
-- Keeps peninsular_demand_mwh and non_peninsular_demand_mwh as separate columns (never mixed).
with non_peninsular_daily as (
    select
        madrid_date as delivery_date,
        round(sum(demand_mw), 4) as non_peninsular_demand_mwh
    from {{ ref('demand_system_hourly') }}
    where system_scope = 'non_peninsular'
    group by madrid_date
),

hourly as (
    select * from {{ ref('mibel_hourly_market') }}
)

select
    h.delivery_date,
    cast(count(*) as integer) as hours_in_day,
    round(avg(h.price_es_eur_mwh), 6) as price_es_mean_eur_mwh,
    min(h.price_es_eur_mwh) as price_es_min_eur_mwh,
    max(h.price_es_eur_mwh) as price_es_max_eur_mwh,
    round(
        avg(h.price_es_eur_mwh) filter (where h.madrid_hour between 8 and 19),
        6
    ) as price_es_peak_eur_mwh,
    round(
        avg(h.price_es_eur_mwh) filter (where h.madrid_hour < 8 or h.madrid_hour > 19),
        6
    ) as price_es_offpeak_eur_mwh,
    round(avg(h.price_pt_eur_mwh), 6) as price_pt_mean_eur_mwh,
    round(avg(h.spread_es_minus_pt_eur_mwh), 6) as spread_mean_eur_mwh,
    round(avg(abs(h.spread_es_minus_pt_eur_mwh)), 6) as spread_abs_mean_eur_mwh,
    cast(sum(case when h.is_decoupled then 1 else 0 end) as integer) as decoupled_hours,
    cast(
        sum(case when h.is_zero_or_negative_es then 1 else 0 end) as integer
    ) as zero_or_negative_es_hours,
    round(sum(h.peninsular_demand_mw), 4) as peninsular_demand_mwh,
    max(h.peninsular_demand_mw) as peninsular_peak_demand_mw,
    max(h.demand_ramp_1h_mw) filter (where h.is_evening_ramp_window) as evening_ramp_max_mw,
    max(np.non_peninsular_demand_mwh) as non_peninsular_demand_mwh,
    max(h.peninsular_renewable_share_pct) as peninsular_renewable_share_pct,
    max(h.peninsular_solar_share_pct) as peninsular_solar_share_pct,
    max(h.peninsular_wind_share_pct) as peninsular_wind_share_pct,
    'Source: OMIE (omie.es) & Red Eléctrica, REData' as source_attribution,
    cast(current_timestamp as timestamptz) as updated_at_utc
from hourly h
left join non_peninsular_daily np
    on h.delivery_date = np.delivery_date
group by h.delivery_date
order by h.delivery_date
