-- Peninsular hourly market mart (ADR 0007):
-- Standardized OMIE MIBEL ES/PT prices joined strictly to peninsular hourly demand and daily
-- peninsular renewable shares. Non-peninsular systems are never joined here.
with peninsular_demand as (
    select
        ts_utc as hour_utc,
        demand_mw as peninsular_demand_mw
    from {{ ref('demand_system_hourly') }}
    where system_scope = 'peninsular'
),

peninsular_gen_daily as (
    select
        generation_date,
        max(reported_total_mwh) as total_mwh,
        sum(case when is_renewable then generation_mwh else 0.0 end) as renewable_mwh,
        sum(
            case
                when technology_slug in ('solar_pv', 'thermal_solar') then generation_mwh
                else 0.0
            end
        ) as solar_mwh,
        sum(case when technology_slug = 'wind' then generation_mwh else 0.0 end) as wind_mwh
    from {{ ref('generation_daily') }}
    where electric_system = 'peninsular'
    group by generation_date
),

joined as (
    select
        p.hour_utc,
        p.delivery_date,
        p.madrid_hour,
        case
            when extract(isodow from p.delivery_date) in (6, 7) then 'weekend'
            else 'weekday'
        end as day_type,
        p.price_es_eur_mwh,
        p.price_pt_eur_mwh,
        p.spread_es_minus_pt_eur_mwh,
        p.is_decoupled,
        p.is_zero_or_negative_es,
        p.pvpc_eur_mwh,
        d.peninsular_demand_mw,
        case
            when g.total_mwh > 0 then round(100.0 * g.renewable_mwh / g.total_mwh, 4)
        end as peninsular_renewable_share_pct,
        case
            when g.total_mwh > 0 then round(100.0 * g.solar_mwh / g.total_mwh, 4)
        end as peninsular_solar_share_pct,
        case
            when g.total_mwh > 0 then round(100.0 * g.wind_mwh / g.total_mwh, 4)
        end as peninsular_wind_share_pct
    from {{ ref('mibel_prices_hourly') }} p
    left join peninsular_demand d
        on p.hour_utc = d.hour_utc
    left join peninsular_gen_daily g
        on p.delivery_date = g.generation_date
)

select
    hour_utc,
    delivery_date,
    madrid_hour,
    day_type,
    price_es_eur_mwh,
    price_pt_eur_mwh,
    spread_es_minus_pt_eur_mwh,
    is_decoupled,
    is_zero_or_negative_es,
    pvpc_eur_mwh,
    peninsular_demand_mw,
    case
        when hour_utc - lag(hour_utc) over (order by hour_utc) = interval '1 hour'
            then round(peninsular_demand_mw - lag(peninsular_demand_mw) over (order by hour_utc), 4)
    end as demand_ramp_1h_mw,
    case
        when hour_utc - lag(hour_utc) over (order by hour_utc) = interval '1 hour'
            then round(price_es_eur_mwh - lag(price_es_eur_mwh) over (order by hour_utc), 6)
    end as price_ramp_1h_eur_mwh,
    cast(
        row_number() over (
            partition by delivery_date
            order by price_es_eur_mwh asc, hour_utc asc
        ) as integer
    ) as intraday_price_rank,
    (madrid_hour between 17 and 22) as is_evening_ramp_window,
    peninsular_renewable_share_pct,
    peninsular_solar_share_pct,
    peninsular_wind_share_pct,
    'Source: OMIE (omie.es) & Red Eléctrica, REData' as source_attribution,
    cast(current_timestamp as timestamptz) as updated_at_utc
from joined
order by hour_utc
