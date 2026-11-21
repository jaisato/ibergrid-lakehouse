-- Daily demand and renewable mix per electric system (peninsular + four non-peninsular systems),
-- preserving explicit system_scope and Canarias ts_utc_verified flags (ADR 0003, ADR 0007).
with daily_demand as (
    select
        madrid_date as demand_date,
        electric_system,
        system_scope,
        ts_utc_verified,
        cast(count(*) as integer) as hours_in_day,
        round(sum(demand_mw), 4) as demand_total_mwh,
        round(avg(demand_mw), 4) as demand_mean_mw,
        min(demand_mw) as demand_min_mw,
        max(demand_mw) as demand_peak_mw
    from {{ ref('demand_system_hourly') }}
    group by 1, 2, 3, 4
),

daily_gen as (
    select
        generation_date as demand_date,
        electric_system,
        max(reported_total_mwh) as generation_total_mwh,
        round(sum(case when is_renewable then generation_mwh else 0.0 end), 4) as renewable_generation_mwh,
        sum(
            case
                when technology_slug in ('solar_pv', 'thermal_solar') then generation_mwh
                else 0.0
            end
        ) as solar_mwh,
        sum(case when technology_slug = 'wind' then generation_mwh else 0.0 end) as wind_mwh
    from {{ ref('generation_daily') }}
    where electric_system <> 'national'
    group by 1, 2
)

select
    d.demand_date,
    d.electric_system,
    d.system_scope,
    d.ts_utc_verified,
    d.hours_in_day,
    d.demand_total_mwh,
    d.demand_mean_mw,
    d.demand_min_mw,
    d.demand_peak_mw,
    g.generation_total_mwh,
    g.renewable_generation_mwh,
    case
        when g.generation_total_mwh > 0
            then round(100.0 * g.renewable_generation_mwh / g.generation_total_mwh, 4)
    end as renewable_share_pct,
    case
        when g.generation_total_mwh > 0
            then round(100.0 * g.solar_mwh / g.generation_total_mwh, 4)
    end as solar_share_pct,
    case
        when g.generation_total_mwh > 0
            then round(100.0 * g.wind_mwh / g.generation_total_mwh, 4)
    end as wind_share_pct,
    'Source: Red Eléctrica, REData' as source_attribution,
    cast(current_timestamp as timestamptz) as updated_at_utc
from daily_demand d
left join daily_gen g
    on d.demand_date = g.demand_date
   and d.electric_system = g.electric_system
order by d.demand_date, d.electric_system
