-- Calendar-month x day-type x Madrid-hour profile for industrial / data-center load shifting
-- (peninsular MIBEL market only, ADR 0007).
with grouped as (
    select
        cast(extract(month from delivery_date) as integer) as calendar_month,
        day_type,
        madrid_hour,
        cast(count(*) as integer) as sample_hours,
        round(avg(price_es_eur_mwh), 6) as price_es_mean_eur_mwh,
        round(quantile_cont(price_es_eur_mwh, 0.10), 6) as price_es_p10_eur_mwh,
        round(quantile_cont(price_es_eur_mwh, 0.50), 6) as price_es_p50_eur_mwh,
        round(quantile_cont(price_es_eur_mwh, 0.90), 6) as price_es_p90_eur_mwh,
        round(
            100.0 * avg(case when is_zero_or_negative_es then 1.0 else 0.0 end),
            4
        ) as zero_or_negative_es_pct,
        round(
            100.0 * avg(case when is_decoupled then 1.0 else 0.0 end),
            4
        ) as decoupled_pct,
        round(avg(peninsular_demand_mw), 4) as peninsular_demand_mean_mw,
        round(avg(peninsular_renewable_share_pct), 4) as peninsular_renewable_share_mean_pct
    from {{ ref('mibel_hourly_market') }}
    group by 1, 2, 3
),

ranked as (
    select
        *,
        cast(
            row_number() over (
                partition by calendar_month, day_type
                order by price_es_mean_eur_mwh asc, madrid_hour asc
            ) as integer
        ) as window_rank
    from grouped
)

select
    calendar_month,
    day_type,
    madrid_hour,
    sample_hours,
    price_es_mean_eur_mwh,
    price_es_p10_eur_mwh,
    price_es_p50_eur_mwh,
    price_es_p90_eur_mwh,
    zero_or_negative_es_pct,
    decoupled_pct,
    peninsular_demand_mean_mw,
    peninsular_renewable_share_mean_pct,
    window_rank,
    (window_rank <= 4) as is_optimal_4h_window,
    'Source: OMIE (omie.es) & Red Eléctrica, REData' as source_attribution,
    cast(current_timestamp as timestamptz) as updated_at_utc
from ranked
order by calendar_month, day_type, madrid_hour
