-- OMIE day-ahead MIBEL prices at native delivery resolution:
-- v1 (60 min) through 2025-09-30 and v2 (15 min) from 2025-10-01 (ADR 0004).
select
    delivery_date,
    cast(period as integer) as period,
    cast(resolution_minutes as integer) as resolution_minutes,
    interval_start_utc,
    interval_end_utc,
    price_es_eur_mwh,
    price_pt_eur_mwh,
    round(price_es_eur_mwh - price_pt_eur_mwh, 6) as spread_es_minus_pt_eur_mwh,
    abs(price_es_eur_mwh - price_pt_eur_mwh) > 1e-6 as is_decoupled,
    price_es_eur_mwh <= 0.0 as is_zero_or_negative_es,
    price_pt_eur_mwh <= 0.0 as is_zero_or_negative_pt,
    cast(contract_version as integer) as contract_version,
    cast(file_version as integer) as file_version,
    source_file
from {{ ref('stg_omie_day_ahead_prices') }}
order by delivery_date, period
