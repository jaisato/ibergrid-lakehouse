select
    delivery_date,
    period,
    resolution_minutes,
    interval_start_utc,
    interval_end_utc,
    price_es_eur_mwh,
    price_pt_eur_mwh,
    contract_version,
    source_file,
    file_version,
    content_sha256,
    fetched_at,
    run_id,
    landing_path
from {{ source('bronze', 'omie_day_ahead_prices') }}
