-- Cross-source check (ADR 0004): wherever both OMIE Spain day-ahead price and REData peninsular
-- spot price exist for the same UTC hour, they must agree within 0.02 EUR/MWh.
select
    hour_utc,
    delivery_date,
    price_es_eur_mwh,
    redata_spot_eur_mwh,
    omie_redata_diff_eur_mwh
from {{ ref('mibel_prices_hourly') }}
where omie_redata_diff_eur_mwh is not null
  and abs(omie_redata_diff_eur_mwh) > 0.02
