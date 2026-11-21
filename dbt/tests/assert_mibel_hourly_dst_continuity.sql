-- Every Madrid delivery day in silver.mibel_prices_hourly must have the exact DST-aware hour
-- count (23, 24 or 25) and contiguous 1-hour UTC intervals from Madrid midnight to next midnight.
with per_day as (
    select
        delivery_date,
        count(*) as actual_hours,
        cast(
            extract(
                epoch from (
                    (cast(delivery_date + interval 1 day as timestamp) at time zone 'Europe/Madrid')
                    - (cast(delivery_date as timestamp) at time zone 'Europe/Madrid')
                )
            ) / 3600
            as integer
        ) as expected_hours,
        min(hour_utc) as first_hour_utc,
        max(hour_utc) as last_hour_utc,
        (cast(delivery_date as timestamp) at time zone 'Europe/Madrid') as expected_first_utc,
        (
            (cast(delivery_date + interval 1 day as timestamp) at time zone 'Europe/Madrid')
            - interval '1 hour'
        ) as expected_last_utc
    from {{ ref('mibel_prices_hourly') }}
    group by delivery_date
)

select *
from per_day
where actual_hours <> expected_hours
   or first_hour_utc <> expected_first_utc
   or last_hour_utc <> expected_last_utc
