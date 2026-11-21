-- Strict peninsular vs non-peninsular isolation and Canarias timestamp verification flag:
-- 1. system_scope must match electric_system exactly.
-- 2. canarias must always carry ts_utc_verified = false; other systems true.
-- 3. gold.mibel_daily_summary.peninsular_demand_mwh must equal peninsular-only system demand,
--    never blending Baleares, Canarias, Ceuta or Melilla.
with bad_scope as (
    select
        'bad_scope' as violation,
        electric_system,
        cast(madrid_date as varchar) as detail
    from {{ ref('demand_system_hourly') }}
    where (electric_system = 'peninsular' and system_scope <> 'peninsular')
       or (electric_system <> 'peninsular' and system_scope <> 'non_peninsular')
       or (electric_system = 'canarias' and ts_utc_verified)
       or (electric_system <> 'canarias' and not ts_utc_verified)
),

bad_daily_mix as (
    select
        'peninsular_demand_mismatch' as violation,
        'peninsular' as electric_system,
        cast(m.delivery_date as varchar) as detail
    from {{ ref('mibel_daily_summary') }} m
    inner join {{ ref('system_demand_daily') }} s
        on m.delivery_date = s.demand_date
       and s.electric_system = 'peninsular'
    where m.peninsular_demand_mwh is null
       or abs(m.peninsular_demand_mwh - s.demand_total_mwh) > 0.01
)

select * from bad_scope
union all
select * from bad_daily_mix
