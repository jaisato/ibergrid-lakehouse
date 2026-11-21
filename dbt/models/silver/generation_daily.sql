-- Daily net generation per electric system and technology (ADR 0003, ADR 0007).
-- Excludes Total generation (series_id = '10296') from technology rows while carrying
-- reported_total_mwh on each row so downstream sums never double-count.
with raw_gen as (
    select
        madrid_date as generation_date,
        electric_system,
        series_id,
        value as generation_mwh,
        series_last_update
    from {{ ref('stg_redata_observations') }}
    where dataset = 'generation_daily'
),

totals as (
    select
        generation_date,
        electric_system,
        generation_mwh as reported_total_mwh
    from raw_gen
    where series_id = '10296'
),

tech_rows as (
    select
        g.generation_date,
        g.electric_system,
        case
            when g.electric_system = 'national' then 'national'
            when g.electric_system = 'peninsular' then 'peninsular'
            else 'non_peninsular'
        end as system_scope,
        g.series_id,
        t.technology_slug,
        t.technology_name,
        t.energy_class,
        t.is_renewable,
        g.generation_mwh,
        tot.reported_total_mwh,
        case
            when tot.reported_total_mwh > 0
                then round(100.0 * g.generation_mwh / tot.reported_total_mwh, 6)
        end as share_pct,
        g.series_last_update as series_last_update_utc
    from raw_gen g
    inner join {{ ref('generation_technologies') }} t
        on g.series_id = t.series_id
    inner join totals tot
        on g.generation_date = tot.generation_date
       and g.electric_system = tot.electric_system
    where t.energy_class <> 'total'
)

select
    generation_date,
    electric_system,
    system_scope,
    series_id,
    technology_slug,
    technology_name,
    energy_class,
    is_renewable,
    generation_mwh,
    reported_total_mwh,
    share_pct,
    series_last_update_utc
from tech_rows
order by generation_date, electric_system, series_id
