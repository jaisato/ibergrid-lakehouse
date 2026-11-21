-- Technology rows in silver.generation_daily must reconcile to reported_total_mwh (series 10296)
-- for every (generation_date, electric_system), proving Total generation is never double-counted.
select
    generation_date,
    electric_system,
    max(reported_total_mwh) as reported_total_mwh,
    sum(generation_mwh) as sum_technologies_mwh,
    abs(max(reported_total_mwh) - sum(generation_mwh)) as abs_diff_mwh
from {{ ref('generation_daily') }}
group by generation_date, electric_system
having abs(max(reported_total_mwh) - sum(generation_mwh)) > 0.05
