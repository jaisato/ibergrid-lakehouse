-- Every series_id delivered by REData generation_daily must exist in the generation_technologies
-- seed so new REE technologies never silently vanish from Silver shares.
select distinct
    r.series_id,
    r.series_type,
    r.series_title
from {{ ref('stg_redata_observations') }} r
left join {{ ref('generation_technologies') }} t
    on r.series_id = t.series_id
where r.dataset = 'generation_daily'
  and t.series_id is null
