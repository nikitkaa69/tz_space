/* Количество уникальных людей по craft */
select
    craft,
    count(*) as people_count
from {{ ref('current_people') }}
group by craft
order by people_count desc
