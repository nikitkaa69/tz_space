/* Текущий список людей (уникально по craft, name).
Если вдруг в space.people осталось несколько версий до мерджа, выбираем последнюю по _inserted_at. */

with ranked as (
    select
        craft,
        name,
        _inserted_at,
        row_number() over (partition by craft, name order by _inserted_at desc) as rn
    from {{ source('space_src', 'people') }}
)
select
    craft,
    name,
    _inserted_at
from ranked
where rn = 1
