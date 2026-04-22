-- Zero-join calendar features computed purely from scheduled_departure_utc and flight_date. The holidays CTE could alternatively be a seed — use a seed if you need to update holidays regularly or share them with other models.

-- BTS day_of_week convention: 1=Monday, 7=Sunday (different from ISO or Unix conventions). is_weekend checks for 6 (Saturday) or 7 (Sunday).

-- TODO: move to seed ??

with holidays as (
    -- US major travel holidays 2018-2025.
    -- Extend by adding rows — or migrate to a seed if this list grows.
    -- New Year's Day
    select date '2018-01-01' as d union all select date '2019-01-01' union all
    select date '2020-01-01' union all select date '2021-01-01' union all
    select date '2022-01-01' union all select date '2023-01-01' union all
    select date '2024-01-01' union all select date '2025-01-01' union all
    select date '2026-01-01' union all
    -- Independence Day
    select date '2018-07-04' union all select date '2019-07-04' union all
    select date '2020-07-04' union all select date '2021-07-04' union all
    select date '2022-07-04' union all select date '2023-07-04' union all
    select date '2024-07-04' union all select date '2025-07-04' union all
    -- select date '2026-07-04' union all
    -- Christmas
    select date '2018-12-25' union all select date '2019-12-25' union all
    select date '2020-12-25' union all select date '2021-12-25' union all
    select date '2022-12-25' union all select date '2023-12-25' union all
    select date '2024-12-25' union all select date '2025-12-25' union all
    -- Thanksgiving (4th Thursday of November)
    select date '2018-11-22' union all select date '2019-11-28' union all
    select date '2020-11-26' union all select date '2021-11-25' union all
    select date '2022-11-24' union all select date '2023-11-23' union all
    select date '2024-11-28' union all select date '2025-11-27' union all
    -- Labor Day (1st Monday of September)
    select date '2018-09-03' union all select date '2019-09-02' union all
    select date '2020-09-07' union all select date '2021-09-06' union all
    select date '2022-09-05' union all select date '2023-09-04' union all
    select date '2024-09-02' union all select date '2025-09-01' union all
    -- Memorial Day (last Monday of May)
    select date '2018-05-28' union all select date '2019-05-27' union all
    select date '2020-05-25' union all select date '2021-05-31' union all
    select date '2022-05-30' union all select date '2023-05-29' union all
    select date '2024-05-27' union all select date '2025-05-26'
    -- union all select date '2026-05-25'
)

select
    f.flight_id,
    f.scheduled_departure_utc                                   as event_ts,

    extract(hour from f.scheduled_departure_utc)::int           as scheduled_hour_utc,
    f.day_of_week,          -- BTS convention: 1=Mon, 7=Sun
    f.month                 as month_of_year,
    ceil(f.month / 3.0)::int as quarter,

    -- BTS day_of_week: 6=Sat, 7=Sun
    f.day_of_week in (6, 7)                                     as is_weekend,

    h.d is not null                                             as is_holiday,

    -- Morning rush: 7-9 UTC (approx 3-5am ET / midnight-2am PT — adjust if needed)
    -- or Evening rush: 16-19 UTC (noon-3pm ET / 9am-noon PT).
    -- Better: use local hour from origin_tz, but that requires a join.
    -- UTC hour is a reasonable proxy for nationwide patterns.
    extract(hour from f.scheduled_departure_utc) between 12 and 14
        or extract(hour from f.scheduled_departure_utc) between 21 and 23  as is_rush_hour

from {{ ref('stg_flights') }} f
left join holidays h on h.d = f.flight_date
