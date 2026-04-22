-- passes if zero rows are returned
-- check no weather observation timestamp is in the future relative to flights scheduled departure

select

    count(*)                                                as pit_violations,
    min(origin_obs_time_utc - scheduled_departure_utc)      as max_future_gap

from {{ ref('int_flights_enriched') }}
where origin_obs_time_utc > scheduled_departure_utc