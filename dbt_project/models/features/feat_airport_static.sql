-- DELETE - silly claude idea. Join directly in marts from seed

-- select

--     f.flight_id,
--     f.origin,
--     f.scheduled_departure_utc                  as event_ts,
--     coalesce(h.hub_size, 'small_regional')      as origin_hub_size

-- from {{ ref('stg_flights') }}
-- left join {{ ref('hub_airports') }} h
-- on h.iata_code = f.origin