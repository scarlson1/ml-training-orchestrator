from feast import Entity, ValueType

origin_airport = Entity(
    name='origin_airport',
    join_keys=['origin'],
    value_type=ValueType.STRING,
    description='IATA departure airport code (e.g. ORG, ATL)',
)

dest_airport = Entity(
    name='dest_airport',
    join_keys=['dest'],
    value_type=ValueType.STRING,
    description='IATA arrival airport code',
)

carrier = Entity(
    name='carrier',
    join_keys=['carrier'],
    value_type=ValueType.STRING,
    description='BTS two-letter carrier code (e.g. AA, UA, DL)',
)

route = Entity(
    name='route',
    join_keys=['route_key'],
    value_type=ValueType.STRING,
    description='Origin-destination pair: {origin}-{dest} (e.g. ORD-ATL)',
)

aircraft_tail = Entity(
    name='aircraft_tail',
    join_keys=['tail_number'],
    value_type=ValueType.STRING,
    description='FAA tail number identifying a specific aircraft',
)

# Why no flight entity?
#
# A flight entity with flight_id as the join key would force you to know the flight ID at serving time, before the flight has happened. You'll never know the BTS-assigned flight ID for a future flight. At serving time, you know: which airport is the flight departing from, which carrier, which aircraft (from the inbound assignment), which route. Design entities around what's knowable at prediction time.
