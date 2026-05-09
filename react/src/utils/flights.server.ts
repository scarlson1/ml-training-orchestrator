import z from 'zod';

export const flightScheduleOptions = z.object({
  origin: z.string().length(3),
  dest: z.string().length(3),
});

export interface ScheduledFlight {
  airline_iata: string;
  airline_icao: string;
  flight_iata: string;
  flight_icao: string;
  flight_number: string;
  dep_iata: string;
  dep_icao: string;
  dep_terminal: null | string;
  dep_gate: string;
  dep_time: string;
  dep_time_utc: string;
  dep_estimated: string;
  dep_estimated_utc: string;
  dep_actual: string;
  dep_actual_utc: string;
  arr_iata: string;
  arr_icao: string;
  arr_terminal: string;
  arr_gate: string;
  arr_baggage: string;
  arr_time: string;
  arr_time_utc: string;
  arr_estimated: string;
  arr_estimated_utc: string;
  cs_airline_iata: string;
  cs_flight_number: string;
  cs_flight_iata: string;
  status: string;
  duration: number;
  delayed: number;
  dep_delayed: number;
  arr_delayed: null | number | boolean;
  aircraft_icao: null;
  arr_time_ts: number;
  dep_time_ts: number;
  arr_estimated_ts: number;
  dep_estimated_ts: number;
  dep_actual_ts: number;
}

export const getFlightSchedule = async ({
  origin,
  dest,
  api_key,
}: {
  origin: string;
  dest: string;
  api_key: string;
}) => {
  const url = new URL('https://airlabs.co/api/v9/schedules');

  url.search = new URLSearchParams({
    api_key,
    dep_iata: origin,
    arr_iata: dest,
  }).toString();

  let res = await fetch(url);
  if (!res.ok) throw new Error(`Response status: ${res.status}`);

  const d = (await res.json()) as {
    response: ScheduledFlight[];
  };

  return d.response;
};

export interface FlightInfo {
  hex: string; //	ICAO24 Hex address.
  reg_number: string; //	Aircraft Registration Number
  aircraft_icao: string; //	Aircraft ICAO type. Available in the Free plan.
  flag: string; //	ISO 2 country code from Countries DB. Available in the Free plan.
  lat: number; //	Aircraft Geo-Latitude for now. Available in the Free plan.
  lng: number; //	Aircraft Geo-Longitude for now. Available in the Free plan.
  alt: number; //	Aircraft elevation for now (meters).
  dir: number; //	Aircraft head direction for now. Available in the Free plan.
  speed: number; //	Aircraft horizontal speed (km) for now.
  v_speed: number; //	Aircraft vertical speed (km) for now.
  squawk: string; //	Aircraft squawk signal code.
  airline_iata: string; //	Airline IATA code. Available in the Free plan.
  airline_icao: string; //	Airline ICAO code.
  flight_iata: string; //	Flight IATA code-number. Available in the Free plan.
  flight_icao: string; //	Flight ICAO code-number.
  flight_number: string; //	Flight number only. Available in the Free plan.
  cs_airline_iata: string; //	Codeshared airline IATA code.
  cs_flight_iata: string; //	Codeshared flight IATA code-number.
  cs_flight_number: string; //	Codeshared flight number.
  dep_iata: string; //	Departure airport IATA code. Available in the Free plan.
  dep_icao: string; //	Departure airport ICAO code.
  dep_terminal: string; //	Estimated departure terminal.
  dep_gate: string; //	Estimated departure gate.
  dep_time: string; //	Departure time in the airport time zone. Available in the Free plan.
  dep_time_ts: number; //	Departure UNIX timestamp.
  dep_time_utc: string; //	Departure time in UTC time zone.
  dep_estimated: string; //	Updated departure time in the airport time zone.
  dep_estimated_ts: number; //	Updated departure UNIX timestamp.
  dep_estimated_utc: string; //	Updated departure time in UTC time zone.
  arr_iata: string; //	Arrival airport IATA code. Available in the Free plan.
  arr_icao: string; //	Arrival airport ICAO code.
  arr_terminal: string; //	Estimated arrival terminal.
  arr_gate: string; //	Estimated arrival gate.
  arr_baggage: string; //	Arrival baggage claim carousel number.
  arr_time: string; //	Arrival time in the airport time zone. Available in the Free plan.
  arr_time_ts: number; //	Arrival UNIX timestamp.
  arr_time_utc: string; //	Arrival time in UTC time zone.
  arr_estimated: string; //	Updated arrival time in the airport time zone.
  arr_estimated_ts: string; //	Updated arrival UNIX timestamp.
  arr_estimated_utc: string; //	Updated arrival time in UTC time zone.
  duration: number; //	Estimated flight time (in minutes).
  delayed: string; //	(deprecated) Estimated flight delay time (in minutes).
  dep_delayed: string; //	Estimated time of flight departure delay (in minutes).
  arr_delayed: string; //	Estimated time of flight arrival delay (in minutes).
  updated: number; //	UNIX timestamp of last aircraft signal.
  status: string; //	Current flight status - scheduled, en-route, landed.
  model: string; //	Aircraft full model name.
  manufacturer: string; //	Aircraft manufacturer name. Available in the Free plan.
  msn: string; //	Manufacturer serial number.
  type: string; //	Aircraft type - landplane, seaplane, tiltrotor, helicopter, gyrocopter, amphibian.
  engine: string; //	Aircraft engine type - jet, piston, turboprop/turboshaft, electric.
  engine_count: string; //	Aircraft engine number - 1, 2, 3, 4, 6, 8
  built: string; //	Aircraft built year
  age: string; //	Aircraft age (years)
}

export const flightInfoOptions = z.object({
  flight_iata: z.string(),
  // api_key,
});

export const getAirLabFlightInfo = async ({
  flight_iata,
  api_key,
}: {
  flight_iata: string; // Carrier abrv + flight number
  api_key: string;
}) => {
  const url = new URL('https://airlabs.co/api/v9/flight');

  url.search = new URLSearchParams({
    api_key,
    flight_iata,
  }).toString();

  let res = await fetch(url);
  if (!res.ok) throw new Error(`Response status: ${res.status}`);

  const d = (await res.json()) as {
    response: FlightInfo;
  };

  return d.response;
};

interface LabFlight {
  hex: string; //	ICAO24 Hex address.
  reg_number: string; //	Aircraft Registration Number
  flag: string; //	ISO 2 country code from Countries DB. Available in the Free plan.
  lat: number; //	Aircraft Geo-Latitude for now. Available in the Free plan.
  lng: number; //	Aircraft Geo-Longitude for now. Available in the Free plan.
  alt: number; //	Aircraft elevation for now (meters).
  dir: number; //	Aircraft head direction for now. Available in the Free plan.
  speed: number; //	Aircraft horizontal speed (km) for now.
  v_speed: number; //	Aircraft vertical speed (km) for now.
  squawk: string; //	Aircraft squawk signal code.
  airline_icao: string; //	Airline ICAO code.
  airline_iata: string; //	Airline IATA code.
  aircraft_icao: string; //	Aircraft ICAO type. Available in the Free plan.
  flight_icao: string; //	Flight ICAO code-number.
  flight_iata: string; //	Flight IATA code-number.
  flight_number: string; //	Flight number only.
  dep_icao: string; //	Departure Airport ICAO code.
  dep_iata: string; //	Departure Airport IATA code. Available in the Free plan.
  arr_icao: string; //	Arrival Airport ICAO code.
  arr_iata: string; //	Arrival Airport IATA code.
  updated: number; //	UNIX timestamp of last aircraft signal.
  status: string; //	Current flight status - scheduled, en-route, landed.
}

export const getAirLabFlights = async ({
  origin,
  dest,
  api_key,
}: {
  origin: string;
  dest: string;
  api_key: string;
}) => {
  const url = new URL('https://airlabs.co/api/v9/flight');

  url.search = new URLSearchParams({
    api_key,
    dep_iata: origin,
    arr_iata: dest,
  }).toString();

  let res = await fetch(url);
  if (!res.ok) throw new Error(`Response status: ${res.status}`);

  const d = (await res.json()) as {
    response: LabFlight[];
  };

  return d.response;
};
