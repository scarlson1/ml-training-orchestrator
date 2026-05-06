// Server-only helpers (DB queries, internal logic)

import z from 'zod';

export interface MetarResponse {
  altim: number;
  clouds: Record<string, any>[];
  cover: string;
  dewp: number;
  elev: number;
  fltCat: string;
  icaoId: string;
  lat: number;
  lon: number;
  metarType: string;
  name: string;
  obsTime: number;
  qcField: number;
  rawOb: string;
  receiptTime: string;
  reportTime: string;
  slp: number;
  temp: number;
  visib: string;
  wdir: number;
  wgst: number;
  wspd: number;
}

interface TafResponse {
  bulletinTime: string;
  dbPopTime: string;
  elev: number;
  fcsts: {
    altim: null;
    clouds: { cover: string; base: number; type: null | string }[];
    icgTurb: any[];
    notDecoded: null | any;
    temp: any[];
    vertVis: null | any;
    visib: string;
    wdir: number | null;
    wgst: number | null;
    wshearDir: null | number;
    wshearHgt: null | number;
    wshearSpd: number;
    wspd: number;
    wxString: string | null;
    timeFrom: number;
    timeTo: number;
    timeBec: null | any;
    fcstChange: string | null;
    probability: null | number;
  }[];
  icaoId: string;
  issueTime: string;
  lat: number;
  lon: number;
  mostRecent: number;
  name: string;
  prior: number;
  rawTAF: string;
  remarks: string;
  validTimeFrom: string;
  validTimeTo: string;
}

interface AviationWeatherMap {
  metar: MetarResponse;
  taf: TafResponse;
}

export const getWeatherData = async <T extends keyof AviationWeatherMap>(
  endpoint: T, // 'metar' | 'taf',
  icao: string,
): Promise<AviationWeatherMap[T] | undefined> => {
  try {
    const url = new URL(`https://aviationweather.gov/api/data/${endpoint}`);
    url.search = new URLSearchParams({
      ids: icao,
      format: 'json',
    }).toString();
    let res = await fetch(url);
    if (!res.ok) throw new Error(`Response status: ${res.status}`);

    const data = (await res.json()) as Array<AviationWeatherMap[T]>;

    return (data.length ? data[0] : {}) as AviationWeatherMap[T];
  } catch (err) {
    console.log('getWeather Error: ', err);
  }
};

export const getDelaysOptions = z.object({
  // api_key: z.string(),
  delay: z.int().transform((d) => d.toString()),
  type: z.enum(['departures', 'arrivals']),
  dep_iata: z.string().length(3).optional(),
  dep_icao: z.string().min(4).max(5).optional(),
  arr_iata: z.string().length(3).optional(),
  arr_icao: z.string().min(4).max(5).optional(),
  airline_icao: z.string().optional(),
  airline_iata: z.string().optional(),
  flight_icao: z.string().optional(),
  flight_iata: z.string().optional(),
  flight_number: z.string().optional(),
  _fields: z.string().optional(),
  limit: z
    .int()
    .transform((v) => v.toString())
    .optional(),
  offset: z
    .int()
    .transform((v) => v.toString())
    .optional(),
});
export type GetDelaysOptions = z.input<typeof getDelaysOptions>;
export type GetDelaysOptionParsed = z.infer<typeof getDelaysOptions>;

interface GetDelaysResponse {
  aircraft_icao: string | null;
  airline_iata: string;
  airline_icao: string;
  arr_baggage: string;
  arr_delayed: number;
  arr_estimated: string;
  arr_estimated_ts: number;
  arr_estimated_utc: string;
  arr_gate: string;
  arr_iata: string;
  arr_icao: string;
  arr_terminal: string;
  arr_time: string;
  arr_time_ts: number;
  arr_time_utc: string;
  cs_airline_iata: string | null;
  cs_flight_iata: string | null;
  cs_flight_number: string | null;
  delayed: number;
  dep_actual: string;
  dep_actual_ts: number;
  dep_actual_utc: string;
  dep_delayed: number;
  dep_estimated: string;
  dep_estimated_ts: number;
  dep_estimated_utc: string;
  dep_gate: string;
  dep_iata: string;
  dep_icao: string;
  dep_terminal: string;
  dep_time: string;
  dep_time_ts: number;
  dep_time_utc: string;
  duration: number;
  flight_iata: string;
  flight_icao: string;
  flight_number: string;
  status: string;
}

export const getDelays = async (
  data: GetDelaysOptionParsed & { api_key: string },
) => {
  const url = new URL('https://airlabs.co/api/v9/delays');
  url.search = new URLSearchParams(data).toString();

  let res = await fetch(url);
  if (!res.ok) throw new Error(`Response status: ${res.status}`);

  const d = (await res.json()) as {
    response: GetDelaysResponse[];
    request: Object;
    terms: string;
  };

  return d.response;
};
