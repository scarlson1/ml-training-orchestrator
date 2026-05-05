// Server-only helpers (DB queries, internal logic)

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

export const getWeatherData = async (
  endpoint: 'metar' | 'taf',
  icao: string,
) => {
  const url = new URL(`https://aviationweather.gov/api/data/${endpoint}`);
  url.search = new URLSearchParams({
    ids: icao,
    format: 'json',
  }).toString();
  let res = await fetch(url);
  return (await res.json()) as MetarResponse[];
};
