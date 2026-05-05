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
