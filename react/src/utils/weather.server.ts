// Server-only helpers (DB queries, internal logic)

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
  return await res.json();
};
