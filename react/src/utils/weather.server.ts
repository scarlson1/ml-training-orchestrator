// Server-only helpers (DB queries, internal logic)

import { createServerFn } from '@tanstack/react-start';
import { getWeatherData } from '~/utils/weather.functions';

export const getWeather = createServerFn({ method: 'GET' })
  .inputValidator((data: { endpoint: 'metar' | 'taf'; icao: string }) => data)
  .handler(async ({ data }) => {
    return getWeatherData(data.endpoint, data.icao);
  });
