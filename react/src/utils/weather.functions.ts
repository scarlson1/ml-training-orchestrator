// Server function wrappers (createServerFn)

import { createServerFn } from '@tanstack/react-start';
import { getWeatherData } from '~/utils/weather.server';

export const getWeather = createServerFn({ method: 'GET' })
  .inputValidator((data: { endpoint: 'metar' | 'taf'; icao: string }) => data)
  .handler(async ({ data }) => {
    return getWeatherData(data.endpoint, data.icao);
  });
