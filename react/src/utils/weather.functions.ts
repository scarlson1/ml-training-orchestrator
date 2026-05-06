// Server function wrappers (createServerFn)

import { createServerFn } from '@tanstack/react-start';
import { serverEnv } from '~/config/env';
import {
  getDelays,
  getDelaysOptions,
  getWeatherData,
} from '~/utils/weather.server';

export const getWeather = createServerFn({ method: 'GET' })
  .inputValidator((data: { endpoint: 'metar' | 'taf'; icao: string }) => data)
  .handler(async ({ data }) => {
    return getWeatherData(data.endpoint, data.icao);
  });

// GET request (default)
export const getDelayStatus = createServerFn()
  .inputValidator(getDelaysOptions)
  .handler(async ({ data }) => {
    const api_key = serverEnv.AIRLABS_KEY;
    return getDelays({ ...data, api_key });
  });
