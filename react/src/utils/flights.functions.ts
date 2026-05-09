import { createServerFn } from '@tanstack/react-start';
import { serverEnv } from '~/config/env';
import {
  flightInfoOptions,
  flightScheduleOptions,
  getAirLabFlightInfo,
  getAirLabFlights,
  getFlightSchedule,
} from '~/utils/flights.server';

export const getScheduledFlights = createServerFn()
  .inputValidator(flightScheduleOptions)
  .handler(async ({ data }) => {
    const api_key = serverEnv.AIRLABS_KEY;
    return getFlightSchedule({ ...data, api_key });
  });

export const getFlightInfo = createServerFn()
  .inputValidator(flightInfoOptions)
  .handler(async ({ data }) => {
    const api_key = serverEnv.AIRLABS_KEY;
    return getAirLabFlightInfo({ ...data, api_key });
  });

export const getFlights = createServerFn()
  .inputValidator(flightScheduleOptions)
  .handler(async ({ data }) => {
    const api_key = serverEnv.AIRLABS_KEY;
    return getAirLabFlights({ ...data, api_key });
  });
