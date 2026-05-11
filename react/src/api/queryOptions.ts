import { queryOptions } from '@tanstack/react-query';
import { apiFetch } from '~/api/apiFetch';
import { getDelayStatus, getWeather } from '~/utils/weather.functions';
import type { GetDelaysOptions } from '~/utils/weather.server';

// ----- Prediction queries -----

export interface PredictionSummary {
  n_flights_today: number;
  positive_rate_today: number | null;
  model_version: string | null;
  days_since_retrain: number | null;
  model_loaded_at: string;
  data_as_of: string | null;
}

export const todaysPredictionOptions = queryOptions({
  queryKey: ['predictions', 'today'],
  queryFn: () => apiFetch<PredictionSummary>('/api/predictions/today'),
  staleTime: 60 * 30 * 1000,
});

export interface PredictionRun {
  score_date: string;
  model_version: string;
  n_flights: number;
  positive_rate: number;
  avg_proba: number;
  n_with_actuals: number; // always 0 for today's predictions / last 30 days ??
}

export const predictionOptions = queryOptions({
  queryKey: ['predictions'],
  queryFn: () => apiFetch<{ rows: PredictionRun[] }>('/api/predictions'),
  staleTime: 60 * 30 * 1000,
});

// ----- Drift queries -----

export interface DriftSummary {
  psi_breaches: number;
  n_features: number;
}

export const driftSummaryOptions = queryOptions({
  queryKey: ['drift', 'summary'],
  queryFn: () => apiFetch<DriftSummary>('/api/drift/summary'),
  staleTime: 60 * 30 * 1000,
});

export interface DriftRow {
  report_date: string;
  feature_name: string;
  psi_score: number;
  kl_divergence: number | null;
  rank: number;
  is_breached: boolean;
}

export interface DriftResponse {
  rows: DriftRow[];
  report_date: string;
  n_breached: number;
}

export const driftMetricsOptions = (
  params?: URLSearchParams, // start: string, end: string
) =>
  queryOptions({
    queryKey: ['driftMetrics', params],
    queryFn: () =>
      apiFetch<DriftResponse>(
        // `/api/drift/metrics?start=${start || ''}&end=${end || ''}`,
        `/api/drift/metrics`,
        {},
        params,
      ),
    staleTime: 60 * 30 * 1000,
    gcTime: 1000 * 60 * 30,
  });

// ----- Route queries -----

export interface RouteHistoryResponse {
  route_key: string;
  history: number[];
  days: number;
  data_as_of: string | null;
}

export const routeHistoryOptions = (
  origin: string,
  dest: string,
  days: number = 14,
) =>
  queryOptions({
    queryKey: ['routes', 'history', { origin, dest, days }],
    queryFn: () =>
      apiFetch<RouteHistoryResponse>(
        `/api/routes/history`,
        {},
        { origin, dest, days: days.toString() },
      ),
    staleTime: 60 * 10 * 1000,
    gcTime: 1000 * 60 * 10,
  });

export interface NetworkDelayResponse {
  rows: {
    origin: string;
    otp: number;
    avg_delay_min: number;
    status_indicator: string;
  }[];
  data_as_of: string | null;
}

export const networkDelayOptions = (days: number = 7) =>
  queryOptions({
    queryKey: ['routes', 'network', days],
    queryFn: () => apiFetch<NetworkDelayResponse>(`/api/network?days=${days}`),
    staleTime: 60 * 10 * 1000,
    gcTime: 1000 * 60 * 10,
  });

export interface SampleFlight {
  flight_id: string;
  carrier: string;
  flight_number: string;
  origin: string;
  dest: string;
  scheduled_departure_utc: string;
  onTimeProb: number;
  tail_number: string;
}

export const sampleFlightOptions = (limit: number = 4) =>
  queryOptions({
    queryKey: ['flights', 'sample', limit],
    queryFn: () =>
      apiFetch<SampleFlight[]>(
        '/api/flights/sample',
        {},
        { limit: limit.toString() },
      ),
    staleTime: 1000 * 60 * 60 * 6,
    gcTime: 1000 * 60 * 60 * 6,
  });

// ----- Carrier queries -----

export interface CarrierComparisonResponse {
  days: number;
  carriers: { carrier: string; otp: number; avg_delay: number }[];
  data_as_of: string | null;
}

export const carrierComparisonOptions = (
  origin: string,
  dest: string,
  days: number = 14,
) =>
  queryOptions({
    queryKey: ['carriers', origin, dest, 'history', days],
    queryFn: () =>
      apiFetch<CarrierComparisonResponse>(
        `/api/carriers/comparison?origin=${origin}&dest=${dest}&days=${days}`,
      ),
    staleTime: 60 * 10 * 1000,
    gcTime: 1000 * 60 * 30,
  });

export interface CarrierRouteDay {
  score_date: string;
  avg_delay_proba: number;
  avg_actual_delay_min: number | null;
  n_flights: number;
}

export interface CarrierRouteHistoryResponse {
  route_key: string;
  carrier: string;
  rows: CarrierRouteDay[];
  data_as_of: string | null;
}

// TODO: move to queries file if keeping component

export const carrierRouteHistoryOptions = (
  origin: string,
  dest: string,
  carrier: string,
  days = 30,
) =>
  queryOptions({
    queryKey: ['routes', 'carrier-history', { origin, dest, carrier, days }],
    queryFn: () =>
      apiFetch<CarrierRouteHistoryResponse>(
        `/api/routes/carrier-history`,
        {},
        { origin, dest, carrier, days: days.toString() },
      ),
    staleTime: 60 * 10 * 1000,
    gcTime: 1000 * 60 * 30,
  });

// ----- Model queries -----

export interface ModelInfo {
  model_name: string;
  model_version: string;
  champion_alias: string;
  loaded_at: string;
  registered_at: string;
  training_roc_auc: number;
  feature_service: string;
  shadow_version: string | number;
}

export const modelInfoOptions = queryOptions({
  queryKey: ['modelInfo'],
  queryFn: () => apiFetch<ModelInfo>('/model-info'),
  staleTime: 60 * 10 * 1000,
  gcTime: 1000 * 60 * 30,
});

export interface ModelStats {
  model_version: string;
  last_scored: string;
  avg_roc_auc: number;
  avg_accuracy: number;
  avg_precision_score: number;
  avg_recall_score: number;
  avg_f1: number;
  avg_log_loss: number;
  avg_brier_score: number;
  avg_positive_rate: number;
  avg_actual_positive_rate: number;
  avg_n_flights_scored: number;
  total_n_flights: number;
}

export const modelStatsOptions = (champion: boolean = false) =>
  queryOptions({
    queryKey: ['models', { champion }],
    queryFn: () =>
      apiFetch<{ rows: ModelStats[] }>(`/api/model-stats?champion=${champion}`),
    staleTime: 60 * 10 * 1000,
    gcTime: 1000 * 60 * 30,
  });

// ----- Accuracy queries -----

export interface AccuracyPoint {
  score_date: string;
  model_version: string;
  roc_auc: number;
  f1: number;
  precision_score: number;
  recall_score: number;
  positive_rate: number;
  actual_positive_rate: number;
  n_with_actuals: number;
}

export const accuracyOptions = queryOptions({
  queryKey: ['accuracy'],
  queryFn: () => apiFetch<{ rows: AccuracyPoint[] }>('/api/accuracy'),
  staleTime: 60 * 30 * 1000,
  gcTime: 1000 * 60 * 30,
});

// ----- Weather queries -----

export const aviationWeatherOptions = (
  endpoint: 'metar' | 'taf',
  icao: string,
) =>
  queryOptions({
    queryKey: ['weather', endpoint, icao],
    queryFn: async () => getWeather({ data: { endpoint, icao } }),
    staleTime: 1000 * 60 * 15,
    gcTime: 1000 * 60 * 30,
  });

// ----- Airlabs queries -----

export const delayOptions = (options: GetDelaysOptions) =>
  queryOptions({
    queryKey: ['delays', options],
    queryFn: () => getDelayStatus({ data: options }),
    staleTime: 1000 * 60 * 15,
    gcTime: 1000 * 60 * 30,
  });
