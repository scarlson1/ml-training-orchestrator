import { queryOptions } from '@tanstack/react-query';
import { apiFetch } from '~/api/apiFetch';

// ----- Prediction queries -----

export interface PredictionSummary {
  n_flights_today: number;
  positive_rate_today: number | null;
  model_version: string | null;
  days_since_retrain: number | null;
  model_loaded_at: string;
}

export const todaysPredictionOptions = queryOptions({
  queryKey: ['predictions', 'today'],
  queryFn: () => apiFetch<PredictionSummary>('/api/predictions/today'),
  // .then(
  //   (r) => r.json() as Promise<PredictionSummary>,
  // ),
  staleTime: 60 * 60 * 1000,
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
  staleTime: 60 * 60 * 1000,
});

// ----- Drift queries -----

export interface DriftSummary {
  psi_breaches: number;
  n_features: number;
}

export const driftSummaryOptions = queryOptions({
  queryKey: ['drift', 'summary'],
  queryFn: () => apiFetch<DriftSummary>('/api/drift/summary'),
  staleTime: 60 * 60 * 1000,
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
    staleTime: 60 * 60 * 1000,
  });

// ----- Route queries -----

export interface RouteHistoryResponse {
  route_key: string;
  history: number[];
  days: number;
}

export const routeHistoryOptions = (
  origin: string,
  dest: string,
  days: number = 14,
) =>
  queryOptions({
    queryKey: ['routes', origin, dest, 'history', days],
    queryFn: () =>
      apiFetch<RouteHistoryResponse>(
        `/api/routes/${origin}-${dest}/history?days=${days}`,
      ),
  });

export interface NetworkDelayResponse {
  rows: {
    origin: string;
    otp: number;
    avg_delay_min: number;
    status_indicator: string;
  }[];
}

export const networkDelayOptions = (days: number = 7) =>
  queryOptions({
    queryKey: ['routes', 'network', days],
    queryFn: () => apiFetch<NetworkDelayResponse>(`/api/network?days=${days}`),
  });

// ----- Carrier queries -----

export interface CarrierComparisonResponse {
  days: number;
  carriers: { carrier: string; otp: number; avg_delay: number }[];
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
  // .then(async (r) => {
  //   let res = (await r.json()) as ModelInfo;
  //   if (!r.ok) {
  //     console.log(r.statusText);
  //     throw new Error(`Failed to load model info.`);
  //   }
  //   return res;
  // }),
  staleTime: 60 * 60 * 1000,
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
    // .then(
    //   (r) => r.json() as Promise<{ rows: ModelStats[] }>,
    // ),
    staleTime: 60 * 60 * 1000,
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
  // .then(
  //   (r) => r.json() as Promise<{ rows: AccuracyPoint[] }>,
  // ),
  staleTime: 60 * 60 * 1000,
});
