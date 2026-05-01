import Box from '@mui/material/Box';
import Typography from '@mui/material/Typography';
import { useSuspenseQuery } from '@tanstack/react-query';
import { createFileRoute } from '@tanstack/react-router';
import { useMemo } from 'react';
import { apiFetch } from '~/api';
import { monoFont, serifFont } from '~/config/themePrimitives';

/*
 * Drift explorer: date range + two panels —
 *   Heatmap (features × dates, cell color = PSI severity)
 *   Feature detail — PSI + KL divergence time series for a selected feature
 *
 * query: GET /api/drift/metrics?start=&end= → DriftMetrics
 * shape: { features: string[], dates: string[], psi: number[][], kl: number[][] }
 *   psi[featureIdx][dateIdx], kl[featureIdx][dateIdx]
 */

export const Route = createFileRoute('/drift')({
  validateSearch: (search: Record<string, unknown>) => ({
    start: (search.start as string) ?? undefined,
    end: (search.end as string) ?? undefined,
  }),
  component: Drift,
});

interface DriftRow {
  report_date: string;
  feature_name: string;
  psi_score: number;
  kl_divergence: number | null;
  rank: number;
  is_breached: boolean;
}

interface DriftResponse {
  rows: DriftRow[];
  report_date: string;
  n_breached: number;
}

function psiSeverity(psi: number): 'green' | 'amber' | 'red' {
  if (psi >= 0.2) return 'red';
  if (psi >= 0.1) return 'amber';
  return 'green';
}

function Drift() {
  const { start, end } = Route.useSearch();

  const params = new URLSearchParams();
  if (start) params.set('start', start);
  if (end) params.set('end', end);

  // query: GET /api/drift/metrics?start=&end= → DriftMetrics
  const { data: tmp } = useSuspenseQuery({
    queryKey: ['driftMetrics', { start, end }],
    queryFn: () =>
      apiFetch(`/api/drift/metrics?${params}`).then(
        (r) => r.json() as Promise<DriftResponse>,
      ),
    staleTime: 60 * 60 * 1000,
  });

  // TODO: delete dummy data
  const data = tmp?.rows.length
    ? tmp
    : {
        report_date: '2026-04-28',
        n_breached: 2,
        rows: [
          {
            report_date: '2026-04-28',
            feature_name: 'departure_delay_minutes',
            psi_score: 0.312,
            kl_divergence: 0.187,
            rank: 1,
            is_breached: true,
            model_version: 'v2.4.1',
          },
          {
            report_date: '2026-04-28',
            feature_name: 'carrier_code',
            psi_score: 0.241,
            kl_divergence: 0.094,
            rank: 2,
            is_breached: true,
            model_version: 'v2.4.1',
          },
          {
            report_date: '2026-04-28',
            feature_name: 'origin_airport',
            psi_score: 0.083,
            kl_divergence: 0.041,
            rank: 3,
            is_breached: false,
            model_version: 'v2.4.1',
          },
          {
            report_date: '2026-04-28',
            feature_name: 'scheduled_elapsed_time',
            psi_score: 0.047,
            kl_divergence: null,
            rank: 4,
            is_breached: false,
            model_version: null,
          },
          {
            report_date: '2026-04-28',
            feature_name: 'distance_miles',
            psi_score: 0.021,
            kl_divergence: 0.009,
            rank: 5,
            is_breached: false,
            model_version: 'v2.4.1',
          },
          {
            report_date: '2026-04-27',
            feature_name: 'departure_delay_minutes',
            psi_score: 0.289,
            kl_divergence: 0.161,
            rank: 1,
            is_breached: true,
            model_version: 'v2.4.1',
          },
          {
            report_date: '2026-04-27',
            feature_name: 'carrier_code',
            psi_score: 0.198,
            kl_divergence: 0.077,
            rank: 2,
            is_breached: false,
            model_version: 'v2.4.1',
          },
          {
            report_date: '2026-04-27',
            feature_name: 'origin_airport',
            psi_score: 0.091,
            kl_divergence: 0.048,
            rank: 3,
            is_breached: false,
            model_version: 'v2.4.1',
          },
          {
            report_date: '2026-04-27',
            feature_name: 'scheduled_elapsed_time',
            psi_score: 0.055,
            kl_divergence: null,
            rank: 4,
            is_breached: false,
            model_version: null,
          },
          {
            report_date: '2026-04-27',
            feature_name: 'distance_miles',
            psi_score: 0.018,
            kl_divergence: 0.006,
            rank: 5,
            is_breached: false,
            model_version: 'v2.4.1',
          },
          {
            report_date: '2026-04-26',
            feature_name: 'departure_delay_minutes',
            psi_score: 0.267,
            kl_divergence: 0.142,
            rank: 1,
            is_breached: true,
            model_version: 'v2.4.1',
          },
          {
            report_date: '2026-04-26',
            feature_name: 'carrier_code',
            psi_score: 0.174,
            kl_divergence: 0.063,
            rank: 2,
            is_breached: false,
            model_version: 'v2.4.1',
          },
          {
            report_date: '2026-04-26',
            feature_name: 'origin_airport',
            psi_score: 0.102,
            kl_divergence: 0.052,
            rank: 3,
            is_breached: false,
            model_version: 'v2.4.1',
          },
          {
            report_date: '2026-04-26',
            feature_name: 'scheduled_elapsed_time',
            psi_score: 0.061,
            kl_divergence: 0.029,
            rank: 4,
            is_breached: false,
            model_version: 'v2.4.1',
          },
          {
            report_date: '2026-04-26',
            feature_name: 'distance_miles',
            psi_score: 0.024,
            kl_divergence: 0.011,
            rank: 5,
            is_breached: false,
            model_version: 'v2.4.1',
          },
          {
            report_date: '2026-04-25',
            feature_name: 'departure_delay_minutes',
            psi_score: 0.351,
            kl_divergence: 0.214,
            rank: 1,
            is_breached: true,
            model_version: 'v2.3.9',
          },
          {
            report_date: '2026-04-25',
            feature_name: 'carrier_code',
            psi_score: 0.228,
            kl_divergence: 0.088,
            rank: 2,
            is_breached: true,
            model_version: 'v2.3.9',
          },
          {
            report_date: '2026-04-25',
            feature_name: 'origin_airport',
            psi_score: 0.119,
            kl_divergence: 0.059,
            rank: 3,
            is_breached: false,
            model_version: 'v2.3.9',
          },
          {
            report_date: '2026-04-25',
            feature_name: 'scheduled_elapsed_time',
            psi_score: 0.073,
            kl_divergence: null,
            rank: 4,
            is_breached: false,
            model_version: null,
          },
          {
            report_date: '2026-04-25',
            feature_name: 'distance_miles',
            psi_score: 0.031,
            kl_divergence: 0.014,
            rank: 5,
            is_breached: false,
            model_version: 'v2.3.9',
          },
          {
            report_date: '2026-04-24',
            feature_name: 'departure_delay_minutes',
            psi_score: 0.144,
            kl_divergence: 0.071,
            rank: 1,
            is_breached: false,
            model_version: 'v2.3.9',
          },
          {
            report_date: '2026-04-24',
            feature_name: 'carrier_code',
            psi_score: 0.112,
            kl_divergence: 0.043,
            rank: 2,
            is_breached: false,
            model_version: 'v2.3.9',
          },
          {
            report_date: '2026-04-24',
            feature_name: 'origin_airport',
            psi_score: 0.089,
            kl_divergence: 0.037,
            rank: 3,
            is_breached: false,
            model_version: 'v2.3.9',
          },
          {
            report_date: '2026-04-24',
            feature_name: 'scheduled_elapsed_time',
            psi_score: 0.052,
            kl_divergence: 0.022,
            rank: 4,
            is_breached: false,
            model_version: 'v2.3.9',
          },
          {
            report_date: '2026-04-24',
            feature_name: 'distance_miles',
            psi_score: 0.016,
            kl_divergence: 0.007,
            rank: 5,
            is_breached: false,
            model_version: 'v2.3.9',
          },
        ],
      };

  const rows = useMemo(() => data?.rows || [], [data]);

  // pivot flat rows → features × dates matrix, preserving rank order
  const features = useMemo(() => {
    return [...new Map(rows.map((r) => [r.feature_name, r.rank])).entries()]
      .sort((a, b) => a[1] - b[1])
      .map(([f]) => f);
  }, [rows]);

  const dates = useMemo(
    () => [...new Set(rows.map((r) => r.report_date))].sort(),
    [rows],
  );

  const psi = useMemo(() => {
    const lookup = new Map(
      rows.map((r) => [`${r.feature_name}|${r.report_date}`, r]),
    );
    return features.map((f) =>
      dates.map((d) => lookup.get(`${f}|${d}`)?.psi_score ?? 0),
    );
  }, [rows]);

  return (
    <>
      <Box
        sx={{ px: 7, py: 7, borderBottom: '1px solid', borderColor: 'divider' }}
      >
        <Typography
          variant='overline'
          sx={{ color: 'text.disabled', mb: 2, display: 'block' }}
        >
          Feature distribution shift
        </Typography>
        <Typography variant='h2' sx={{ mb: 2 }}>
          Drift metrics
        </Typography>
        <Typography
          variant='body1'
          sx={{ color: 'text.secondary', maxWidth: 560 }}
        >
          Population Stability Index per feature over time. PSI &gt; 0.1 signals
          minor shift; PSI &gt; 0.2 signals major shift and should trigger a
          retrain evaluation.
        </Typography>
      </Box>

      <PsiLegend />
      <DriftHeatmap features={features} dates={dates} psi={psi} />
    </>
  );
}

// ─── legend ───────────────────────────────────────────────────────────────────

function PsiLegend() {
  return (
    <Box
      sx={{
        px: 7,
        py: 2.5,
        borderBottom: '1px solid',
        borderColor: 'divider',
        display: 'flex',
        alignItems: 'center',
        gap: 4,
      }}
    >
      <Typography
        sx={{ fontFamily: monoFont, fontSize: 11, color: 'text.disabled' }}
      >
        PSI thresholds
      </Typography>
      {[
        { label: '&lt; 0.1 · stable', color: 'success.main' },
        { label: '0.1 – 0.2 · minor shift', color: 'warning.main' },
        { label: '&gt; 0.2 · major shift', color: 'error.main' },
      ].map(({ label, color }) => (
        <Box
          key={label}
          sx={{ display: 'flex', alignItems: 'center', gap: 0.75 }}
        >
          <Box
            sx={{
              width: 8,
              height: 8,
              borderRadius: 0.5,
              bgcolor: color,
              flexShrink: 0,
            }}
          />
          <Typography
            sx={{ fontFamily: monoFont, fontSize: 11, color: 'text.secondary' }}
            dangerouslySetInnerHTML={{ __html: label }}
          />
        </Box>
      ))}
    </Box>
  );
}

// ─── heatmap ──────────────────────────────────────────────────────────────────

const SEVERITY_BG = {
  green: 'rgba(31, 122, 63, 0.15)',
  amber: 'rgba(181, 112, 27, 0.2)',
  red: 'rgba(178, 59, 42, 0.25)',
} as const;

const SEVERITY_COLOR = {
  green: 'success.main',
  amber: 'warning.main',
  red: 'error.main',
} as const;

function DriftHeatmap({
  features,
  dates,
  psi,
}: {
  features: string[];
  dates: string[];
  psi: number[][];
}) {
  if (features.length === 0 || dates.length === 0) {
    return (
      <Box sx={{ px: 7, py: 6 }}>
        <Typography sx={{ fontSize: 13, color: 'text.disabled' }}>
          No data — connect /api/drift/metrics
        </Typography>
      </Box>
    );
  }

  return (
    <Box sx={{ px: 7, py: 5, overflowX: 'auto' }}>
      <Typography
        variant='overline'
        sx={{ color: 'text.disabled', display: 'block' }}
      >
        PSI heatmap
      </Typography>
      <Typography variant='h3' sx={{ mt: 0.75, mb: 3 }}>
        Features × dates
      </Typography>

      <Box sx={{ display: 'inline-block', minWidth: '100%' }}>
        {/* date header */}
        <Box
          sx={{
            display: 'grid',
            gridTemplateColumns: `200px repeat(${dates.length}, 64px)`,
            gap: '2px',
            mb: '2px',
          }}
        >
          <Box />
          {dates.map((d) => (
            <Typography
              key={d}
              sx={{
                fontFamily: monoFont,
                fontSize: 10,
                color: 'text.disabled',
                textAlign: 'center',
                pb: 0.5,
              }}
            >
              {d}
            </Typography>
          ))}
        </Box>

        {/* feature rows */}
        {features.map((feature, fi) => (
          <Box
            key={feature}
            sx={{
              display: 'grid',
              gridTemplateColumns: `200px repeat(${dates.length}, 64px)`,
              gap: '2px',
              mb: '2px',
            }}
          >
            <Typography
              sx={{
                fontFamily: monoFont,
                fontSize: 12,
                color: 'text.secondary',
                display: 'flex',
                alignItems: 'center',
                pr: 1,
              }}
            >
              {feature}
            </Typography>
            {dates.map((_, di) => {
              const val = psi[fi]?.[di] ?? 0;
              const sev = psiSeverity(val);
              return (
                <Box
                  key={di}
                  title={`${feature} · ${dates[di]} · PSI ${val.toFixed(3)}`}
                  sx={{
                    height: 32,
                    bgcolor: SEVERITY_BG[sev],
                    borderRadius: 0.5,
                    display: 'flex',
                    alignItems: 'center',
                    justifyContent: 'center',
                    cursor: 'default',
                    transition: 'opacity 0.1s',
                    '&:hover': { opacity: 0.7 },
                  }}
                >
                  <Typography
                    sx={{
                      fontFamily: monoFont,
                      fontSize: 9,
                      color: SEVERITY_COLOR[sev],
                    }}
                  >
                    {val.toFixed(2)}
                  </Typography>
                </Box>
              );
            })}
          </Box>
        ))}
      </Box>

      <SummaryRow features={features} dates={dates} psi={psi} />
    </Box>
  );
}

function SummaryRow({
  features,
  dates,
  psi,
}: {
  features: string[];
  dates: string[];
  psi: number[][];
}) {
  const redCount = features.reduce(
    (acc, _, fi) =>
      acc +
      dates.filter((_, di) => psiSeverity(psi[fi]?.[di] ?? 0) === 'red').length,
    0,
  );
  const amberCount = features.reduce(
    (acc, _, fi) =>
      acc +
      dates.filter((_, di) => psiSeverity(psi[fi]?.[di] ?? 0) === 'amber')
        .length,
    0,
  );

  return (
    <Box sx={{ mt: 4, display: 'flex', gap: 4 }}>
      <SummaryPill
        value={String(redCount)}
        label='cells · PSI > 0.2'
        color='error.main'
      />
      <SummaryPill
        value={String(amberCount)}
        label='cells · PSI 0.1–0.2'
        color='warning.main'
      />
    </Box>
  );
}

function SummaryPill({
  value,
  label,
  color,
}: {
  value: string;
  label: string;
  color: string;
}) {
  return (
    <Box sx={{ display: 'flex', alignItems: 'baseline', gap: 1 }}>
      <Typography
        sx={{
          fontFamily: serifFont,
          fontSize: 32,
          lineHeight: 1,
          letterSpacing: '-0.02em',
          color,
        }}
      >
        {value}
      </Typography>
      <Typography
        sx={{ fontFamily: monoFont, fontSize: 11, color: 'text.secondary' }}
      >
        {label}
      </Typography>
    </Box>
  );
}
