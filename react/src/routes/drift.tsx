import Box from '@mui/material/Box';
import Typography from '@mui/material/Typography';
import { useSuspenseQuery } from '@tanstack/react-query';
import { createFileRoute } from '@tanstack/react-router';
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

// TODO: confirm shape with backend
type DriftMetrics = {
  features: string[];
  dates: string[];
  psi: number[][];
  kl: number[][];
};

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
  const { data } = useSuspenseQuery({
    queryKey: ['drift-metrics', { start, end }],
    queryFn: () =>
      apiFetch(`/api/drift/metrics?${params}`).then((r) => r.json() as Promise<DriftMetrics>),
    staleTime: 60 * 60 * 1000,
  });

  const features = data?.features ?? [];
  const dates = data?.dates ?? [];
  const psi = data?.psi ?? [];

  return (
    <>
      <Box sx={{ px: 7, py: 7, borderBottom: '1px solid', borderColor: 'divider' }}>
        <Typography variant='overline' sx={{ color: 'text.disabled', mb: 2, display: 'block' }}>
          Feature distribution shift
        </Typography>
        <Typography variant='h2' sx={{ mb: 2 }}>
          Drift metrics
        </Typography>
        <Typography variant='body1' sx={{ color: 'text.secondary', maxWidth: 560 }}>
          Population Stability Index per feature over time. PSI &gt; 0.1 signals minor shift;
          PSI &gt; 0.2 signals major shift and should trigger a retrain evaluation.
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
      <Typography sx={{ fontFamily: monoFont, fontSize: 11, color: 'text.disabled' }}>
        PSI thresholds
      </Typography>
      {[
        { label: '&lt; 0.1 · stable', color: 'success.main' },
        { label: '0.1 – 0.2 · minor shift', color: 'warning.main' },
        { label: '&gt; 0.2 · major shift', color: 'error.main' },
      ].map(({ label, color }) => (
        <Box key={label} sx={{ display: 'flex', alignItems: 'center', gap: 0.75 }}>
          <Box sx={{ width: 8, height: 8, borderRadius: 0.5, bgcolor: color, flexShrink: 0 }} />
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
      <Typography variant='overline' sx={{ color: 'text.disabled', display: 'block' }}>
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
  const redCount = features.reduce((acc, _, fi) =>
    acc + dates.filter((_, di) => psiSeverity(psi[fi]?.[di] ?? 0) === 'red').length, 0);
  const amberCount = features.reduce((acc, _, fi) =>
    acc + dates.filter((_, di) => psiSeverity(psi[fi]?.[di] ?? 0) === 'amber').length, 0);

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
        sx={{ fontFamily: serifFont, fontSize: 32, lineHeight: 1, letterSpacing: '-0.02em', color }}
      >
        {value}
      </Typography>
      <Typography sx={{ fontFamily: monoFont, fontSize: 11, color: 'text.secondary' }}>
        {label}
      </Typography>
    </Box>
  );
}
