import Box from '@mui/material/Box';
import Typography from '@mui/material/Typography';
import { useSuspenseQuery } from '@tanstack/react-query';
import { createFileRoute } from '@tanstack/react-router';
import { apiFetch } from '~/api';
import { Sparkline } from '~/components/Sparkline';
import { monoFont, serifFont } from '~/config/themePrimitives';

/*
 * Daily scoring summary: flights scored, positive rate, null feature rows.
 * Helps catch upstream feature pipeline failures before they hit accuracy metrics.
 *
 * query: GET /api/predictions → PredictionRun[]
 * shape: { date, count, positive_rate, null_feature_rows, model_version }
 */

export const Route = createFileRoute('/predictions')({
  component: Predictions,
});

// TODO: confirm shape with backend
type PredictionRun = {
  date: string;
  count: number;
  positive_rate: number;
  null_feature_rows: number;
  model_version: string;
};

function Predictions() {
  // query: GET /api/predictions → PredictionRun[]
  const { data } = useSuspenseQuery({
    queryKey: ['predictions'],
    queryFn: () =>
      apiFetch('/api/predictions').then((r) => r.json() as Promise<PredictionRun[]>),
    staleTime: 60 * 60 * 1000,
  });

  const runs: PredictionRun[] = Array.isArray(data) ? data : [];

  return (
    <>
      <PageHeader
        overline='Daily scoring summary'
        title='Prediction runs'
        description='Flights scored per run, delay positive rate, and null feature rows. Spikes in null rows indicate upstream feature pipeline failures.'
      />
      <ScoringMetricsStrip runs={runs} />
      <RunsTable runs={runs} />
    </>
  );
}

// ─── shared page header ───────────────────────────────────────────────────────

type PageHeaderProps = { overline: string; title: string; description: string };

function PageHeader({ overline, title, description }: PageHeaderProps) {
  return (
    <Box sx={{ px: 7, py: 7, borderBottom: '1px solid', borderColor: 'divider' }}>
      <Typography variant='overline' sx={{ color: 'text.disabled', mb: 2, display: 'block' }}>
        {overline}
      </Typography>
      <Typography variant='h2' sx={{ mb: 2 }}>
        {title}
      </Typography>
      <Typography variant='body1' sx={{ color: 'text.secondary', maxWidth: 560 }}>
        {description}
      </Typography>
    </Box>
  );
}

// ─── sparkline metric strip ───────────────────────────────────────────────────

function ScoringMetricsStrip({ runs }: { runs: PredictionRun[] }) {
  const latest = runs[runs.length - 1];
  return (
    <Box
      sx={{
        px: 7,
        py: 5,
        borderBottom: '1px solid',
        borderColor: 'divider',
        display: 'grid',
        gridTemplateColumns: 'repeat(3, 1fr)',
        gap: 4,
      }}
    >
      <MetricCard
        label='Flights scored'
        value={latest ? String(latest.count) : '—'}
        sub='latest run'
        sparkValues={runs.map((r) => r.count)}
        alert={false}
      />
      <MetricCard
        label='Delay positive rate'
        value={latest ? `${(latest.positive_rate * 100).toFixed(1)}%` : '—'}
        sub='latest run'
        sparkValues={runs.map((r) => r.positive_rate * 100)}
        alert={false}
      />
      <MetricCard
        label='Null feature rows'
        value={latest ? String(latest.null_feature_rows) : '—'}
        sub='latest run'
        sparkValues={runs.map((r) => r.null_feature_rows)}
        alert={!!latest?.null_feature_rows && latest.null_feature_rows > 0}
      />
    </Box>
  );
}

type MetricCardProps = {
  label: string;
  value: string;
  sub: string;
  sparkValues: number[];
  alert: boolean;
};

function MetricCard({ label, value, sub, sparkValues, alert }: MetricCardProps) {
  return (
    <Box
      sx={{
        bgcolor: 'background.paper',
        border: '1px solid',
        borderColor: alert ? 'error.main' : 'divider',
        borderRadius: 1,
        p: 2.5,
      }}
    >
      <Typography variant='overline' sx={{ color: 'text.disabled', display: 'block', mb: 1 }}>
        {label}
      </Typography>
      <Typography
        sx={{
          fontFamily: serifFont,
          fontSize: 36,
          fontWeight: 400,
          lineHeight: 1,
          letterSpacing: '-0.02em',
          color: alert ? 'error.main' : 'text.primary',
          mb: 1,
        }}
      >
        {value}
      </Typography>
      <Typography sx={{ fontFamily: monoFont, fontSize: 11, color: 'text.disabled', mb: 2 }}>
        {sub}
      </Typography>
      {sparkValues.length > 1 && (
        <Box sx={{ color: alert ? 'error.main' : 'text.secondary' }}>
          <Sparkline values={sparkValues} color='currentColor' height={28} width={200} />
        </Box>
      )}
    </Box>
  );
}

// ─── runs table ───────────────────────────────────────────────────────────────

const COLS = ['Date', 'Flights scored', 'Delay rate', 'Null rows', 'Model'];

function RunsTable({ runs }: { runs: PredictionRun[] }) {
  const sorted = [...runs].reverse();
  return (
    <Box sx={{ px: 7, py: 5 }}>
      <Typography variant='overline' sx={{ color: 'text.disabled', display: 'block' }}>
        Run history
      </Typography>
      <Typography variant='h3' sx={{ mt: 0.75, mb: 3 }}>
        All scoring runs
      </Typography>
      <Box
        sx={{
          display: 'grid',
          gridTemplateColumns: '160px 1fr 1fr 1fr 1fr',
          gap: 2,
          py: 1,
          borderBottom: '1px solid',
          borderColor: 'divider',
        }}
      >
        {COLS.map((col) => (
          <Typography
            key={col}
            variant='overline'
            sx={{ color: 'text.disabled', fontSize: '0.6rem' }}
          >
            {col}
          </Typography>
        ))}
      </Box>
      {sorted.length === 0 ? (
        <Typography sx={{ fontSize: 13, color: 'text.disabled', py: 4 }}>
          No data — connect /api/predictions
        </Typography>
      ) : (
        sorted.map((run, i) => <RunRow key={i} run={run} />)
      )}
    </Box>
  );
}

function RunRow({ run }: { run: PredictionRun }) {
  const hasNulls = run.null_feature_rows > 0;
  return (
    <Box
      sx={{
        display: 'grid',
        gridTemplateColumns: '160px 1fr 1fr 1fr 1fr',
        gap: 2,
        alignItems: 'center',
        py: 1.5,
        borderBottom: '1px solid',
        borderColor: 'divider',
        '&:last-child': { borderBottom: 'none' },
        '&:hover': { bgcolor: 'action.hover' },
        transition: 'background 0.1s',
      }}
    >
      <Typography sx={{ fontFamily: monoFont, fontSize: 12, color: 'text.secondary' }}>
        {run.date}
      </Typography>
      <Typography sx={{ fontFamily: monoFont, fontSize: 13 }}>
        {run.count.toLocaleString()}
      </Typography>
      <Typography sx={{ fontFamily: monoFont, fontSize: 13 }}>
        {(run.positive_rate * 100).toFixed(1)}%
      </Typography>
      <Typography
        sx={{
          fontFamily: monoFont,
          fontSize: 13,
          color: hasNulls ? 'error.main' : 'text.secondary',
        }}
      >
        {run.null_feature_rows}
      </Typography>
      <Typography sx={{ fontFamily: monoFont, fontSize: 12, color: 'text.secondary' }}>
        {run.model_version}
      </Typography>
    </Box>
  );
}
