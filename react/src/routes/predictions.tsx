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

type PredictionRun = {
  score_date: string;
  model_version: string;
  n_flights: number;
  positive_rate: number;
  avg_proba: number;
  n_with_actuals: number; // always 0 for today's predictions / last 30 days ??
};

function Predictions() {
  // returns last 30 days (default) of predictions
  const { data } = useSuspenseQuery({
    queryKey: ['predictions'],
    queryFn: () =>
      apiFetch('/api/predictions').then(
        (r) => r.json() as Promise<{ rows: PredictionRun[] }>,
      ),
    staleTime: 60 * 60 * 1000,
  });

  // TODO: remove fake data
  let runs: PredictionRun[] =
    !data?.rows?.length && import.meta.env.DEV
      ? {
          rows: [
            {
              score_date: '2026-05-01',
              model_version: 'v2.4.1',
              n_flights: 812,
              positive_rate: 0.1898,
              avg_proba: 0.2761,
              n_with_actuals: 0,
            },
            {
              score_date: '2026-04-30',
              model_version: 'v2.4.1',
              n_flights: 567,
              positive_rate: 0.2798,
              avg_proba: 0.2961,
              n_with_actuals: 0,
            },
            {
              score_date: '2026-04-29',
              model_version: 'v2.4.1',
              n_flights: 642,
              positive_rate: 0.1298,
              avg_proba: 0.1561,
              n_with_actuals: 0,
            },
            {
              score_date: '2026-04-28',
              model_version: 'v2.4.1',
              n_flights: 585,
              positive_rate: 0.0898,
              avg_proba: 0.0661,
              n_with_actuals: 0,
            },
            {
              score_date: '2026-04-27',
              model_version: 'v2.4.1',
              n_flights: 834,
              positive_rate: 0.1098,
              avg_proba: 0.0761,
              n_with_actuals: 0,
            },
            {
              score_date: '2026-04-26',
              model_version: 'v2.4.1',
              n_flights: 712,
              positive_rate: 0.0598,
              avg_proba: 0.1012,
              n_with_actuals: 0,
            },
            {
              score_date: '2026-04-25',
              model_version: 'v2.3.9',
              n_flights: 791,
              positive_rate: 0.3512,
              avg_proba: 0.3814,
              n_with_actuals: 0,
            },
            {
              score_date: '2026-04-24',
              model_version: 'v2.3.9',
              n_flights: 748,
              positive_rate: 0.3201,
              avg_proba: 0.3547,
              n_with_actuals: 0,
            },
            {
              score_date: '2026-04-23',
              model_version: 'v2.3.9',
              n_flights: 803,
              positive_rate: 0.2944,
              avg_proba: 0.3312,
              n_with_actuals: 789,
            },
            {
              score_date: '2026-04-22',
              model_version: 'v2.3.9',
              n_flights: 671,
              positive_rate: 0.1672,
              avg_proba: 0.1923,
              n_with_actuals: 671,
            },
            {
              score_date: '2026-04-21',
              model_version: 'v2.3.9',
              n_flights: 558,
              positive_rate: 0.1441,
              avg_proba: 0.1688,
              n_with_actuals: 558,
            },
            {
              score_date: '2026-04-20',
              model_version: 'v2.3.9',
              n_flights: 694,
              positive_rate: 0.1193,
              avg_proba: 0.1401,
              n_with_actuals: 694,
            },
            {
              score_date: '2026-04-19',
              model_version: 'v2.3.9',
              n_flights: 721,
              positive_rate: 0.0987,
              avg_proba: 0.1144,
              n_with_actuals: 721,
            },
            {
              score_date: '2026-04-18',
              model_version: 'v2.3.9',
              n_flights: 683,
              positive_rate: 0.1342,
              avg_proba: 0.1578,
              n_with_actuals: 683,
            },
            {
              score_date: '2026-04-17',
              model_version: 'v2.3.9',
              n_flights: 759,
              positive_rate: 0.2103,
              avg_proba: 0.2341,
              n_with_actuals: 759,
            },
            {
              score_date: '2026-04-16',
              model_version: 'v2.3.9',
              n_flights: 614,
              positive_rate: 0.1867,
              avg_proba: 0.2091,
              n_with_actuals: 614,
            },
            {
              score_date: '2026-04-15',
              model_version: 'v2.3.9',
              n_flights: 842,
              positive_rate: 0.1554,
              avg_proba: 0.1792,
              n_with_actuals: 842,
            },
            {
              score_date: '2026-04-14',
              model_version: 'v2.3.9',
              n_flights: 776,
              positive_rate: 0.1288,
              avg_proba: 0.1511,
              n_with_actuals: 776,
            },
            {
              score_date: '2026-04-13',
              model_version: 'v2.3.9',
              n_flights: 529,
              positive_rate: 0.0934,
              avg_proba: 0.1072,
              n_with_actuals: 529,
            },
            {
              score_date: '2026-04-12',
              model_version: 'v2.3.9',
              n_flights: 661,
              positive_rate: 0.1621,
              avg_proba: 0.1844,
              n_with_actuals: 661,
            },
            {
              score_date: '2026-04-11',
              model_version: 'v2.3.9',
              n_flights: 704,
              positive_rate: 0.2014,
              avg_proba: 0.2267,
              n_with_actuals: 704,
            },
            {
              score_date: '2026-04-10',
              model_version: 'v2.3.9',
              n_flights: 633,
              positive_rate: 0.1779,
              avg_proba: 0.2003,
              n_with_actuals: 633,
            },
            {
              score_date: '2026-04-09',
              model_version: 'v2.3.9',
              n_flights: 788,
              positive_rate: 0.1434,
              avg_proba: 0.1661,
              n_with_actuals: 788,
            },
            {
              score_date: '2026-04-08',
              model_version: 'v2.3.9',
              n_flights: 815,
              positive_rate: 0.1121,
              avg_proba: 0.1338,
              n_with_actuals: 815,
            },
            {
              score_date: '2026-04-07',
              model_version: 'v2.3.9',
              n_flights: 542,
              positive_rate: 0.0876,
              avg_proba: 0.1023,
              n_with_actuals: 542,
            },
            {
              score_date: '2026-04-06',
              model_version: 'v2.3.9',
              n_flights: 697,
              positive_rate: 0.1312,
              avg_proba: 0.1534,
              n_with_actuals: 697,
            },
            {
              score_date: '2026-04-05',
              model_version: 'v2.3.9',
              n_flights: 731,
              positive_rate: 0.1698,
              avg_proba: 0.1921,
              n_with_actuals: 731,
            },
            {
              score_date: '2026-04-04',
              model_version: 'v2.3.9',
              n_flights: 669,
              positive_rate: 0.2234,
              avg_proba: 0.2489,
              n_with_actuals: 669,
            },
            {
              score_date: '2026-04-03',
              model_version: 'v2.3.9',
              n_flights: 824,
              positive_rate: 0.2567,
              avg_proba: 0.2812,
              n_with_actuals: 824,
            },
            {
              score_date: '2026-04-02',
              model_version: 'v2.3.9',
              n_flights: 753,
              positive_rate: 0.1989,
              avg_proba: 0.2213,
              n_with_actuals: 753,
            },
          ],
        }.rows
      : data?.rows || [];

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
    <Box
      sx={{ px: 7, py: 7, borderBottom: '1px solid', borderColor: 'divider' }}
    >
      <Typography
        variant='overline'
        sx={{ color: 'text.disabled', mb: 2, display: 'block' }}
      >
        {overline}
      </Typography>
      <Typography variant='h2' sx={{ mb: 2 }}>
        {title}
      </Typography>
      <Typography
        variant='body1'
        sx={{ color: 'text.secondary', maxWidth: 560 }}
      >
        {description}
      </Typography>
    </Box>
  );
}

// ─── sparkline metric strip ───────────────────────────────────────────────────

function ScoringMetricsStrip({ runs }: { runs: PredictionRun[] }) {
  const latest = runs[0];
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
        value={latest ? String(latest.n_flights) : '—'}
        sub='latest run'
        sparkValues={runs.map((r) => r.n_flights)}
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
        label='Average Proba'
        value={latest ? String(latest.avg_proba) : '—'}
        sub='latest run'
        sparkValues={runs.map((r) => r.avg_proba)}
        alert={!!latest?.avg_proba && latest.avg_proba > 0.3}
      />
      {/* <MetricCard
        label='Null feature rows'
        value={latest ? String(latest.null_feature_rows) : '—'}
        sub='latest run'
        sparkValues={runs.map((r) => r.null_feature_rows)}
        alert={!!latest?.null_feature_rows && latest.null_feature_rows > 0}
      /> */}
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

function MetricCard({
  label,
  value,
  sub,
  sparkValues,
  alert,
}: MetricCardProps) {
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
      <Typography
        variant='overline'
        sx={{ color: 'text.disabled', display: 'block', mb: 1 }}
      >
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
      <Typography
        sx={{
          fontFamily: monoFont,
          fontSize: 11,
          color: 'text.disabled',
          mb: 2,
        }}
      >
        {sub}
      </Typography>
      {sparkValues.length > 1 && (
        <Box sx={{ color: alert ? 'error.main' : 'text.secondary' }}>
          <Sparkline
            values={sparkValues}
            color='currentColor'
            height={28}
            width={200}
          />
        </Box>
      )}
    </Box>
  );
}

// ─── runs table ───────────────────────────────────────────────────────────────

const COLS = ['Date', 'Flights scored', 'Delay rate', 'Average Proba', 'Model'];

function RunsTable({ runs }: { runs: PredictionRun[] }) {
  const sorted = [...runs].reverse();
  return (
    <Box sx={{ px: 7, py: 5 }}>
      <Typography
        variant='overline'
        sx={{ color: 'text.disabled', display: 'block' }}
      >
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
      <Typography
        sx={{ fontFamily: monoFont, fontSize: 12, color: 'text.secondary' }}
      >
        {run.score_date}
      </Typography>
      <Typography sx={{ fontFamily: monoFont, fontSize: 13 }}>
        {run.n_flights.toLocaleString()}
      </Typography>
      <Typography sx={{ fontFamily: monoFont, fontSize: 13 }}>
        {(run.positive_rate * 100).toFixed(1)}%
      </Typography>
      <Typography
        sx={{
          fontFamily: monoFont,
          fontSize: 13,
          color: 'text.secondary',
          // color: hasNulls ? 'error.main' : 'text.secondary',
        }}
      >
        {run.avg_proba.toFixed(4)}
      </Typography>
      <Typography
        sx={{ fontFamily: monoFont, fontSize: 12, color: 'text.secondary' }}
      >
        {run.model_version}
      </Typography>
    </Box>
  );
}
