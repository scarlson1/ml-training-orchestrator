import Box from '@mui/material/Box';
import Typography from '@mui/material/Typography';
import { useSuspenseQuery } from '@tanstack/react-query';
import { createFileRoute } from '@tanstack/react-router';
import { apiFetch } from '~/api';
import { Sparkline } from '~/components/Sparkline';
import { monoFont, serifFont } from '~/config/themePrimitives';

/*
 * Accuracy over time: ROC-AUC, F1, precision, recall per model version.
 * Actuals arrive ~60d after BTS submission, so recent dates may be empty.
 * Also shows positive_rate vs actual_positive_rate to surface label shift.
 *
 * query: GET /api/accuracy → AccuracyPoint[]
 * shape: { date, model_version, roc_auc, f1, precision, recall,
 *           positive_rate, actual_positive_rate }
 */

export const Route = createFileRoute('/accuracy')({
  component: Accuracy,
});

// TODO: confirm shape with backend
type AccuracyPoint = {
  score_date: string;
  model_version: string;
  roc_auc: number;
  f1: number;
  precision_score: number;
  recall_score: number;
  positive_rate: number;
  actual_positive_rate: number;
  n_with_actuals: number;
};

function Accuracy() {
  // query: GET /api/accuracy → AccuracyPoint[]
  const { data: tmp } = useSuspenseQuery({
    queryKey: ['accuracy'],
    queryFn: () =>
      apiFetch('/api/accuracy').then(
        (r) => r.json() as Promise<{ rows: AccuracyPoint[] }>,
      ),
    staleTime: 60 * 60 * 1000,
  });

  const data =
    !tmp.rows?.length && import.meta.env.DEV
      ? {
          rows: [
            {
              score_date: '2026-03-01',
              model_version: 'v2.3.9',
              roc_auc: 0.8812,
              f1: 0.7134,
              precision_score: 0.7423,
              recall_score: 0.6871,
              brier_score: 0.1634,
              positive_rate: 0.2312,
              actual_positive_rate: 0.2489,
              n_with_actuals: 812,
            },
            {
              score_date: '2026-03-04',
              model_version: 'v2.3.9',
              roc_auc: 0.8934,
              f1: 0.7289,
              precision_score: 0.7612,
              recall_score: 0.6994,
              brier_score: 0.1589,
              positive_rate: 0.2198,
              actual_positive_rate: 0.2341,
              n_with_actuals: 743,
            },
            {
              score_date: '2026-03-07',
              model_version: 'v2.3.9',
              roc_auc: 0.8767,
              f1: 0.7051,
              precision_score: 0.7298,
              recall_score: 0.6821,
              brier_score: 0.1701,
              positive_rate: 0.2441,
              actual_positive_rate: 0.2567,
              n_with_actuals: 891,
            },
            {
              score_date: '2026-03-10',
              model_version: 'v2.3.9',
              roc_auc: 0.9012,
              f1: 0.7341,
              precision_score: 0.7734,
              recall_score: 0.6981,
              brier_score: 0.1521,
              positive_rate: 0.2089,
              actual_positive_rate: 0.2213,
              n_with_actuals: 778,
            },
            {
              score_date: '2026-03-13',
              model_version: 'v2.3.9',
              roc_auc: 0.8891,
              f1: 0.7198,
              precision_score: 0.7512,
              recall_score: 0.6912,
              brier_score: 0.1612,
              positive_rate: 0.2334,
              actual_positive_rate: 0.2478,
              n_with_actuals: 834,
            },
            {
              score_date: '2026-03-16',
              model_version: 'v2.3.9',
              roc_auc: 0.9078,
              f1: 0.7412,
              precision_score: 0.7823,
              recall_score: 0.7041,
              brier_score: 0.1498,
              positive_rate: 0.2012,
              actual_positive_rate: 0.2189,
              n_with_actuals: 761,
            },
            {
              score_date: '2026-03-19',
              model_version: 'v2.3.9',
              roc_auc: 0.8823,
              f1: 0.7089,
              precision_score: 0.7334,
              recall_score: 0.6863,
              brier_score: 0.1678,
              positive_rate: 0.2389,
              actual_positive_rate: 0.2512,
              n_with_actuals: 809,
            },
            {
              score_date: '2026-03-22',
              model_version: 'v2.3.9',
              roc_auc: 0.9134,
              f1: 0.7523,
              precision_score: 0.7934,
              recall_score: 0.7148,
              brier_score: 0.1456,
              positive_rate: 0.1934,
              actual_positive_rate: 0.2067,
              n_with_actuals: 756,
            },
            {
              score_date: '2026-03-25',
              model_version: 'v2.3.9',
              roc_auc: 0.8978,
              f1: 0.7267,
              precision_score: 0.7589,
              recall_score: 0.6971,
              brier_score: 0.1567,
              positive_rate: 0.2267,
              actual_positive_rate: 0.2398,
              n_with_actuals: 823,
            },
            {
              score_date: '2026-03-28',
              model_version: 'v2.3.9',
              roc_auc: 0.9201,
              f1: 0.7634,
              precision_score: 0.8012,
              recall_score: 0.7289,
              brier_score: 0.1412,
              positive_rate: 0.1878,
              actual_positive_rate: 0.1998,
              n_with_actuals: 748,
            },
            {
              score_date: '2026-03-31',
              model_version: 'v2.3.9',
              roc_auc: 0.9089,
              f1: 0.7489,
              precision_score: 0.7867,
              recall_score: 0.7134,
              brier_score: 0.1489,
              positive_rate: 0.2034,
              actual_positive_rate: 0.2167,
              n_with_actuals: 791,
            },
            {
              score_date: '2026-04-03',
              model_version: 'v2.3.9',
              roc_auc: 0.9267,
              f1: 0.7712,
              precision_score: 0.8134,
              recall_score: 0.7334,
              brier_score: 0.1378,
              positive_rate: 0.1812,
              actual_positive_rate: 0.1934,
              n_with_actuals: 767,
            },
            {
              score_date: '2026-04-06',
              model_version: 'v2.3.9',
              roc_auc: 0.9178,
              f1: 0.7598,
              precision_score: 0.7978,
              recall_score: 0.7245,
              brier_score: 0.1434,
              positive_rate: 0.1967,
              actual_positive_rate: 0.2089,
              n_with_actuals: 812,
            },
            {
              score_date: '2026-04-09',
              model_version: 'v2.3.9',
              roc_auc: 0.9312,
              f1: 0.7834,
              precision_score: 0.8234,
              recall_score: 0.7467,
              brier_score: 0.1334,
              positive_rate: 0.1756,
              actual_positive_rate: 0.1878,
              n_with_actuals: 779,
            },
            {
              score_date: '2026-04-12',
              model_version: 'v2.3.9',
              roc_auc: 0.9234,
              f1: 0.7712,
              precision_score: 0.8089,
              recall_score: 0.7356,
              brier_score: 0.1389,
              positive_rate: 0.1889,
              actual_positive_rate: 0.2012,
              n_with_actuals: 834,
            },
            {
              score_date: '2026-04-15',
              model_version: 'v2.3.9',
              roc_auc: 0.9389,
              f1: 0.7956,
              precision_score: 0.8367,
              recall_score: 0.7578,
              brier_score: 0.1289,
              positive_rate: 0.1712,
              actual_positive_rate: 0.1823,
              n_with_actuals: 756,
            },
            {
              score_date: '2026-04-18',
              model_version: 'v2.3.9',
              roc_auc: 0.9301,
              f1: 0.7823,
              precision_score: 0.8198,
              recall_score: 0.7478,
              brier_score: 0.1345,
              positive_rate: 0.1845,
              actual_positive_rate: 0.1967,
              n_with_actuals: 798,
            },
            {
              score_date: '2026-04-21',
              model_version: 'v2.3.9',
              roc_auc: 0.9423,
              f1: 0.8034,
              precision_score: 0.8456,
              recall_score: 0.7645,
              brier_score: 0.1245,
              positive_rate: 0.1678,
              actual_positive_rate: 0.1789,
              n_with_actuals: 812,
            },
            {
              score_date: '2026-04-25',
              model_version: 'v2.4.1',
              roc_auc: 0.9489,
              f1: 0.8123,
              precision_score: 0.8534,
              recall_score: 0.7756,
              brier_score: 0.1198,
              positive_rate: 0.1934,
              actual_positive_rate: 0.2089,
              n_with_actuals: 823,
            },
            {
              score_date: '2026-04-28',
              model_version: 'v2.4.1',
              roc_auc: 0.9512,
              f1: 0.8198,
              precision_score: 0.8612,
              recall_score: 0.7834,
              brier_score: 0.1167,
              positive_rate: 0.1889,
              actual_positive_rate: 0.2012,
              n_with_actuals: 791,
            },
          ],
        }
      : tmp;

  const points: AccuracyPoint[] = data?.rows ?? [];
  const latest = points[points.length - 1];

  return (
    <>
      <Box
        sx={{ px: 7, py: 7, borderBottom: '1px solid', borderColor: 'divider' }}
      >
        <Typography
          variant='overline'
          sx={{ color: 'text.disabled', mb: 2, display: 'block' }}
        >
          Model performance · post-hoc actuals
        </Typography>
        <Typography variant='h2' sx={{ mb: 2 }}>
          Accuracy
        </Typography>
        <Typography
          variant='body1'
          sx={{ color: 'text.secondary', maxWidth: 560 }}
        >
          ROC-AUC, F1, precision and recall evaluated against BTS actuals, which
          arrive approximately 60 days after the flight date. Recent dates may
          be pending.
        </Typography>
      </Box>

      <MetricsRow latest={latest} />
      <ChartGrid points={points} />
      <AccuracyTable points={points} />
    </>
  );
}

// ─── headline metrics ─────────────────────────────────────────────────────────

function MetricsRow({ latest }: { latest: AccuracyPoint | undefined }) {
  const fmt4 = (v: number | undefined) => (v != null ? v.toFixed(4) : '—');
  const fmtPct = (v: number | undefined) =>
    v != null ? `${(v * 100).toFixed(1)}%` : '—';

  return (
    <Box
      sx={{
        px: 7,
        py: 5,
        borderBottom: '1px solid',
        borderColor: 'divider',
        display: 'grid',
        gridTemplateColumns: 'repeat(4, 1fr)',
        gap: 6,
      }}
    >
      {[
        { label: 'ROC-AUC', value: fmt4(latest?.roc_auc) },
        { label: 'F1', value: fmt4(latest?.f1) },
        { label: 'Precision', value: fmtPct(latest?.precision_score) },
        { label: 'Recall', value: fmtPct(latest?.recall_score) },
      ].map(({ label, value }) => (
        <Box key={label}>
          <Typography
            variant='overline'
            sx={{ color: 'text.disabled', mb: 1, display: 'block' }}
          >
            {label}
          </Typography>
          <Typography
            sx={{
              fontFamily: serifFont,
              fontSize: 44,
              fontWeight: 400,
              lineHeight: 1,
              letterSpacing: '-0.02em',
            }}
          >
            {value}
          </Typography>
          <Typography
            sx={{
              fontFamily: monoFont,
              fontSize: 11,
              color: 'text.disabled',
              mt: 1,
            }}
          >
            {latest?.score_date ?? 'latest'}
          </Typography>
        </Box>
      ))}
    </Box>
  );
}

// ─── chart grid ───────────────────────────────────────────────────────────────

function ChartGrid({ points }: { points: AccuracyPoint[] }) {
  return (
    <Box
      sx={{
        px: 7,
        py: 5,
        borderBottom: '1px solid',
        borderColor: 'divider',
        display: 'grid',
        gridTemplateColumns: '1.3fr 1fr',
        gap: 4,
      }}
    >
      <AucChart points={points} />
      <LabelShiftChart points={points} />
    </Box>
  );
}

function AucChart({ points }: { points: AccuracyPoint[] }) {
  const aucValues = points.map((p) => p.roc_auc);
  const f1Values = points.map((p) => p.f1);
  const labels = points.map((p) => p.score_date);

  return (
    <Box
      sx={{
        bgcolor: 'background.paper',
        border: '1px solid',
        borderColor: 'divider',
        borderRadius: 1,
        p: 3,
      }}
    >
      <Box
        sx={{
          display: 'flex',
          justifyContent: 'space-between',
          alignItems: 'baseline',
          mb: 2.5,
        }}
      >
        <Box>
          <Typography
            variant='overline'
            sx={{ color: 'text.disabled', display: 'block' }}
          >
            ROC-AUC · F1 · 14d
          </Typography>
          <Typography variant='h3' sx={{ mt: 0.75 }}>
            Performance over time
          </Typography>
        </Box>
        {aucValues.length > 0 && (
          <Typography sx={{ fontFamily: monoFont, fontSize: 12 }}>
            {aucValues[aucValues.length - 1]?.toFixed(4)}
            <Box component='span' sx={{ color: 'text.disabled' }}>
              {' '}
              AUC
            </Box>
          </Typography>
        )}
      </Box>

      {aucValues.length > 1 ? (
        <Box>
          <TimelineChart
            series={[
              { values: aucValues, color: 'primary.main', label: 'ROC-AUC' },
              { values: f1Values, color: 'text.secondary', label: 'F1' },
            ]}
            labels={labels}
            height={160}
          />
          <Box
            sx={{ display: 'flex', justifyContent: 'space-between', mt: 1.5 }}
          >
            <Typography
              sx={{
                fontFamily: monoFont,
                fontSize: 10,
                color: 'text.disabled',
              }}
            >
              {labels[0]}
            </Typography>
            <Typography
              sx={{
                fontFamily: monoFont,
                fontSize: 10,
                color: 'text.disabled',
              }}
            >
              {labels[labels.length - 1]}
            </Typography>
          </Box>
        </Box>
      ) : (
        <Typography sx={{ fontSize: 13, color: 'text.disabled', py: 2 }}>
          No data — connect /api/accuracy
        </Typography>
      )}
    </Box>
  );
}

function LabelShiftChart({ points }: { points: AccuracyPoint[] }) {
  const predicted = points.map((p) => p.positive_rate * 100);
  const actual = points.map((p) => p.actual_positive_rate * 100);
  const labels = points.map((p) => p.score_date);

  return (
    <Box
      sx={{
        bgcolor: 'background.paper',
        border: '1px solid',
        borderColor: 'divider',
        borderRadius: 1,
        p: 3,
      }}
    >
      <Box sx={{ mb: 2.5 }}>
        <Typography
          variant='overline'
          sx={{ color: 'text.disabled', display: 'block' }}
        >
          Label shift detector
        </Typography>
        <Typography variant='h3' sx={{ mt: 0.75 }}>
          Predicted vs actual rate
        </Typography>
      </Box>

      {predicted.length > 1 ? (
        <Box>
          <TimelineChart
            series={[
              { values: predicted, color: 'primary.main', label: 'Predicted' },
              { values: actual, color: 'success.main', label: 'Actual' },
            ]}
            labels={labels}
            height={160}
          />
          <Box sx={{ display: 'flex', gap: 3, mt: 2 }}>
            {[
              { label: 'Predicted', color: 'primary.main' },
              { label: 'Actual', color: 'success.main' },
            ].map(({ label, color }) => (
              <Box
                key={label}
                sx={{ display: 'flex', alignItems: 'center', gap: 0.75 }}
              >
                <Box
                  sx={{ width: 16, height: 1.5, bgcolor: color, flexShrink: 0 }}
                />
                <Typography
                  sx={{
                    fontFamily: monoFont,
                    fontSize: 11,
                    color: 'text.secondary',
                  }}
                >
                  {label}
                </Typography>
              </Box>
            ))}
          </Box>
        </Box>
      ) : (
        <Typography sx={{ fontSize: 13, color: 'text.disabled', py: 2 }}>
          No data — connect /api/accuracy
        </Typography>
      )}
    </Box>
  );
}

// ─── SVG multi-series timeline ────────────────────────────────────────────────

type Series = { values: number[]; color: string; label: string };

function TimelineChart({
  series,
  height = 140,
}: {
  series: Series[];
  labels: string[];
  height?: number;
}) {
  const allValues = series.flatMap((s) => s.values);
  if (allValues.length === 0) return null;

  return (
    <Box sx={{ position: 'relative', width: '100%' }}>
      {series.map((s, i) => (
        <Box
          key={i}
          sx={{
            position: i === 0 ? 'relative' : 'absolute',
            top: 0,
            left: 0,
            right: 0,
            color: s.color,
          }}
        >
          <Sparkline
            values={s.values}
            color='currentColor'
            height={height}
            width={400}
          />
        </Box>
      ))}
    </Box>
  );
}

// ─── data table ───────────────────────────────────────────────────────────────

const COLS = ['Date', 'Version', 'ROC-AUC', 'F1', 'Precision', 'Recall'];

function AccuracyTable({ points }: { points: AccuracyPoint[] }) {
  const sorted = [...points].reverse();
  return (
    <Box sx={{ px: 7, py: 5 }}>
      <Typography
        variant='overline'
        sx={{ color: 'text.disabled', display: 'block' }}
      >
        Evaluation history
      </Typography>
      <Typography variant='h3' sx={{ mt: 0.75, mb: 3 }}>
        All evaluations
      </Typography>

      <Box
        sx={{
          display: 'grid',
          gridTemplateColumns: '160px 100px repeat(4, 1fr)',
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
          No data — connect /api/accuracy
        </Typography>
      ) : (
        sorted.map((p, i) => <AccuracyRow key={i} point={p} />)
      )}
    </Box>
  );
}

function AccuracyRow({ point: p }: { point: AccuracyPoint }) {
  return (
    <Box
      sx={{
        display: 'grid',
        gridTemplateColumns: '160px 100px repeat(4, 1fr)',
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
        {p.score_date}
      </Typography>
      <Typography
        sx={{ fontFamily: monoFont, fontSize: 12, color: 'text.secondary' }}
      >
        {p.model_version}
      </Typography>
      <Typography sx={{ fontFamily: monoFont, fontSize: 13 }}>
        {p.roc_auc.toFixed(4)}
      </Typography>
      <Typography sx={{ fontFamily: monoFont, fontSize: 13 }}>
        {p.f1.toFixed(4)}
      </Typography>
      <Typography sx={{ fontFamily: monoFont, fontSize: 13 }}>
        {(p.precision_score * 100).toFixed(1)}%
      </Typography>
      <Typography sx={{ fontFamily: monoFont, fontSize: 13 }}>
        {(p.recall_score * 100).toFixed(1)}%
      </Typography>
    </Box>
  );
}
