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
  date: string;
  model_version: string;
  roc_auc: number;
  f1: number;
  precision: number;
  recall: number;
  positive_rate: number;
  actual_positive_rate: number;
};

function Accuracy() {
  // query: GET /api/accuracy → AccuracyPoint[]
  const { data } = useSuspenseQuery({
    queryKey: ['accuracy'],
    queryFn: () =>
      apiFetch('/api/accuracy').then((r) => r.json() as Promise<AccuracyPoint[]>),
    staleTime: 60 * 60 * 1000,
  });

  const points: AccuracyPoint[] = Array.isArray(data) ? data : [];
  const latest = points[points.length - 1];

  return (
    <>
      <Box sx={{ px: 7, py: 7, borderBottom: '1px solid', borderColor: 'divider' }}>
        <Typography variant='overline' sx={{ color: 'text.disabled', mb: 2, display: 'block' }}>
          Model performance · post-hoc actuals
        </Typography>
        <Typography variant='h2' sx={{ mb: 2 }}>
          Accuracy
        </Typography>
        <Typography variant='body1' sx={{ color: 'text.secondary', maxWidth: 560 }}>
          ROC-AUC, F1, precision and recall evaluated against BTS actuals, which arrive
          approximately 60 days after the flight date. Recent dates may be pending.
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
  const fmtPct = (v: number | undefined) => (v != null ? `${(v * 100).toFixed(1)}%` : '—');

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
        { label: 'Precision', value: fmtPct(latest?.precision) },
        { label: 'Recall', value: fmtPct(latest?.recall) },
      ].map(({ label, value }) => (
        <Box key={label}>
          <Typography variant='overline' sx={{ color: 'text.disabled', mb: 1, display: 'block' }}>
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
          <Typography sx={{ fontFamily: monoFont, fontSize: 11, color: 'text.disabled', mt: 1 }}>
            {latest?.date ?? 'latest'}
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
  const labels = points.map((p) => p.date);

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
      <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'baseline', mb: 2.5 }}>
        <Box>
          <Typography variant='overline' sx={{ color: 'text.disabled', display: 'block' }}>
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
              {' '}AUC
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
          <Box sx={{ display: 'flex', justifyContent: 'space-between', mt: 1.5 }}>
            <Typography sx={{ fontFamily: monoFont, fontSize: 10, color: 'text.disabled' }}>
              {labels[0]}
            </Typography>
            <Typography sx={{ fontFamily: monoFont, fontSize: 10, color: 'text.disabled' }}>
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
  const labels = points.map((p) => p.date);

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
        <Typography variant='overline' sx={{ color: 'text.disabled', display: 'block' }}>
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
              <Box key={label} sx={{ display: 'flex', alignItems: 'center', gap: 0.75 }}>
                <Box sx={{ width: 16, height: 1.5, bgcolor: color, flexShrink: 0 }} />
                <Typography sx={{ fontFamily: monoFont, fontSize: 11, color: 'text.secondary' }}>
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
          <Sparkline values={s.values} color='currentColor' height={height} width={400} />
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
      <Typography variant='overline' sx={{ color: 'text.disabled', display: 'block' }}>
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
      <Typography sx={{ fontFamily: monoFont, fontSize: 12, color: 'text.secondary' }}>
        {p.date}
      </Typography>
      <Typography sx={{ fontFamily: monoFont, fontSize: 12, color: 'text.secondary' }}>
        {p.model_version}
      </Typography>
      <Typography sx={{ fontFamily: monoFont, fontSize: 13 }}>{p.roc_auc.toFixed(4)}</Typography>
      <Typography sx={{ fontFamily: monoFont, fontSize: 13 }}>{p.f1.toFixed(4)}</Typography>
      <Typography sx={{ fontFamily: monoFont, fontSize: 13 }}>
        {(p.precision * 100).toFixed(1)}%
      </Typography>
      <Typography sx={{ fontFamily: monoFont, fontSize: 13 }}>
        {(p.recall * 100).toFixed(1)}%
      </Typography>
    </Box>
  );
}
