import { OpenInNew } from '@mui/icons-material';
import { Button } from '@mui/material';
import Box from '@mui/material/Box';
import Typography from '@mui/material/Typography';
import { useSuspenseQuery } from '@tanstack/react-query';
import { createFileRoute } from '@tanstack/react-router';
import { apiFetch } from '~/api';
import { Sparkline } from '~/components/Sparkline';
import { monoFont, serifFont } from '~/config/themePrimitives';

/*
 * Model registry: all MLflow model versions, champion/challenger badge, ROC-AUC,
 * dataset hash, registered date. Line chart of AUC across versions.
 *
 * query: GET /api/model-stats → ModelVersion[]
 * shape: { version, status, roc_auc, f1, dataset_hash, registered_at }
 */

export const Route = createFileRoute('/models')({
  component: Models,
});

interface ModelStats {
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

function Models() {
  // query: GET /api/model-stats → ModelVersion[]
  const { data: tmp } = useSuspenseQuery({
    queryKey: ['models'],
    queryFn: () =>
      apiFetch('/api/model-stats').then(
        (r) => r.json() as Promise<{ rows: ModelStats[] }>,
      ),
    staleTime: 60 * 60 * 1000,
  });

  const { data: championDataTmp } = useSuspenseQuery({
    queryKey: ['models', 'champion'],
    queryFn: () =>
      apiFetch('/api/model-stats?champion=true').then(
        (r) => r.json() as Promise<{ rows: ModelStats[] }>,
      ),
    staleTime: 60 * 60 * 1000,
  });

  // TODO: delete
  let data =
    !tmp?.rows?.length && import.meta.env.DEV
      ? {
          rows: [
            {
              model_version: '09824435',
              last_scored: '2026-05-01',
              avg_roc_auc: 0.9583,
              avg_accuracy: 0.9241,
              avg_precision_score: 0.8712,
              avg_recall_score: 0.7934,
              avg_f1: 0.8304,
              avg_log_loss: 0.1872,
              avg_brier_score: 0.0714,
              avg_positive_rate: 0.1883,
              avg_actual_positive_rate: 0.2147,
              avg_n_flights_scored: 12394,
              total_n_flights: 42456,
            },
            {
              model_version: '09824434',
              last_scored: '2026-04-18',
              avg_roc_auc: 0.9441,
              avg_accuracy: 0.9118,
              avg_precision_score: 0.8534,
              avg_recall_score: 0.7701,
              avg_f1: 0.8097,
              avg_log_loss: 0.2214,
              avg_brier_score: 0.0883,
              avg_positive_rate: 0.2283,
              avg_actual_positive_rate: 0.2183,
              avg_n_flights_scored: 13789,
              total_n_flights: 124318,
            },
            {
              model_version: '09824433',
              last_scored: '2026-03-21',
              avg_roc_auc: 0.9312,
              avg_accuracy: 0.9034,
              avg_precision_score: 0.8291,
              avg_recall_score: 0.7512,
              avg_f1: 0.7882,
              avg_log_loss: 0.2589,
              avg_brier_score: 0.1021,
              avg_positive_rate: 0.2114,
              avg_actual_positive_rate: 0.2267,
              avg_n_flights_scored: 11203,
              total_n_flights: 89441,
            },
            {
              model_version: '09824432',
              last_scored: '2026-02-14',
              avg_roc_auc: 0.9178,
              avg_accuracy: 0.8967,
              avg_precision_score: 0.8043,
              avg_recall_score: 0.7288,
              avg_f1: 0.7647,
              avg_log_loss: 0.2934,
              avg_brier_score: 0.1189,
              avg_positive_rate: 0.1944,
              avg_actual_positive_rate: 0.2091,
              avg_n_flights_scored: 14512,
              total_n_flights: 173824,
            },
            {
              model_version: '09824431',
              last_scored: '2026-01-09',
              avg_roc_auc: 0.9034,
              avg_accuracy: 0.8812,
              avg_precision_score: 0.7834,
              avg_recall_score: 0.7103,
              avg_f1: 0.7451,
              avg_log_loss: 0.3341,
              avg_brier_score: 0.1378,
              avg_positive_rate: 0.2441,
              avg_actual_positive_rate: 0.2312,
              avg_n_flights_scored: 9874,
              total_n_flights: 69123,
            },
            {
              model_version: '09824430',
              last_scored: '2025-12-01',
              avg_roc_auc: 0.8891,
              avg_accuracy: 0.8634,
              avg_precision_score: 0.7612,
              avg_recall_score: 0.6934,
              avg_f1: 0.7257,
              avg_log_loss: 0.3812,
              avg_brier_score: 0.1592,
              avg_positive_rate: 0.2567,
              avg_actual_positive_rate: 0.2489,
              avg_n_flights_scored: 10341,
              total_n_flights: 51204,
            },
            {
              model_version: '09824429',
              last_scored: '2025-10-17',
              avg_roc_auc: 0.8712,
              avg_accuracy: 0.8441,
              avg_precision_score: 0.7334,
              avg_recall_score: 0.6712,
              avg_f1: 0.7009,
              avg_log_loss: 0.4234,
              avg_brier_score: 0.1814,
              avg_positive_rate: 0.2789,
              avg_actual_positive_rate: 0.2634,
              avg_n_flights_scored: 8923,
              total_n_flights: 35678,
            },
            {
              model_version: '09824428',
              last_scored: '2025-09-03',
              avg_roc_auc: 0.8534,
              avg_accuracy: 0.8223,
              avg_precision_score: 0.7091,
              avg_recall_score: 0.6489,
              avg_f1: 0.6776,
              avg_log_loss: 0.4712,
              avg_brier_score: 0.2043,
              avg_positive_rate: 0.3012,
              avg_actual_positive_rate: 0.2834,
              avg_n_flights_scored: 7654,
              total_n_flights: 23419,
            },
          ],
        }
      : tmp;

  let championData =
    !championDataTmp?.rows?.length && import.meta.env.DEV
      ? {
          rows: [
            {
              model_version: '09824435',
              last_scored: '2026-05-01',
              avg_roc_auc: 0.9583,
              avg_accuracy: 0.9241,
              avg_precision_score: 0.8712,
              avg_recall_score: 0.7934,
              avg_f1: 0.8304,
              avg_log_loss: 0.1872,
              avg_brier_score: 0.0714,
              avg_positive_rate: 0.1883,
              avg_actual_positive_rate: 0.2147,
              avg_n_flights_scored: 12394,
              total_n_flights: 42456,
            },
          ],
        }
      : championDataTmp;

  const versions = data.rows;
  const champion = championData?.rows[0] ?? null;
  const aucValues = versions.map((v) => v.avg_roc_auc);

  return (
    <>
      <Box
        sx={{ px: 7, py: 7, borderBottom: '1px solid', borderColor: 'divider' }}
      >
        <Typography
          variant='overline'
          sx={{ color: 'text.disabled', mb: 2, display: 'block' }}
        >
          Model registry
        </Typography>
        <Typography variant='h2' sx={{ mb: 2 }}>
          Versions
        </Typography>
        <Typography
          variant='body1'
          sx={{ color: 'text.secondary', maxWidth: 560 }}
        >
          All MLflow model versions with ROC-AUC, F1, and dataset lineage. The
          champion is the version currently serving predictions.
        </Typography>
      </Box>

      <ChampionBanner champion={champion} aucValues={aucValues} />
      <VersionTable versions={versions} />
    </>
  );
}

// ─── champion banner ──────────────────────────────────────────────────────────

function ChampionBanner({
  champion,
  aucValues,
}: {
  champion: ModelStats | null;
  aucValues: number[];
}) {
  const revAucValues = [...aucValues].reverse();

  return (
    <Box
      sx={{
        px: 7,
        py: 5,
        borderBottom: '1px solid',
        borderColor: 'divider',
        display: 'grid',
        gridTemplateColumns: '1.5fr 1fr 1fr 1fr',
        gap: 6,
        alignItems: 'start',
      }}
    >
      <Box>
        <Typography
          variant='overline'
          sx={{ color: 'text.disabled', display: 'block', mb: 1 }}
        >
          Champion model
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
          {champion?.model_version ?? '—'}
        </Typography>
        <Typography
          sx={{
            fontFamily: monoFont,
            fontSize: 11,
            color: 'text.secondary',
            mt: 1,
          }}
        >
          {champion?.last_scored ?? 'connect /api/model-stats'}
        </Typography>
      </Box>

      <MetricColumn
        label='ROC-AUC'
        value={
          champion?.avg_roc_auc != null ? champion.avg_roc_auc.toFixed(4) : '—'
        }
      />
      <MetricColumn
        label='F1'
        value={champion?.avg_f1 != null ? champion.avg_f1.toFixed(4) : '—'}
      />

      <Box>
        <Typography
          variant='overline'
          sx={{ color: 'text.disabled', display: 'block', mb: 1 }}
        >
          AUC trend · all versions
        </Typography>
        {aucValues.length > 1 ? (
          <Box sx={{ color: 'primary.main', mt: 1 }}>
            <Sparkline
              values={revAucValues}
              color='currentColor'
              height={40}
              width={180}
            />
          </Box>
        ) : (
          <Typography
            sx={{
              fontFamily: monoFont,
              fontSize: 12,
              color: 'text.disabled',
              mt: 1,
            }}
          >
            need ≥ 2 versions
          </Typography>
        )}
      </Box>
    </Box>
  );
}

function MetricColumn({ label, value }: { label: string; value: string }) {
  return (
    <Box>
      <Typography
        variant='overline'
        sx={{ color: 'text.disabled', display: 'block', mb: 1 }}
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
    </Box>
  );
}

// ─── version table ────────────────────────────────────────────────────────────

const COLS = [
  'Version',
  'ROC-AUC',
  'F1',
  'Accuracy',
  'Log Loss',
  'Last scored',
  'MLflow',
];

function VersionTable({ versions }: { versions: ModelStats[] }) {
  // const sorted = [...versions].reverse();
  return (
    <Box sx={{ px: 7, py: 5 }}>
      <Typography
        variant='overline'
        sx={{ color: 'text.disabled', display: 'block' }}
      >
        All versions
      </Typography>
      <Typography variant='h3' sx={{ mt: 0.75, mb: 3 }}>
        Version history
      </Typography>

      <Box
        sx={{
          display: 'grid',
          gridTemplateColumns: '100px 120px 1fr 1fr 180px 160px 100px',
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

      {versions.length === 0 ? (
        <Typography sx={{ fontSize: 13, color: 'text.disabled', py: 4 }}>
          No data — connect /api/model-stats
        </Typography>
      ) : (
        versions.map((v) => <VersionRow key={v.model_version} version={v} />)
      )}
    </Box>
  );
}

function VersionRow({ version: v }: { version: ModelStats }) {
  return (
    <Box
      sx={{
        display: 'grid',
        gridTemplateColumns: '140px 1fr 1fr 1fr 1fr 160px 100px',
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
      <Typography sx={{ fontFamily: monoFont, fontSize: 13, fontWeight: 600 }}>
        {v.model_version}
      </Typography>
      <Typography sx={{ fontFamily: monoFont, fontSize: 13 }}>
        {v.avg_roc_auc.toFixed(4)}
      </Typography>
      <Typography sx={{ fontFamily: monoFont, fontSize: 13 }}>
        {v.avg_f1.toFixed(4)}
      </Typography>
      <Typography sx={{ fontFamily: monoFont, fontSize: 13 }}>
        {v.avg_accuracy.toFixed(4)}
      </Typography>
      <Typography sx={{ fontFamily: monoFont, fontSize: 13 }}>
        {v.avg_log_loss.toFixed(4)}
      </Typography>
      <Typography
        sx={{ fontFamily: monoFont, fontSize: 12, color: 'text.secondary' }}
      >
        {v.last_scored}
      </Typography>
      <Button
        size='small'
        href={`${import.meta.env.VITE_MLFLOW_DASHBOARD_URL}/`}
        endIcon={<OpenInNew fontSize='small' />}
        rel='noopener noreferrer'
        target='_blank'
      >
        MLflow
      </Button>
    </Box>
  );
}
