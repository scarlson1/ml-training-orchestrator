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

// TODO: confirm shape with backend
type ModelVersion = {
  version: string;
  status: 'champion' | 'challenger' | 'archived';
  roc_auc: number;
  f1: number;
  dataset_hash: string;
  registered_at: string;
};

function Models() {
  // query: GET /api/model-stats → ModelVersion[]
  const { data } = useSuspenseQuery({
    queryKey: ['models'],
    queryFn: () =>
      apiFetch('/api/model-stats').then((r) => r.json() as Promise<ModelVersion[]>),
    staleTime: 60 * 60 * 1000,
  });

  const versions: ModelVersion[] = Array.isArray(data) ? data : [];
  const champion = versions.find((v) => v.status === 'champion');
  const aucValues = versions.map((v) => v.roc_auc);

  return (
    <>
      <Box sx={{ px: 7, py: 7, borderBottom: '1px solid', borderColor: 'divider' }}>
        <Typography variant='overline' sx={{ color: 'text.disabled', mb: 2, display: 'block' }}>
          Model registry
        </Typography>
        <Typography variant='h2' sx={{ mb: 2 }}>
          Versions
        </Typography>
        <Typography variant='body1' sx={{ color: 'text.secondary', maxWidth: 560 }}>
          All MLflow model versions with ROC-AUC, F1, and dataset lineage. The champion is the
          version currently serving predictions.
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
  champion: ModelVersion | undefined;
  aucValues: number[];
}) {
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
        <Typography variant='overline' sx={{ color: 'text.disabled', display: 'block', mb: 1 }}>
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
          {champion?.version ?? '—'}
        </Typography>
        <Typography sx={{ fontFamily: monoFont, fontSize: 11, color: 'text.secondary', mt: 1 }}>
          {champion?.registered_at ?? 'connect /api/model-stats'}
        </Typography>
      </Box>

      <MetricColumn
        label='ROC-AUC'
        value={champion?.roc_auc != null ? champion.roc_auc.toFixed(4) : '—'}
      />
      <MetricColumn
        label='F1'
        value={champion?.f1 != null ? champion.f1.toFixed(4) : '—'}
      />

      <Box>
        <Typography variant='overline' sx={{ color: 'text.disabled', display: 'block', mb: 1 }}>
          AUC trend · all versions
        </Typography>
        {aucValues.length > 1 ? (
          <Box sx={{ color: 'primary.main', mt: 1 }}>
            <Sparkline values={aucValues} color='currentColor' height={40} width={180} />
          </Box>
        ) : (
          <Typography sx={{ fontFamily: monoFont, fontSize: 12, color: 'text.disabled', mt: 1 }}>
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
      <Typography variant='overline' sx={{ color: 'text.disabled', display: 'block', mb: 1 }}>
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

const COLS = ['Version', 'Status', 'ROC-AUC', 'F1', 'Dataset', 'Registered'];

const STATUS_COLORS = {
  champion: 'success.main',
  challenger: 'warning.main',
  archived: 'text.disabled',
} as const;

function VersionTable({ versions }: { versions: ModelVersion[] }) {
  const sorted = [...versions].reverse();
  return (
    <Box sx={{ px: 7, py: 5 }}>
      <Typography variant='overline' sx={{ color: 'text.disabled', display: 'block' }}>
        All versions
      </Typography>
      <Typography variant='h3' sx={{ mt: 0.75, mb: 3 }}>
        Version history
      </Typography>

      <Box
        sx={{
          display: 'grid',
          gridTemplateColumns: '100px 120px 1fr 1fr 180px 160px',
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
          No data — connect /api/model-stats
        </Typography>
      ) : (
        sorted.map((v) => <VersionRow key={v.version} version={v} />)
      )}
    </Box>
  );
}

function VersionRow({ version: v }: { version: ModelVersion }) {
  return (
    <Box
      sx={{
        display: 'grid',
        gridTemplateColumns: '100px 120px 1fr 1fr 180px 160px',
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
        {v.version}
      </Typography>
      <Box
        sx={{
          display: 'inline-flex',
          alignItems: 'center',
          gap: 0.75,
          px: 1,
          py: 0.5,
          border: '1px solid',
          borderColor: STATUS_COLORS[v.status],
          borderRadius: 0.5,
          alignSelf: 'center',
          width: 'fit-content',
        }}
      >
        <Box
          sx={{
            width: 5,
            height: 5,
            borderRadius: '50%',
            bgcolor: STATUS_COLORS[v.status],
            flexShrink: 0,
          }}
        />
        <Typography
          sx={{
            fontFamily: monoFont,
            fontSize: 10,
            letterSpacing: '0.06em',
            color: STATUS_COLORS[v.status],
          }}
        >
          {v.status}
        </Typography>
      </Box>
      <Typography sx={{ fontFamily: monoFont, fontSize: 13 }}>{v.roc_auc.toFixed(4)}</Typography>
      <Typography sx={{ fontFamily: monoFont, fontSize: 13 }}>{v.f1.toFixed(4)}</Typography>
      <Typography
        sx={{
          fontFamily: monoFont,
          fontSize: 11,
          color: 'text.disabled',
          overflow: 'hidden',
          textOverflow: 'ellipsis',
          whiteSpace: 'nowrap',
        }}
        title={v.dataset_hash}
      >
        {v.dataset_hash}
      </Typography>
      <Typography sx={{ fontFamily: monoFont, fontSize: 12, color: 'text.secondary' }}>
        {v.registered_at}
      </Typography>
    </Box>
  );
}
