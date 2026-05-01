import { OpenInNewRounded } from '@mui/icons-material';
import Box from '@mui/material/Box';
import Typography from '@mui/material/Typography';
import { useSuspenseQuery } from '@tanstack/react-query';
import { createFileRoute } from '@tanstack/react-router';
import { apiFetch } from '~/api';
import { monoFont, serifFont } from '~/config/themePrimitives';

export const Route = createFileRoute('/')({
  component: Home,
});

// TODO: confirm shapes with backend
type PredictionSummary = {
  count: number;
  positive_rate: number;
  null_feature_rows: number;
  run_at: string;
};

type DriftSummary = {
  psi_breaches: number;
  features: Array<{ name: string; psi: number; severity: 'green' | 'amber' | 'red' }>;
};

function Home() {
  return (
    <>
      <HeroSection />
      <StatsRow />
      <BodyGrid />
    </>
  );
}

// ─── hero ────────────────────────────────────────────────────────────────────

function HeroSection() {
  return (
    <Box
      sx={{
        px: 7,
        py: 7,
        borderBottom: '1px solid',
        borderColor: 'divider',
        display: 'grid',
        gridTemplateColumns: '1.1fr 1fr',
        gap: 8,
        alignItems: 'end',
      }}
    >
      <Box>
        <Typography variant='overline' sx={{ color: 'text.disabled', mb: 2, display: 'block' }}>
          Probabilistic delay prediction · Confidence-first
        </Typography>
        <Typography variant='h1' sx={{ mb: 2.5 }}>
          Know whether
          <br />
          the flight{' '}
          <Box component='em' sx={{ fontStyle: 'italic', color: 'text.secondary' }}>
            will
          </Box>{' '}
          hold —
          <br />
          before it pushes back.
        </Typography>
        <Typography variant='body1' sx={{ color: 'text.secondary', maxWidth: 480, lineHeight: 1.6 }}>
          Holdline's ensemble model fuses METAR, TAF, ground-stop bulletins, fleet rotation, and 9
          years of carrier OTP data into a calibrated probability — refreshed every 90 seconds.
        </Typography>
      </Box>
      <MonitoringLinksPanel />
    </Box>
  );
}

function MonitoringLinksPanel() {
  const links = [
    { label: 'Dagster', href: import.meta.env.VITE_DAGSTER_URL },
    { label: 'MLflow', href: import.meta.env.VITE_MLFLOW_DASHBOARD_URL },
    { label: 'S3 Storage', href: import.meta.env.VITE_S3_DASHBOARD_URL },
  ];

  return (
    <Box
      sx={{
        bgcolor: 'action.hover',
        border: '1px solid',
        borderColor: 'divider',
        borderRadius: 1,
        p: 3,
      }}
    >
      <Typography variant='overline' sx={{ color: 'text.disabled', mb: 1.75, display: 'block' }}>
        Monitoring links
      </Typography>
      <Box sx={{ display: 'flex', flexDirection: 'column', gap: 1 }}>
        {links.map(({ label, href }) => (
          <Box
            key={label}
            component='a'
            href={href}
            rel='noreferrer noopener'
            target='_blank'
            sx={{
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'space-between',
              px: 1.5,
              py: 1.25,
              bgcolor: 'background.paper',
              border: '1px solid',
              borderColor: 'divider',
              borderRadius: 0.5,
              color: 'text.primary',
              textDecoration: 'none',
              transition: 'border-color 0.15s',
              '&:hover': { borderColor: 'text.secondary' },
            }}
          >
            <Typography sx={{ fontFamily: monoFont, fontSize: 13 }}>{label}</Typography>
            <OpenInNewRounded sx={{ fontSize: 14, color: 'text.disabled' }} />
          </Box>
        ))}
      </Box>
    </Box>
  );
}

// ─── stats row ───────────────────────────────────────────────────────────────

function StatsRow() {
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
      <TodayStats />
      <ChampionModelStat />
    </Box>
  );
}

function TodayStats() {
  // query: GET /api/predictions/today → PredictionSummary
  const { data } = useSuspenseQuery({
    queryKey: ['predictions', 'today'],
    queryFn: () =>
      apiFetch('/api/predictions/today').then((r) => r.json() as Promise<PredictionSummary>),
    staleTime: 60 * 60 * 1000,
  });

  return (
    <>
      <StatCard
        label='Flights scored today'
        value={data?.count != null ? String(data.count) : '—'}
      />
      <StatCard
        label='Delay probability rate'
        value={
          data?.positive_rate != null ? `${(data.positive_rate * 100).toFixed(1)}%` : '—'
        }
      />
      <StatCard
        label='Null feature rows'
        value={data?.null_feature_rows != null ? String(data.null_feature_rows) : '—'}
        alert={data?.null_feature_rows != null && data.null_feature_rows > 0}
      />
    </>
  );
}

function ChampionModelStat() {
  // TODO: query GET /api/model-stats?champion=true → { version: string, roc_auc: number }
  return <StatCard label='Champion model' value='—' sub='connect /api/model-stats' />;
}

type StatCardProps = { label: string; value: string; sub?: string; alert?: boolean };

function StatCard({ label, value, sub, alert }: StatCardProps) {
  return (
    <Box>
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
          color: alert ? 'error.main' : 'text.primary',
        }}
      >
        {value}
      </Typography>
      {sub && (
        <Typography sx={{ fontFamily: monoFont, fontSize: 11, color: 'text.disabled', mt: 1 }}>
          {sub}
        </Typography>
      )}
    </Box>
  );
}

// ─── body grid ───────────────────────────────────────────────────────────────

function BodyGrid() {
  return (
    <Box
      sx={{
        px: 7,
        py: 5,
        display: 'grid',
        gridTemplateColumns: '1.3fr 1fr',
        gap: 4,
      }}
    >
      <FeatureDriftPanel />
      <PsiStatusCard />
    </Box>
  );
}

function FeatureDriftPanel() {
  // query: GET /api/drift/summary → DriftSummary
  const { data } = useSuspenseQuery({
    queryKey: ['drift', 'summary'],
    queryFn: () =>
      apiFetch('/api/drift/summary').then((r) => r.json() as Promise<DriftSummary>),
    staleTime: 60 * 60 * 1000,
  });

  const features = data?.features ?? [];

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
          <Typography variant='overline' sx={{ color: 'text.disabled', display: 'block' }}>
            Feature drift · today
          </Typography>
          <Typography variant='h3' sx={{ mt: 0.75 }}>
            What's driving shift
          </Typography>
        </Box>
        <Typography sx={{ fontFamily: monoFont, fontSize: 11, color: 'text.secondary' }}>
          PSI · population stability
        </Typography>
      </Box>

      {features.length === 0 ? (
        <Typography sx={{ fontSize: 13, color: 'text.disabled', py: 3 }}>
          No data — connect /api/drift/summary
        </Typography>
      ) : (
        features.map((f, i) => <DriftRow key={i} feature={f} />)
      )}
    </Box>
  );
}

type DriftFeature = {
  name: string;
  psi: number;
  severity: 'green' | 'amber' | 'red';
};

function DriftRow({ feature }: { feature: DriftFeature }) {
  const colorMap = { green: 'success.main', amber: 'warning.main', red: 'error.main' } as const;
  const barColor = colorMap[feature.severity];
  const pct = Math.min(feature.psi / 0.4, 1) * 100;

  return (
    <Box
      sx={{
        display: 'grid',
        gridTemplateColumns: '1fr 180px 52px',
        gap: 2,
        alignItems: 'center',
        py: 1.25,
        borderBottom: '1px solid',
        borderColor: 'divider',
        '&:last-child': { borderBottom: 'none' },
      }}
    >
      <Typography sx={{ fontSize: 13, fontWeight: 500 }}>{feature.name}</Typography>
      <Box
        sx={{
          position: 'relative',
          height: 6,
          bgcolor: 'action.disabledBackground',
          borderRadius: 0.25,
        }}
      >
        <Box
          sx={{
            position: 'absolute',
            top: 0,
            left: 0,
            bottom: 0,
            width: `${pct}%`,
            bgcolor: barColor,
            borderRadius: 0.25,
          }}
        />
      </Box>
      <Typography
        sx={{ fontFamily: monoFont, fontSize: 12, textAlign: 'right', color: barColor }}
      >
        {feature.psi.toFixed(3)}
      </Typography>
    </Box>
  );
}

function PsiStatusCard() {
  const { data } = useSuspenseQuery({
    queryKey: ['drift', 'summary'],
    queryFn: () =>
      apiFetch('/api/drift/summary').then((r) => r.json() as Promise<DriftSummary>),
    staleTime: 60 * 60 * 1000,
  });

  const breachCount = data?.psi_breaches ?? 0;
  const hasBreaches = breachCount > 0;

  return (
    <Box
      sx={{
        bgcolor: 'background.paper',
        border: '1px solid',
        borderColor: hasBreaches ? 'error.main' : 'divider',
        borderRadius: 1,
        p: 3,
        display: 'flex',
        flexDirection: 'column',
        gap: 2.5,
      }}
    >
      <Box>
        <Typography variant='overline' sx={{ color: 'text.disabled', display: 'block' }}>
          PSI status
        </Typography>
        <Typography variant='h3' sx={{ mt: 0.75 }}>
          {hasBreaches ? 'Breaches detected' : 'All clear'}
        </Typography>
      </Box>

      <Box sx={{ display: 'flex', alignItems: 'baseline', gap: 1 }}>
        <Typography
          sx={{
            fontFamily: serifFont,
            fontSize: 64,
            lineHeight: 1,
            letterSpacing: '-0.025em',
            color: hasBreaches ? 'error.main' : 'success.main',
          }}
        >
          {breachCount}
        </Typography>
        <Typography sx={{ fontSize: 14, color: 'text.secondary' }}>
          {breachCount === 1 ? 'feature' : 'features'} breached
        </Typography>
      </Box>

      <Typography sx={{ fontSize: 13, color: 'text.secondary', lineHeight: 1.55 }}>
        {hasBreaches
          ? 'PSI threshold of 0.2 exceeded. Review drift metrics for affected features.'
          : 'No PSI threshold breaches. All monitored features are within bounds.'}
      </Typography>

      {hasBreaches && (
        <Box
          sx={{
            fontFamily: monoFont,
            fontSize: 11,
            color: 'error.main',
            px: 1.25,
            py: 0.75,
            border: '1px solid',
            borderColor: 'error.main',
            borderRadius: 0.5,
            alignSelf: 'flex-start',
          }}
        >
          PSI &gt; 0.2 · retrain recommended
        </Box>
      )}
    </Box>
  );
}
