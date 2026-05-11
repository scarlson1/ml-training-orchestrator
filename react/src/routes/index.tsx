import Box from '@mui/material/Box';
import Chip from '@mui/material/Chip';
import Paper from '@mui/material/Paper';
import Skeleton from '@mui/material/Skeleton';
import Stack from '@mui/material/Stack';
import { useTheme } from '@mui/material/styles';
import ToggleButton from '@mui/material/ToggleButton';
import ToggleButtonGroup from '@mui/material/ToggleButtonGroup';
import Typography from '@mui/material/Typography';
import { useSuspenseQuery } from '@tanstack/react-query';
import { createFileRoute } from '@tanstack/react-router';
import { Suspense, useMemo, useState } from 'react';
import { ErrorBoundary } from 'react-error-boundary';
import {
  aviationWeatherOptions,
  delayOptions,
  driftSummaryOptions,
  sampleFlightOptions,
  todaysPredictionOptions,
  type SampleFlight,
} from '~/api/queryOptions';
import { CarrierComparison } from '~/components/CarrierComparison';
import { CarrierDelayTrend } from '~/components/CarrierDelayTrend';
import { Globe } from '~/components/Globe';
import { NetworkMap } from '~/components/NetworkMap';
import {
  PredictDelay,
  type PredictContext,
  type PredictResponse,
} from '~/components/PredictDelay';
import { RouteHistoryChart } from '~/components/RouteHistoryChart';
import { RouterButton } from '~/components/RouterButton';
import { StatCard } from '~/components/StatCard';
import { monoFont, serifFont } from '~/config/themePrimitives';
import { useResolvedMode } from '~/hooks/useResolvedMode';
import { carrierCodeToName, getAirportCity, iataToIcao } from '~/utils/misc';
import type { GetDelaysOptions } from '~/utils/weather.server';

// TODO: consider adding SHAP values / feature attribution calculations to backend (wire to existing attribution component)

export const Route = createFileRoute('/')({
  component: Index,
  loader: ({ context: { queryClient } }) =>
    Promise.allSettled([
      queryClient.prefetchQuery(todaysPredictionOptions),
      queryClient.prefetchQuery(driftSummaryOptions),
    ]),
});

// ─── Mock data ────────────────────────────────────────────────────────────────

interface Factor {
  name: string;
  value: number;
  detail: string;
}

// for tracking common data between user input predict & sample flights
interface ActiveFlight extends PredictContext {
  flight_id?: string;
  baseline_ontime_prob?: number; // pre-computed aggregate from sample flights API — not present for user-entered flights
}

const sampleToActive = (f: SampleFlight): ActiveFlight => ({
  origin: f.origin,
  dest: f.dest,
  carrier: f.carrier,
  flight_number: f.flight_number,
  flight_id: f.flight_id,
  scheduled_departure_utc: f.scheduled_departure_utc,
  baseline_ontime_prob: f.onTimeProb,
});

// ─── Small atoms ──────────────────────────────────────────────────────────────

function ProbabilityArc({ prob, size = 200 }: { prob: number; size?: number }) {
  const p = useTheme().vars.palette;
  const r = size / 2 - 18;
  const cx = size / 2;
  const cy = size / 2;
  const startA = Math.PI * 0.8;
  const endA = Math.PI * 0.2 + Math.PI * 2;
  const total = endA - startA;
  const ang = startA + total * prob;
  const polar = (a: number): [number, number] => [
    cx + r * Math.cos(a),
    cy + r * Math.sin(a),
  ];
  const [x1, y1] = polar(startA);
  const [x2, y2] = polar(ang);
  const [bx, by] = polar(endA);
  const largeArc = ang - startA > Math.PI ? 1 : 0;

  return (
    <svg width={size} height={size} style={{ display: 'block' }}>
      <path
        d={`M ${x1} ${y1} A ${r} ${r} 0 1 1 ${bx} ${by}`}
        stroke={p.custom.lineSoft}
        strokeWidth='2'
        fill='none'
      />
      <path
        d={`M ${x1} ${y1} A ${r} ${r} 0 ${largeArc} 1 ${x2} ${y2}`}
        stroke={p.text.primary}
        strokeWidth='2'
        fill='none'
        strokeLinecap='round'
      />
      {[0, 0.25, 0.5, 0.75, 1].map((pv, i) => {
        const a = startA + total * pv;
        const [tx, ty] = polar(a);
        const [tx2, ty2] = [
          cx + (r - 6) * Math.cos(a),
          cy + (r - 6) * Math.sin(a),
        ];
        return (
          <line
            key={i}
            x1={tx}
            y1={ty}
            x2={tx2}
            y2={ty2}
            stroke={p.divider}
            strokeWidth='1'
          />
        );
      })}
    </svg>
  );
}

function FactorBar({ factor }: { factor: Factor }) {
  const p = useTheme().vars.palette;
  const v = factor.value;
  const pct = Math.min(Math.abs(v) / 0.5, 1) * 50; // normalize & clamp to 1
  const positive = v >= 0;

  return (
    <Box
      sx={{
        display: 'flex',
        alignItems: 'center',
        gap: 2,
        py: '10px',
        borderBottom: `1px solid ${p.custom.lineSoft}`,
      }}
    >
      <Box sx={{ flex: '1 1 0', minWidth: 0, overflow: 'hidden' }}>
        <Typography
          sx={{
            fontSize: 13,
            color: p.text.primary,
            fontWeight: 500,
            whiteSpace: 'nowrap',
            overflow: 'hidden',
            textOverflow: 'ellipsis',
          }}
        >
          {factor.name}
        </Typography>
        <Typography
          sx={{
            fontSize: 11,
            color: p.text.disabled,
            mt: '2px',
            whiteSpace: 'nowrap',
            overflow: 'hidden',
            textOverflow: 'ellipsis',
          }}
        >
          {factor.detail}
        </Typography>
      </Box>
      <Box
        sx={{
          flex: '2 1 0',
          position: 'relative',
          height: 6,
          bgcolor: p.custom.lineSoft,
          borderRadius: '1px',
          minWidth: 0,
        }}
      >
        <Box
          sx={{
            position: 'absolute',
            top: 0,
            bottom: 0,
            left: '50%',
            width: '1px',
            bgcolor: p.divider,
          }}
        />
        <Box
          sx={{
            position: 'absolute',
            top: 0,
            bottom: 0,
            left: positive ? '50%' : `${50 - pct}%`,
            width: `${pct}%`,
            bgcolor: positive ? p.success.main : p.error.main,
            borderRadius: '1px',
          }}
        />
      </Box>
      <Typography
        sx={{
          flex: '0 0 36px',
          fontFamily: monoFont,
          fontSize: 11,
          textAlign: 'right',
          color: positive ? p.success.main : p.error.main,
          whiteSpace: 'nowrap',
        }}
      >
        {positive ? '+' : ''}
        {(v * 100).toFixed(0)}
      </Typography>
    </Box>
  );
}

// ─── FlightFake switcher ──────────────────────────────────────────────────────────

function FlightSwitcher({
  flights,
  currentId,
  onPick,
}: {
  flights: SampleFlight[];
  currentId: string | null;
  onPick: (f: SampleFlight) => void;
}) {
  const p = useTheme().vars.palette;

  return (
    <ToggleButtonGroup
      value={currentId || ''}
      exclusive
      onChange={(_, newId: string | null) => {
        if (newId != null) {
          const f = flights.find((fl) => fl.flight_id === newId);
          if (f) onPick(f);
        }
      }}
      sx={{
        display: 'flex',
        flexWrap: 'wrap',
        gap: 1,
        '& .MuiToggleButtonGroup-grouped': {
          border: `1px solid ${p.divider} !important`,
          borderRadius: '2px !important',
          color: p.text.primary,
          fontFamily: monoFont,
          fontSize: 11,
          letterSpacing: '0.04em',
          px: '10px',
          py: '6px',
          textTransform: 'none',
          '&.Mui-selected': {
            bgcolor: p.text.primary,
            color: p.background.default,
            borderColor: `${p.text.primary} !important`,
            '&:hover': { bgcolor: p.text.secondary },
          },
        },
      }}
    >
      {flights.map((f) => (
        <ToggleButton key={f.flight_id} value={f.flight_id}>
          {f.carrier} {f.flight_number} · {f.origin}→{f.dest}
        </ToggleButton>
      ))}
    </ToggleButtonGroup>
  );
}

// ─── KPI strip (real data) ────────────────────────────────────────────────────

function KpiItem({
  title,
  value,
  subtitle,
}: {
  title: string;
  value: string | number | null | undefined;
  subtitle: string;
}) {
  const p = useTheme().vars.palette;
  return (
    <Box sx={{ borderLeft: `1px solid ${p.divider}`, pl: '14px' }}>
      <Typography
        sx={{
          fontFamily: monoFont,
          fontSize: 9,
          color: p.text.disabled,
          letterSpacing: '0.12em',
          textTransform: 'uppercase',
        }}
      >
        {title}
      </Typography>
      <Typography
        sx={{
          fontFamily: serifFont,
          fontSize: 26,
          color: p.text.primary,
          letterSpacing: '-0.02em',
          mt: '2px',
        }}
      >
        {value}
      </Typography>
      <Typography
        sx={{
          fontFamily: monoFont,
          fontSize: 10,
          color: p.text.disabled,
          mt: '2px',
        }}
      >
        {subtitle}
      </Typography>
    </Box>
  );
}

function KpiStrip() {
  const p = useTheme().vars.palette;
  const { data: pred } = useSuspenseQuery({
    ...todaysPredictionOptions,
    retry: false,
  });
  const { data: drift } = useSuspenseQuery({
    ...driftSummaryOptions,
    retry: false,
  });

  const nFlights = pred?.n_flights_today ?? 0;
  const onTimePct =
    pred?.positive_rate_today != null
      ? (1 - pred.positive_rate_today) * 100
      : null;
  const alerts = drift?.psi_breaches ?? 0;
  const modelVer = pred?.model_version ?? '—';

  const kpis = [
    {
      l: 'Flights scored',
      v: nFlights ? nFlights.toLocaleString() : '—',
      s: 'in scored partition',
    },
    {
      l: 'Predicted on-time',
      v: onTimePct != null ? `${onTimePct.toFixed(0)}%` : '—',
      s: '↓ live rate',
    },
    {
      l: 'PSI alerts',
      v: String(alerts),
      s: alerts > 0 ? 'drift detected' : 'all clear',
    },
    {
      l: 'Model version',
      v: modelVer ? `v${modelVer.slice(0, 6)}` : '—',
      s: 'champion',
    },
  ];

  return (
    <Box sx={{ mt: '28px' }}>
      <Stack direction='row' spacing='28px'>
        {kpis.map((k, i) => (
          <KpiItem key={`kpi-${i}`} title={k.l} value={k.v} subtitle={k.s} />
        ))}
      </Stack>
      {pred?.data_as_of && (
        <Typography
          sx={{
            mt: '10px',
            fontFamily: monoFont,
            fontSize: 10,
            color: p.text.disabled,
            letterSpacing: '0.08em',
          }}
        >
          Data as of {pred.data_as_of} · BTS reporting lag ~60d
        </Typography>
      )}
    </Box>
  );
}

const KPI_FALLBACK = [
  { l: 'In flight today', v: '—', s: 'flights scored' },
  { l: 'Predicted on-time', v: '—', s: '- live rate' },
  { l: 'PSI alerts', v: '—', s: 'unknown' },
  { l: 'Model version', v: '—', s: 'champion' },
];

// ─── Hero section ─────────────────────────────────────────────────────────────

interface HeroProps {
  sampleFlights: SampleFlight[];
  flight: ActiveFlight | null;
  onPickFlight: (f: SampleFlight) => void;
  onPredict: (p: PredictResponse, context: PredictContext) => void;
}

function HeroSection({
  sampleFlights,
  flight,
  onPickFlight,
  onPredict,
}: HeroProps) {
  const p = useTheme().vars.palette;
  const isDark = useResolvedMode() === 'dark';

  return (
    <Box
      component='section'
      sx={{
        position: 'relative',
        p: '56px 56px 36px',
        borderBottom: `1px solid ${p.divider}`,
        background: isDark
          ? 'radial-gradient(ellipse at 65% 50%, #0A1124 0%, #0F0F0E 65%)'
          : 'radial-gradient(ellipse at 65% 50%, #FFFFFF 0%, #FBFAF7 65%)',
        overflow: 'hidden',
      }}
    >
      <Box
        sx={{
          position: 'absolute',
          right: -120,
          top: -40,
          width: 720,
          height: 720,
          pointerEvents: 'none',
          opacity: 0.95,
          filter: isDark
            ? 'drop-shadow(0 4px 16px rgba(0,0,0,0.40)) drop-shadow(0 1px 4px rgba(0,0,0,0.25))'
            : 'drop-shadow(0 4px 16px rgba(0,0,0,0.06)) drop-shadow(0 1px 3px rgba(0,0,0,0.08))',
        }}
      >
        <Globe isDark={isDark} size={720} />
      </Box>

      <Box
        sx={{
          position: 'relative',
          display: 'grid',
          gridTemplateColumns: '1.1fr 1fr',
          gap: 8,
          alignItems: 'end',
          minHeight: 520,
        }}
      >
        <Box
          sx={{
            alignSelf: 'stretch',
            display: 'flex',
            flexDirection: 'column',
            alignItems: 'flex-start',
            justifyContent: 'flex-start',
          }}
        >
          <Stack
            direction='row'
            sx={{ mb: 2, fontFamily: monoFont, alignItems: 'center' }}
          >
            <Box
              component='span'
              sx={{
                display: 'inline-block',
                width: 6,
                height: 6,
                borderRadius: '50%',
                bgcolor: p.success.main,
                mr: 1,
                flexShrink: 0,
              }}
            />
            <Typography
              sx={{
                fontFamily: monoFont,
                fontSize: 11,
                color: p.text.disabled,
                letterSpacing: '0.12em',
                textTransform: 'uppercase',
              }}
            >
              Live · ML flight delay prediction
            </Typography>
          </Stack>

          <Typography
            component='h1'
            sx={{
              fontFamily: serifFont,
              fontSize: 64,
              lineHeight: 0.98,
              letterSpacing: '-0.025em',
              fontWeight: 400,
              color: p.text.primary,
            }}
          >
            Know whether
            <br />
            the flight{' '}
            <Box
              component='em'
              sx={{ fontStyle: 'italic', color: p.text.secondary }}
            >
              will
            </Box>{' '}
            hold —<br />
            before it pushes back.
          </Typography>

          <Typography
            sx={{
              mt: '20px',
              fontSize: 15,
              color: p.text.secondary,
              maxWidth: 480,
              lineHeight: 1.55,
              fontFamily: 'Inter, sans-serif',
              flexGrow: 1,
            }}
          >
            Holdline's ensemble model fuses METAR, TAF, ground-stop bulletins,
            fleet rotation, and 9 years of carrier OTP data into a calibrated
            probability — features refreshed hourly via Feast.
          </Typography>

          <ErrorBoundary
            fallbackRender={({ error }) => {
              console.log(error);
              return (
                <>
                  <Stack direction='row' spacing='28px' sx={{ mt: '28px' }}>
                    {KPI_FALLBACK.map((k, i) => (
                      <KpiItem
                        key={`kpi-${i}`}
                        title={k.l}
                        value={k.v}
                        subtitle={k.s}
                      />
                    ))}
                  </Stack>
                  <Typography color='error' sx={{ py: 1 }}>
                    {`Api Error: ${error instanceof Error ? error.message : 'Unknown error occurred. See console.'}`}
                  </Typography>
                </>
              );
            }}
          >
            <Suspense
              fallback={
                <Stack direction='row' spacing='28px' sx={{ mt: '28px' }}>
                  {Array.from({ length: 4 }).map((_, i) => (
                    <Box
                      key={`load-kpi-${i}`}
                      sx={{ borderLeft: `1px solid ${p.divider}`, pl: '14px' }}
                    >
                      <Skeleton width={72} height={10} />
                      <Skeleton width={40} height={30} sx={{ mt: '6px' }} />
                      <Skeleton width={56} height={10} sx={{ mt: '6px' }} />
                    </Box>
                  ))}
                </Stack>
              }
            >
              <KpiStrip />
            </Suspense>
          </ErrorBoundary>
        </Box>

        {/* Prediction form */}
        <Paper
          variant='outlined'
          sx={{
            bgcolor: p.custom.panelAlt,
            borderColor: p.custom.lineSoft,
            borderRadius: '4px',
            p: 3,
          }}
        >
          <Typography
            sx={{
              fontFamily: monoFont,
              fontSize: 10,
              color: p.text.disabled,
              letterSpacing: '0.1em',
              textTransform: 'uppercase',
              mb: '14px',
            }}
          >
            Predict a flight
          </Typography>

          <ErrorBoundary fallback={null}>
            <Suspense
              fallback={
                <Stack direction='row' spacing={2}>
                  {Array({ length: 5 }).map(() => (
                    <Skeleton variant='rounded' height={40} width='100%' />
                  ))}
                </Stack>
              }
            >
              <PredictDelay onPredict={onPredict} />
            </Suspense>
          </ErrorBoundary>

          {sampleFlights.length ? (
            <Box sx={{ mt: 1.5 }}>
              <FlightSwitcher
                flights={sampleFlights}
                currentId={flight?.flight_id ?? null}
                onPick={onPickFlight}
              />
            </Box>
          ) : null}
        </Paper>
      </Box>
    </Box>
  );
}

// ─── Prediction headline ──────────────────────────────────────────────────────

interface PredHeadlineProps {
  flight: ActiveFlight | null;
  onTimeProb: number | null;
  verdict: { label: string; color: string };
  prediction: PredictResponse | null;
}

function PredictionHeadline({
  flight,
  onTimeProb,
  verdict,
  prediction,
}: PredHeadlineProps) {
  const p = useTheme().vars.palette;

  // TODO: replace metrics with available data
  const delayP50 = prediction
    ? prediction.delay_probability > 0.5
      ? 30
      : 5
    : 0; // flight.delayMin.p50;
  const delayP90 = prediction
    ? prediction.delay_probability > 0.5
      ? 90
      : 20
    : 0; // flight?.delayMin?.p90;
  const cancelProb = 0; // flight.cancelProb;
  const confidence = 0; // Math.abs(onTimeProb - 0.5) * 2;

  return (
    <Box
      component='section'
      sx={{ p: '40px 56px', borderBottom: `1px solid ${p.divider}` }}
    >
      <Box
        sx={{
          display: 'grid',
          gridTemplateColumns: '1.5fr 1fr 1fr 1fr',
          gap: 6,
          alignItems: 'start',
        }}
      >
        {/* flight identity */}
        <Box>
          <Stack
            direction='row'
            spacing='18px'
            sx={{ mb: 1, alignItems: 'baseline' }}
          >
            <Typography
              sx={{
                fontFamily: monoFont,
                fontSize: 13,
                color: p.text.secondary,
              }}
            >
              {flight?.carrier || '--'} {flight?.flight_number || ''}
            </Typography>
            <Typography
              sx={{
                fontSize: 13,
                color: p.text.disabled,
                fontFamily: 'Inter, sans-serif',
              }}
            >
              {flight?.carrier ? carrierCodeToName(flight.carrier) : '--'}
              {/*  ·  {flight.aircraft} */}
            </Typography>
          </Stack>
          <Stack direction='row' spacing='18px' sx={{ alignItems: 'center' }}>
            <Typography
              sx={{
                fontFamily: serifFont,
                fontSize: 56,
                lineHeight: 1,
                letterSpacing: '-0.03em',
                color: p.text.primary,
              }}
            >
              {flight?.origin || '--'}
            </Typography>
            <svg width='40' height='14' viewBox='0 0 40 14'>
              <line
                x1='0'
                y1='7'
                x2='36'
                y2='7'
                stroke={p.text.secondary}
                strokeWidth='1'
              />
              <polyline
                points='32,3 36,7 32,11'
                fill='none'
                stroke={p.text.secondary}
                strokeWidth='1'
              />
            </svg>
            <Typography
              sx={{
                fontFamily: serifFont,
                fontSize: 56,
                lineHeight: 1,
                letterSpacing: '-0.03em',
                color: p.text.primary,
              }}
            >
              {flight?.dest || '--'}
            </Typography>
          </Stack>
          <Typography
            sx={{
              mt: 1,
              fontSize: 13,
              color: p.text.secondary,
              fontFamily: 'Inter, sans-serif',
            }}
          >
            {/* {flight.from.city} → {flight.to.city} · {flight.scheduled.date} ·
            dep {flight.scheduled.dep} {flight.from.tz} */}
            {getAirportCity(flight?.origin || '') || '-'} →{' '}
            {getAirportCity(flight?.dest || '') || '-'} ·{' '}
            {flight?.scheduled_departure_utc
              ? flight.scheduled_departure_utc + ' UTC'
              : ''}
            {/* · dep {flight.scheduled.dep} {flight.from.tz} */}
          </Typography>
          {prediction && (
            <Chip
              icon={
                <Box
                  component='span'
                  sx={{
                    width: 5,
                    height: 5,
                    borderRadius: '50%',
                    bgcolor: p.primary.main,
                    ml: '10px !important',
                    flexShrink: 0,
                  }}
                />
              }
              label={`live · ${prediction.model_version ? `v${prediction.model_version.slice(0, 6)}` : 'model'} · ${prediction.features_complete ? 'features complete' : 'partial features'}`}
              variant='outlined'
              sx={{
                mt: '14px',
                fontFamily: monoFont,
                fontSize: 10,
                color: p.primary.main,
                borderColor: p.primary.main,
                borderRadius: '3px',
                height: 'auto',
                letterSpacing: '0.06em',
                '& .MuiChip-label': { px: '10px', py: '4px' },
                '& .MuiChip-icon': { color: p.primary.main, mr: 0 },
              }}
            />
          )}
        </Box>

        {/* probability arc */}
        <Box>
          <Typography
            sx={{
              fontFamily: monoFont,
              fontSize: 10,
              color: p.text.disabled,
              letterSpacing: '0.12em',
              textTransform: 'uppercase',
              mb: 1,
            }}
          >
            On-time probability
          </Typography>
          <Box sx={{ position: 'relative', display: 'inline-block' }}>
            <ProbabilityArc prob={onTimeProb ?? 0} size={180} />
            <Box
              sx={{
                position: 'absolute',
                inset: 0,
                display: 'grid',
                placeItems: 'center',
                textAlign: 'center',
              }}
            >
              <Box>
                <Typography
                  sx={{
                    fontFamily: serifFont,
                    fontSize: 44,
                    fontWeight: 400,
                    lineHeight: 1,
                    letterSpacing: '-0.02em',
                    color: p.text.primary,
                  }}
                >
                  {onTimeProb === null ? '--' : (onTimeProb * 100).toFixed(0)}
                  <Box
                    component='span'
                    sx={{ fontSize: 22, color: p.text.secondary }}
                  >
                    %
                  </Box>
                </Typography>
                <Typography
                  sx={{
                    fontSize: 11,
                    color: verdict.color,
                    mt: '6px',
                    fontWeight: 500,
                    fontFamily: 'Inter, sans-serif',
                  }}
                >
                  {verdict.label}
                </Typography>
              </Box>
            </Box>
          </Box>
        </Box>

        {/* expected delay */}
        <Box>
          <Typography
            sx={{
              fontFamily: monoFont,
              fontSize: 10,
              color: p.text.disabled,
              letterSpacing: '0.12em',
              textTransform: 'uppercase',
              mb: 1,
            }}
          >
            Expected delay
          </Typography>
          <Stack direction='row' spacing='6px' sx={{ alignItems: 'baseline' }}>
            <Typography
              sx={{
                fontFamily: serifFont,
                fontSize: 44,
                lineHeight: 1,
                letterSpacing: '-0.02em',
                color: p.text.primary,
              }}
            >
              {delayP50}
            </Typography>
            <Typography
              sx={{
                fontSize: 14,
                color: p.text.secondary,
                fontFamily: 'Inter, sans-serif',
              }}
            >
              min · p50
            </Typography>
          </Stack>
          <Box sx={{ mt: 2 }}>
            {[
              ['p50', `+${delayP50}m`],
              ['p90', `+${delayP90}m`],
              ['cancel', `${(cancelProb * 100).toFixed(1)}%`],
            ].map(([label, val]) => (
              <Stack
                key={label}
                direction='row'
                sx={{
                  py: '4px',
                  borderBottom: `1px solid ${p.custom.lineSoft}`,
                  justifyContent: 'space-between',
                }}
              >
                <Typography
                  sx={{
                    fontSize: 12,
                    color: p.text.secondary,
                    fontFamily: 'Inter, sans-serif',
                  }}
                >
                  {label}
                </Typography>
                <Typography
                  sx={{
                    fontSize: 12,
                    fontFamily: monoFont,
                    color: p.text.secondary,
                  }}
                >
                  {val}
                </Typography>
              </Stack>
            ))}
          </Box>
        </Box>

        {/* model confidence */}
        <Box>
          <Typography
            sx={{
              fontFamily: monoFont,
              fontSize: 10,
              color: p.text.disabled,
              letterSpacing: '0.12em',
              textTransform: 'uppercase',
              mb: 1,
            }}
          >
            Model confidence
          </Typography>
          <Typography
            sx={{
              fontFamily: serifFont,
              fontSize: 44,
              lineHeight: 1,
              letterSpacing: '-0.02em',
              color: p.text.primary,
            }}
          >
            {confidence.toFixed(2)}
          </Typography>
          <Typography
            sx={{
              mt: 2,
              fontSize: 12,
              color: p.text.secondary,
              lineHeight: 1.5,
              fontFamily: 'Inter, sans-serif',
            }}
          >
            {prediction
              ? `Scored live. Brier ~0.06. ${prediction.features_complete ? 'All features resolved.' : 'Some features missing.'}`
              : 'Calibrated against 14d holdout. Brier 0.061.'}
          </Typography>
          <RouterButton
            to='/models' // TODO: add features route ?? /models/:modelId/features ?? or /models/:modelId ?? or /features ??
            variant='outlined'
            sx={{
              mt: '14px',
              color: p.text.primary,
              borderColor: p.divider,
              borderRadius: '2px',
              fontSize: 12,
              textTransform: 'none',
              fontFamily: 'Inter, sans-serif',
              '&:hover': {
                borderColor: p.text.primary,
                bgcolor: 'transparent',
              },
            }}
            endIcon={'→'}
          >
            Inspect features
          </RouterButton>
        </Box>
      </Box>
    </Box>
  );
}

// ─── Feature attribution + route history ─────────────────────────────────────

function AttributionAndHistory({
  flight,
  attributions,
}: {
  flight: ActiveFlight | null;
  attributions?: PredictResponse['attributions'] | null;
}) {
  const p = useTheme().vars.palette;

  // TODO: max height + scroll for attributes or truncate after first 6 ??

  return (
    <Box
      component='section'
      sx={{
        p: '40px 56px',
        display: 'grid',
        gridTemplateColumns: '1.3fr 1fr',
        gap: 4,
        borderBottom: `1px solid ${p.divider}`,
      }}
    >
      <Paper
        variant='outlined'
        sx={{
          bgcolor: p.background.paper,
          borderColor: p.custom.lineSoft,
          borderRadius: '4px',
          p: 3,
        }}
      >
        {attributions?.length ? (
          <>
            <Stack
              direction='row'
              sx={{
                mb: '18px',
                justifyContent: 'space-between',
                alignItems: 'baseline',
              }}
            >
              <Box>
                <Typography
                  sx={{
                    fontFamily: monoFont,
                    fontSize: 10,
                    color: p.text.disabled,
                    letterSpacing: '0.12em',
                    textTransform: 'uppercase',
                  }}
                >
                  Feature attribution
                </Typography>
                <Typography
                  component='h3'
                  sx={{
                    fontFamily: serifFont,
                    fontSize: 22,
                    mt: '6px',
                    fontWeight: 400,
                    letterSpacing: '-0.01em',
                    color: p.text.primary,
                  }}
                >
                  What's driving this prediction
                </Typography>
              </Box>
              <Typography
                sx={{
                  fontSize: 11,
                  color: p.text.disabled,
                  fontFamily: monoFont,
                }}
              >
                SHAP-equivalent · log-odds shift
              </Typography>
            </Stack>

            <Box sx={{ maxHeight: 300, overflowY: 'auto' }}>
              {attributions.map((f, i) => (
                <FactorBar
                  key={i}
                  factor={{
                    name: f.feature,
                    value: f.shap_value,
                    detail: `shap: ${f.shap_value} | feature: ${f.feature_value}`,
                  }}
                />
              ))}
            </Box>
          </>
        ) : (
          <>
            {flight ? (
              <ErrorBoundary
                fallbackRender={({ error }) => (
                  <Typography variant='body2' color='error'>
                    {`Failed to load carrier trend. ${error instanceof Error ? error.message : ''}`}
                  </Typography>
                )}
              >
                <Suspense
                  fallback={
                    <>
                      <Skeleton
                        variant='text'
                        sx={{ fontSize: '0.7rem' }}
                        width={80}
                      />
                      <Skeleton
                        variant='text'
                        sx={{ fontSize: '1.2rem' }}
                        width={140}
                      />
                      <Skeleton
                        variant='rounded'
                        height={160}
                        width='100%'
                        sx={{ mt: 1 }}
                      />
                    </>
                  }
                >
                  <CarrierDelayTrend
                    origin={flight.origin}
                    dest={flight.dest}
                    carrier={flight?.carrier}
                  />
                </Suspense>
              </ErrorBoundary>
            ) : (
              <Typography sx={{ p: 2, textAlign: 'center' }}>
                No flight selected
              </Typography>
            )}
          </>
        )}
      </Paper>
      {/* TODO: replace with real data (add calc to backend for feature attribution or replace with different component) */}

      <Paper
        variant='outlined'
        sx={{
          bgcolor: p.background.paper,
          borderColor: p.custom.lineSoft,
          borderRadius: '4px',
          p: 3,
        }}
      >
        {flight ? (
          <ErrorBoundary
            fallbackRender={({ error }) => {
              console.log('Route history error: ', error);
              return (
                <Typography
                  variant='body2'
                  color='error'
                >{`Failed to load route history. ${error instanceof Error ? error.message : ''}`}</Typography>
              );
            }}
          >
            <Suspense
              fallback={
                <>
                  <Stack
                    direction='row'
                    sx={{
                      mb: '18px',
                      justifyContent: 'space-between',
                      alignItems: 'baseline',
                    }}
                  >
                    <Box>
                      <Skeleton
                        variant='text'
                        sx={{ fontSize: '0.7rem' }}
                        width={60}
                      />
                      <Skeleton
                        variant='text'
                        sx={{ fontSize: '1.2rem' }}
                        width={120}
                      />
                    </Box>
                    <Skeleton
                      variant='text'
                      sx={{ fontSize: '0.9rem' }}
                      width={60}
                    />
                  </Stack>
                  <Skeleton variant='rounded' height={160} width='100%' />
                </>
              }
            >
              <RouteHistoryChart
                origin={flight.origin}
                dest={flight.dest}
                days={14}
              />
            </Suspense>
          </ErrorBoundary>
        ) : (
          <Typography sx={{ textAlign: 'center', p: 2 }}>
            No route selected
          </Typography>
        )}
      </Paper>
    </Box>
  );
}

// ─── Weather + congestion strip ───────────────────────────────────────────────

// TODO: congestion api: https://airlabs.co/docs/delays
function WeatherCongestionStrip({ flight }: { flight: ActiveFlight }) {
  const p = useTheme().vars.palette;

  return (
    <Box
      component='section'
      sx={{
        p: '40px 56px',
        display: 'grid',
        gridTemplateColumns: 'repeat(4, 1fr)',
        gap: 3,
        borderBottom: `1px solid ${p.divider}`,
      }}
    >
      <ErrorBoundary
        fallback={
          <StatCard
            label='Origin Weather'
            code={flight?.origin || '-'} // @ts-ignore
            value={
              <Typography variant='body2' color='error'>
                Error loading weather
              </Typography>
            }
            subtitle={`wind -- · vis --sm`}
            spark={[]}
            color={p.text.primary}
            fill={p.custom.lineSoft}
          />
        }
      >
        <Suspense
          fallback={
            <StatCard
              label='Origin Weather'
              code={flight.origin || '-'} // @ts-ignore
              value={<Skeleton />}
              subtitle={`wind -- · vis --sm`}
              spark={[]}
              color={p.success.main}
              fill={p.custom.lineSoft}
            />
          }
        >
          <WeatherCard
            label='Origin weather'
            code={flight.origin}
            color={p.success.main}
          />
        </Suspense>
      </ErrorBoundary>
      <ErrorBoundary
        fallback={
          <StatCard
            label='Destination Weather'
            code={flight.dest || '-'} // @ts-ignore
            value={
              <Typography variant='body2' color='error'>
                Error loading weather
              </Typography>
            }
            subtitle={`wind -- · vis --sm`}
            spark={[]}
            color={p.text.primary}
            fill={p.custom.lineSoft}
          />
        }
      >
        <Suspense
          fallback={
            <StatCard
              label='Destination weather'
              code={flight.dest || '-'} // @ts-ignore
              value={<Skeleton />}
              subtitle={`wind -- · vis --sm`}
              spark={[]}
              color={p.success.main}
              fill={p.custom.lineSoft}
            />
          }
        >
          <WeatherCard
            label='Destination weather'
            code={flight.dest || '-'}
            color={p.warning.main}
          />
        </Suspense>
      </ErrorBoundary>
      <ErrorBoundary
        fallback={
          <StatCard
            label='Origin Congestion'
            code={flight.dest || '-'} // @ts-ignore
            value={
              <Typography variant='body2' color='error'>
                Error loading congestion
              </Typography>
            }
            subtitle={`taxi --m · queue --`}
            spark={[]}
            color={p.text.primary}
            fill={p.custom.lineSoft}
          />
        }
      >
        <Suspense
          fallback={
            <StatCard
              label='Origin Congestion'
              code={flight.origin || '-'} // @ts-ignore
              value={<Skeleton />}
              subtitle={`taxi --m · queue --`}
              spark={[]}
              color={p.text.primary}
              fill={p.custom.lineSoft}
            />
          }
        >
          <AirportCongestionCard
            delay={30}
            type='departures'
            dep_iata={flight.origin || '-'}
            label='Origin Congestion'
            spark={[12, 14, 16, 15, 18, 17, 19, 18]}
            color={p.text.primary}
          />
        </Suspense>
      </ErrorBoundary>
      <ErrorBoundary
        fallback={
          <StatCard
            label='Destination Congestion'
            code={flight.dest || '-'} // @ts-ignore
            value={
              <Typography variant='body2' color='error'>
                Error loading congestion
              </Typography>
            }
            subtitle={`taxi --m · queue --`}
            spark={[]}
            color={p.text.primary}
            fill={p.custom.lineSoft}
          />
        }
      >
        <Suspense
          fallback={
            <StatCard
              label='Destination Congestion'
              code={flight.dest || '-'} // @ts-ignore
              value={<Skeleton />}
              subtitle={`taxi --m · queue --`}
              spark={[]}
              color={p.text.primary}
              fill={p.custom.lineSoft}
            />
          }
        >
          <AirportCongestionCard
            delay={30}
            type='arrivals'
            dep_iata={flight.dest || '-'}
            label='Destination Congestion'
            color={p.text.primary}
            spark={[10, 12, 15, 18, 22, 26, 28, 30]}
          />
        </Suspense>
      </ErrorBoundary>

      {/* {cards.map((c, i) => (
        <StatCard
          key={`stat-${i}`}
          label={c.l}
          code={c.code}
          value={c.top}
          subtitle={c.sub}
          spark={c.spark}
          color={c.col}
          fill={p.custom.lineSoft}
        />
      ))} */}
    </Box>
  );
}

function WeatherCard({
  code,
  label,
  color,
}: {
  code: string;
  label: string;
  color: string;
}) {
  const p = useTheme().vars.palette;
  const { data } = useSuspenseQuery(
    aviationWeatherOptions('metar', iataToIcao(code)),
  );

  // TODO: wire up taf for sparkline ??
  // const { data: taf } = useAviationWeather('taf', iataToIcao(code));

  return (
    <StatCard
      label={label}
      code={code} // @ts-ignore
      value={`${data?.fltCat} · ${data?.cover}`} // @ts-ignore
      subtitle={`wind ${data?.wdir || '-'}@${data?.wspd || '-'} 240@8 · vis ${data?.visib || '-'}sm`}
      spark={[82, 84, 86, 85, 88, 90, 89, 91]}
      color={color}
      fill={p.custom.lineSoft}
    />
  );
}

interface AirportCongestionCardParams extends GetDelaysOptions {
  label: string;
  spark: number[];
  color: string;
}

function AirportCongestionCard({
  label,
  spark,
  color,
  ...options
}: AirportCongestionCardParams) {
  const p = useTheme().vars.palette;
  const { data } = useSuspenseQuery(delayOptions(options));
  // console.log(label, data);

  const code = options.dep_iata || options.arr_iata || '';

  const [avgDelayMins, col, congestionLevel] = useMemo(() => {
    const totalMins = data.reduce((acc, cur) => acc + cur.delayed, 0);

    const avgDelayMins = Math.floor(totalMins / (data.length || 1));
    const col =
      avgDelayMins < 60
        ? p.success.main
        : avgDelayMins < 120
          ? p.text.primary
          : p.error.main;
    const congestionLevel =
      avgDelayMins < 60 ? 'Light' : avgDelayMins < 120 ? 'Normal' : 'Elevated';

    return [avgDelayMins, col, congestionLevel] as const;
  }, [data]); // TODO: format as days hours minutes

  // TODO: find place in queue from index in data ??

  return (
    <StatCard
      label={label}
      code={code}
      value={congestionLevel}
      subtitle={`taxi ${avgDelayMins}m · queue --`}
      spark={spark}
      color={col} // TODO: calc color from delay
      fill={p.custom.lineSoft}
    />
  );
}

// ─── Network + airline comparison ────────────────────────────────────────────

function NetworkAndAirline({ flight }: { flight: ActiveFlight | null }) {
  const p = useTheme().vars.palette;

  return (
    <Box
      component='section'
      sx={{
        p: '40px 56px',
        display: 'grid',
        gridTemplateColumns: '1.3fr 1fr',
        gap: 4,
        borderBottom: `1px solid ${p.divider}`,
      }}
    >
      <Paper
        variant='outlined'
        sx={{
          bgcolor: p.background.paper,
          borderColor: p.custom.lineSoft,
          borderRadius: '4px',
          p: 3,
        }}
      >
        <Box sx={{ mb: '18px' }}>
          <Typography
            sx={{
              fontFamily: monoFont,
              fontSize: 10,
              color: p.text.disabled,
              letterSpacing: '0.12em',
              textTransform: 'uppercase',
            }}
          >
            Network · simulated
          </Typography>
          <Typography
            component='h3'
            sx={{
              fontFamily: serifFont,
              fontSize: 22,
              mt: '6px',
              fontWeight: 400,
              letterSpacing: '-0.01em',
              color: p.text.primary,
            }}
          >
            Average delays across the system
          </Typography>
        </Box>
        <ErrorBoundary
          fallback={
            <Typography color='error' sx={{ fontFamily: monoFont }}>
              Failed to load network map
            </Typography>
          }
        >
          <Suspense
            fallback={<Skeleton variant='rounded' height={290} width='100%' />}
          >
            <NetworkMap height={290} />
          </Suspense>
        </ErrorBoundary>
      </Paper>

      <Paper
        variant='outlined'
        sx={{
          bgcolor: p.background.paper,
          borderColor: p.custom.lineSoft,
          borderRadius: '4px',
          p: 3,
        }}
      >
        <Box sx={{ mb: '18px' }}>
          <Typography
            sx={{
              fontFamily: monoFont,
              fontSize: 10,
              color: p.text.disabled,
              letterSpacing: '0.12em',
              textTransform: 'uppercase',
            }}
          >
            Carrier comparison
          </Typography>
          <Typography
            component='h3'
            sx={{
              fontFamily: serifFont,
              fontSize: 22,
              mt: '6px',
              fontWeight: 400,
              letterSpacing: '-0.01em',
              color: p.text.primary,
            }}
          >
            {flight?.origin || '-'} → {flight?.dest || '-'} · 30d OTP
          </Typography>
        </Box>
        {flight ? (
          <ErrorBoundary
            fallback={
              <Typography
                color='error'
                variant='body2'
                sx={{ textAlign: 'center' }}
              >
                Failed to load Carrier Comparison
              </Typography>
            }
          >
            <Suspense
              fallback={
                <Box
                  sx={{
                    display: 'grid',
                    gridTemplateColumns: '20px 1fr 80px 56px 50px',
                    gap: '12px',
                    alignItems: 'center',
                    py: '10px',
                    borderBottom: `1px solid ${p.custom.lineSoft}`,
                  }}
                >
                  <Skeleton variant='circular' height={18} width={18} />
                  <Skeleton variant='rounded' height={10} width={60} />
                  <Skeleton
                    variant='rounded'
                    height={8}
                    width={60}
                    sx={{ mx: 'auto' }}
                  />
                  <Skeleton variant='rounded' height={14} width={28} />
                  <Skeleton variant='rounded' height={14} width={28} />
                </Box>
              }
            >
              <CarrierComparison
                currentCarrier={flight.carrier}
                origin={flight.origin}
                dest={flight.dest}
                days={30}
              />
            </Suspense>
          </ErrorBoundary>
        ) : (
          <Typography>TODO: fallback when flight is null</Typography>
        )}
      </Paper>
    </Box>
  );
}

// ─── Root component ───────────────────────────────────────────────────────────

function Index() {
  const p = useTheme().vars.palette;
  const [prediction, setPrediction] = useState<PredictResponse | null>(null);

  const { data: sampleFlights } = useSuspenseQuery(sampleFlightOptions(4));
  // track common data between prediction user input and sample flight button group
  const [flight, setFlight] = useState<ActiveFlight | null>(
    sampleFlights?.[0] ? sampleToActive(sampleFlights[0]) : null,
  );

  const onTimeProb = prediction
    ? 1 - prediction.delay_probability
    : (flight?.baseline_ontime_prob ?? null);
  console.log(onTimeProb, 'on time prob', prediction?.delay_probability);

  const verdict = useMemo(() => {
    if (onTimeProb === null) return { label: 'unknown', color: p.grey[500] };
    if (onTimeProb >= 0.85)
      return { label: 'Likely on time', color: p.success.main };
    if (onTimeProb >= 0.65)
      return { label: 'Mild delay risk', color: p.warning.main };
    return { label: 'Elevated delay risk', color: p.error.main };
  }, [onTimeProb]);

  return (
    <Box sx={{ bgcolor: 'background.default', color: 'text.primary' }}>
      <HeroSection
        sampleFlights={sampleFlights}
        flight={flight}
        onPickFlight={(f) => {
          setFlight(f);
          setPrediction(null);
        }}
        onPredict={(p, ctx) => {
          setPrediction(p);
          setFlight(ctx);
        }}
      />
      <PredictionHeadline
        flight={flight}
        onTimeProb={onTimeProb}
        verdict={verdict}
        prediction={prediction}
      />
      <AttributionAndHistory
        flight={flight}
        attributions={prediction?.attributions || null}
      />
      {/* TODO: handle fallback rendering when flight is null (skipToken type issue) */}
      {flight ? <WeatherCongestionStrip flight={flight} /> : null}

      <NetworkAndAirline flight={flight} />
    </Box>
  );
}
