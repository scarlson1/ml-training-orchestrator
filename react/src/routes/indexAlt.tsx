import Avatar from '@mui/material/Avatar';
import Box from '@mui/material/Box';
import Button from '@mui/material/Button';
import Chip from '@mui/material/Chip';
import LinearProgress from '@mui/material/LinearProgress';
import Link from '@mui/material/Link';
import Paper from '@mui/material/Paper';
import Skeleton from '@mui/material/Skeleton';
import Stack from '@mui/material/Stack';
import ToggleButton from '@mui/material/ToggleButton';
import ToggleButtonGroup from '@mui/material/ToggleButtonGroup';
import Typography from '@mui/material/Typography';
import { useMutation, useSuspenseQuery } from '@tanstack/react-query';
import { createFileRoute } from '@tanstack/react-router';
import { Suspense, useEffect, useMemo, useState } from 'react';
import { ErrorBoundary } from 'react-error-boundary';
import { apiFetch } from '~/api';
import { Globe } from '~/components/Globe';
import { StatCard } from '~/components/StatCard';
import { monoFont, serifFont } from '~/config/themePrimitives';
import { useResolvedMode } from '~/hooks/useResolvedMode';

export const Route = createFileRoute('/indexAlt')({
  component: IndexAlt,
  loader: ({ context: { queryClient } }) =>
    Promise.allSettled([
      queryClient.prefetchQuery({
        queryKey: ['predictions', 'today'],
        queryFn: () => apiFetch('/api/predictions/today').then((r) => r.json()),
        staleTime: 60 * 60 * 1000,
      }),
      queryClient.prefetchQuery({
        queryKey: ['drift', 'summary'],
        queryFn: () => apiFetch('/api/drift/summary').then((r) => r.json()),
        staleTime: 60 * 60 * 1000,
      }),
    ]),
});

// ─── API types ────────────────────────────────────────────────────────────────

interface PredictionSummary {
  n_flights_today: number;
  positive_rate_today: number | null;
  model_version: string | null;
  days_since_retrain: number | null;
  model_loaded_at: string;
}

interface DriftSummary {
  psi_breaches: number;
  n_features: number;
}

interface PredictResponse {
  flight_id: string;
  predicted_is_delayed: boolean;
  delay_probability: number;
  model_name: string;
  model_version: string;
  features_complete: boolean;
}

// ─── Design tokens ────────────────────────────────────────────────────────────

interface Tokens {
  bg: string;
  panel: string;
  panelAlt: string;
  line: string;
  lineSoft: string;
  ink: string;
  inkSoft: string;
  inkMuted: string;
  accent: string;
  good: string;
  warn: string;
  bad: string;
  chipBg: string;
}
// TODO: integrate into theme
const TOKENS: { light: Tokens; dark: Tokens } = {
  light: {
    bg: '#FBFAF7',
    panel: '#FFFFFF',
    panelAlt: '#F4F2EC',
    line: '#E7E3D9',
    lineSoft: '#EFEBE0',
    ink: '#1A1A18',
    inkSoft: '#5C5A52',
    inkMuted: '#8F8C82',
    accent: '#2B6BFF',
    good: '#1F7A3F',
    warn: '#B5701B',
    bad: '#B23B2A',
    chipBg: '#F1EEE5',
  },
  dark: {
    bg: '#0F0F0E',
    panel: '#151514',
    panelAlt: '#1A1A18',
    line: '#26251F',
    lineSoft: '#1F1E18',
    ink: '#F4F1E8',
    inkSoft: '#A09D90',
    inkMuted: '#6D6A60',
    accent: '#7DA8FF',
    good: '#67C28E',
    warn: '#E8B065',
    bad: '#E88370',
    chipBg: '#1C1B15',
  },
};

// ─── Mock data ────────────────────────────────────────────────────────────────

interface Factor {
  name: string;
  value: number;
  detail: string;
}

interface Flight {
  id: string;
  airline: string;
  code: string;
  number: string;
  from: { code: string; city: string; tz: string };
  to: { code: string; city: string; tz: string };
  scheduled: { dep: string; arr: string; date: string };
  aircraft: string;
  onTimeProb: number;
  delayMin: { p50: number; p90: number };
  cancelProb: number;
  factors: Factor[];
  history: number[];
}

const FLIGHTS: Flight[] = [
  {
    id: 'AX2104',
    airline: 'Axiom Air',
    code: 'AX',
    number: '2104',
    from: { code: 'SFO', city: 'San Francisco', tz: 'PT' },
    to: { code: 'JFK', city: 'New York', tz: 'ET' },
    scheduled: { dep: '07:45', arr: '16:18', date: 'Thu, 1 May' },
    aircraft: 'A321neo',
    onTimeProb: 0.83,
    delayMin: { p50: 6, p90: 24 },
    cancelProb: 0.012,
    factors: [
      {
        name: 'Origin congestion',
        value: -0.12,
        detail: 'SFO ground stop risk · low',
      },
      {
        name: 'Destination weather',
        value: -0.08,
        detail: 'JFK · scattered TS after 18:00z',
      },
      {
        name: 'Carrier on-time history',
        value: +0.21,
        detail: 'AX route 7-day OTP 88%',
      },
      {
        name: 'Aircraft rotation',
        value: +0.05,
        detail: 'Inbound from PDX · on time',
      },
      { name: 'Day of week', value: -0.02, detail: 'Thursday · neutral' },
      { name: 'Time of day', value: +0.07, detail: 'Morning bank · favorable' },
    ],
    history: [82, 76, 88, 91, 84, 79, 86, 90, 87, 83, 78, 85, 89, 92],
  },
  {
    id: 'NB418',
    airline: 'Northbound',
    code: 'NB',
    number: '418',
    from: { code: 'ORD', city: 'Chicago', tz: 'CT' },
    to: { code: 'LHR', city: 'London', tz: 'GMT' },
    scheduled: { dep: '20:15', arr: '10:05', date: 'Thu, 1 May' },
    aircraft: 'B787-9',
    onTimeProb: 0.61,
    delayMin: { p50: 22, p90: 78 },
    cancelProb: 0.024,
    factors: [
      {
        name: 'Origin congestion',
        value: -0.28,
        detail: 'ORD evening bank · high traffic',
      },
      {
        name: 'Destination weather',
        value: -0.04,
        detail: 'LHR · light rain, CAT I',
      },
      {
        name: 'Carrier on-time history',
        value: +0.11,
        detail: 'NB transatl. 30-day OTP 71%',
      },
      {
        name: 'Aircraft rotation',
        value: -0.14,
        detail: 'Inbound LAX delayed 18m',
      },
      { name: 'Day of week', value: 0, detail: 'Thursday · neutral' },
      {
        name: 'Time of day',
        value: -0.09,
        detail: 'Evening departure · congested',
      },
    ],
    history: [62, 58, 71, 65, 59, 63, 70, 55, 68, 72, 60, 57, 64, 66],
  },
  {
    id: 'MR906',
    airline: 'Meridian',
    code: 'MR',
    number: '906',
    from: { code: 'ATL', city: 'Atlanta', tz: 'ET' },
    to: { code: 'DEN', city: 'Denver', tz: 'MT' },
    scheduled: { dep: '13:30', arr: '15:02', date: 'Thu, 1 May' },
    aircraft: 'B737-800',
    onTimeProb: 0.42,
    delayMin: { p50: 38, p90: 112 },
    cancelProb: 0.061,
    factors: [
      { name: 'Origin congestion', value: -0.09, detail: 'ATL · normal' },
      {
        name: 'Destination weather',
        value: -0.34,
        detail: 'DEN · convective SIGMET, gusts 38kt',
      },
      {
        name: 'Carrier on-time history',
        value: +0.04,
        detail: 'MR 7-day OTP 74%',
      },
      {
        name: 'Aircraft rotation',
        value: -0.11,
        detail: 'Inbound MCO · delayed 22m',
      },
      { name: 'Day of week', value: -0.02, detail: 'Thursday · neutral' },
      {
        name: 'Time of day',
        value: -0.06,
        detail: 'Afternoon thunderstorm window',
      },
    ],
    history: [54, 48, 62, 41, 38, 45, 52, 39, 44, 50, 46, 42, 36, 49],
  },
  {
    id: 'PE12',
    airline: 'Pacific',
    code: 'PE',
    number: '12',
    from: { code: 'NRT', city: 'Tokyo', tz: 'JST' },
    to: { code: 'LAX', city: 'Los Angeles', tz: 'PT' },
    scheduled: { dep: '17:00', arr: '10:35', date: 'Thu, 1 May' },
    aircraft: 'B777-300ER',
    onTimeProb: 0.91,
    delayMin: { p50: 2, p90: 11 },
    cancelProb: 0.004,
    factors: [
      {
        name: 'Origin congestion',
        value: +0.08,
        detail: 'NRT · light traffic',
      },
      {
        name: 'Destination weather',
        value: +0.06,
        detail: 'LAX · clear, marine layer 06z',
      },
      {
        name: 'Carrier on-time history',
        value: +0.23,
        detail: 'PE NRT-LAX 30-day OTP 93%',
      },
      {
        name: 'Aircraft rotation',
        value: +0.09,
        detail: 'Aircraft on stand 4h+',
      },
      { name: 'Day of week', value: +0.01, detail: 'Thursday · neutral' },
      {
        name: 'Time of day',
        value: +0.04,
        detail: 'Evening departure · favorable',
      },
    ],
    history: [88, 92, 91, 89, 94, 90, 93, 95, 91, 90, 92, 94, 89, 93],
  },
];

const AIRLINE_COMPARISON = [
  { airline: 'Pacific', code: 'PE', otp: 0.93, avgDelay: 4 },
  { airline: 'Axiom Air', code: 'AX', otp: 0.86, avgDelay: 8 },
  { airline: 'Skybridge', code: 'SB', otp: 0.79, avgDelay: 14 },
  { airline: 'Northbound', code: 'NB', otp: 0.71, avgDelay: 22 },
  { airline: 'Meridian', code: 'MR', otp: 0.68, avgDelay: 28 },
];

const NETWORK_AIRPORTS = [
  { code: 'SFO', x: 0.1, y: 0.46, delay: 6, status: 'green' as const },
  { code: 'LAX', x: 0.13, y: 0.58, delay: 11, status: 'green' as const },
  { code: 'SEA', x: 0.13, y: 0.2, delay: 4, status: 'green' as const },
  { code: 'DEN', x: 0.36, y: 0.49, delay: 41, status: 'red' as const },
  { code: 'DFW', x: 0.46, y: 0.7, delay: 22, status: 'amber' as const },
  { code: 'MSP', x: 0.5, y: 0.28, delay: 12, status: 'amber' as const },
  { code: 'ORD', x: 0.58, y: 0.4, delay: 28, status: 'red' as const },
  { code: 'ATL', x: 0.66, y: 0.66, delay: 9, status: 'green' as const },
  { code: 'MIA', x: 0.78, y: 0.86, delay: 11, status: 'green' as const },
  { code: 'BOS', x: 0.86, y: 0.3, delay: 19, status: 'amber' as const },
  { code: 'JFK', x: 0.85, y: 0.36, delay: 24, status: 'amber' as const },
  { code: 'EWR', x: 0.83, y: 0.37, delay: 32, status: 'red' as const },
  { code: 'DCA', x: 0.8, y: 0.46, delay: 11, status: 'green' as const },
];

// ─── Small atoms ──────────────────────────────────────────────────────────────

function ProbabilityArc({
  prob,
  t,
  size = 200,
}: {
  prob: number;
  t: Tokens;
  size?: number;
}) {
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
        stroke={t.lineSoft}
        strokeWidth='2'
        fill='none'
      />
      <path
        d={`M ${x1} ${y1} A ${r} ${r} 0 ${largeArc} 1 ${x2} ${y2}`}
        stroke={t.ink}
        strokeWidth='2'
        fill='none'
        strokeLinecap='round'
      />
      {[0, 0.25, 0.5, 0.75, 1].map((p, i) => {
        const a = startA + total * p;
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
            stroke={t.line}
            strokeWidth='1'
          />
        );
      })}
    </svg>
  );
}

function FactorBar({ factor, t }: { factor: Factor; t: Tokens }) {
  const v = factor.value;
  const pct = Math.min(Math.abs(v) / 0.4, 1) * 50;
  const positive = v >= 0;

  return (
    <Box
      sx={{
        display: 'flex',
        alignItems: 'center',
        gap: 2,
        py: '10px',
        borderBottom: `1px solid ${t.lineSoft}`,
      }}
    >
      <Box sx={{ flex: '1 1 0', minWidth: 0, overflow: 'hidden' }}>
        <Typography
          sx={{
            fontSize: 13,
            color: t.ink,
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
            color: t.inkMuted,
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
          bgcolor: t.lineSoft,
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
            bgcolor: t.line,
          }}
        />
        <Box
          sx={{
            position: 'absolute',
            top: 0,
            bottom: 0,
            left: positive ? '50%' : `${50 - pct}%`,
            width: `${pct}%`,
            bgcolor: positive ? t.good : t.bad,
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
          color: positive ? t.good : t.bad,
          whiteSpace: 'nowrap',
        }}
      >
        {positive ? '+' : ''}
        {(v * 100).toFixed(0)}
      </Typography>
    </Box>
  );
}

// ─── Network map ──────────────────────────────────────────────────────────────

function NetworkMap({ t, height = 280 }: { t: Tokens; height?: number }) {
  const statusColor = (s: 'green' | 'amber' | 'red') =>
    s === 'red' ? t.bad : s === 'amber' ? t.warn : t.good;
  return (
    <Paper
      variant='outlined'
      sx={{
        position: 'relative',
        width: '100%',
        height,
        bgcolor: t.panelAlt,
        borderColor: t.lineSoft,
        borderRadius: '4px',
        overflow: 'hidden',
      }}
    >
      <svg
        viewBox='0 0 100 60'
        width='100%'
        height='100%'
        preserveAspectRatio='xMidYMid meet'
      >
        {[10, 20, 30, 40, 50].map((y) => (
          <line
            key={y}
            x1='0'
            x2='100'
            y1={y}
            y2={y}
            stroke={t.line}
            strokeWidth='0.08'
          />
        ))}
        {[20, 40, 60, 80].map((x) => (
          <line
            key={x}
            x1={x}
            x2={x}
            y1='0'
            y2='60'
            stroke={t.line}
            strokeWidth='0.08'
          />
        ))}
        {[
          ['SFO', 'JFK'],
          ['LAX', 'ORD'],
          ['ATL', 'BOS'],
          ['DEN', 'ATL'],
          ['ORD', 'DCA'],
          ['SEA', 'MIA'],
          ['DFW', 'JFK'],
        ].map(([ac, bc], i) => {
          const A = NETWORK_AIRPORTS.find((n) => n.code === ac);
          const B = NETWORK_AIRPORTS.find((n) => n.code === bc);
          if (!A || !B) return null;
          const x1 = A.x * 100,
            y1 = A.y * 60;
          const x2 = B.x * 100,
            y2 = B.y * 60;
          const mx = (x1 + x2) / 2,
            my = (y1 + y2) / 2 - 8;
          return (
            <path
              key={i}
              d={`M ${x1} ${y1} Q ${mx} ${my} ${x2} ${y2}`}
              stroke={t.line}
              strokeWidth='0.15'
              fill='none'
              strokeDasharray='0.6 0.6'
            />
          );
        })}
        {NETWORK_AIRPORTS.map((n) => {
          const col = statusColor(n.status);
          return (
            <g key={n.code}>
              <circle
                cx={n.x * 100}
                cy={n.y * 60}
                r={1.2 + n.delay / 60}
                fill={col}
                opacity='0.18'
              />
              <circle cx={n.x * 100} cy={n.y * 60} r='0.7' fill={col} />
              <text
                x={n.x * 100 + 1.4}
                y={n.y * 60 + 0.5}
                fontSize='1.6'
                fill={t.inkSoft}
                fontFamily={monoFont}
              >
                {n.code}
              </text>
            </g>
          );
        })}
      </svg>
      <Typography
        sx={{
          position: 'absolute',
          top: 10,
          left: 10,
          fontSize: 10,
          color: t.inkMuted,
          fontFamily: monoFont,
          letterSpacing: '0.06em',
          textTransform: 'uppercase',
        }}
      >
        Live · simulated
      </Typography>
      <Stack
        direction='row'
        spacing='10px'
        sx={{ position: 'absolute', bottom: 10, right: 12 }}
      >
        {[
          [t.good, '<15m'],
          [t.warn, '15–30m'],
          [t.bad, '30m+'],
        ].map(([col, label]) => (
          <Stack
            key={label}
            direction='row'
            spacing='4px'
            sx={{ alignItems: 'center' }}
          >
            <Box
              sx={{
                width: 6,
                height: 6,
                bgcolor: col,
                borderRadius: '50%',
                flexShrink: 0,
              }}
            />
            <Typography
              sx={{ fontSize: 10, color: t.inkSoft, fontFamily: monoFont }}
            >
              {label}
            </Typography>
          </Stack>
        ))}
      </Stack>
    </Paper>
  );
}

// ─── Airline comparison ───────────────────────────────────────────────────────

function AirlineComparison({
  t,
  currentCode,
}: {
  t: Tokens;
  currentCode: string;
}) {
  return (
    <Box>
      {AIRLINE_COMPARISON.map((a) => {
        const isCurrent = a.code === currentCode;
        return (
          <Box
            key={a.code}
            sx={{
              display: 'grid',
              gridTemplateColumns: '20px 1fr 80px 56px 50px',
              gap: '12px',
              alignItems: 'center',
              py: '10px',
              borderBottom: `1px solid ${t.lineSoft}`,
              opacity: isCurrent ? 1 : 0.78,
            }}
          >
            <Avatar
              variant='rounded'
              sx={{
                width: 18,
                height: 18,
                borderRadius: '2px',
                bgcolor: t.chipBg,
                color: t.inkSoft,
                fontFamily: monoFont,
                fontSize: 9,
                fontWeight: 600,
              }}
            >
              {a.code}
            </Avatar>
            <Stack direction='row' sx={{ alignItems: 'center' }}>
              <Typography
                sx={{
                  fontSize: 13,
                  color: t.ink,
                  fontWeight: isCurrent ? 600 : 400,
                }}
              >
                {a.airline}
              </Typography>
              {isCurrent && (
                <Chip
                  label='current'
                  size='small'
                  sx={{
                    ml: 1,
                    height: 16,
                    fontSize: 10,
                    color: t.accent,
                    fontFamily: monoFont,
                    letterSpacing: '0.08em',
                    textTransform: 'uppercase',
                    bgcolor: 'transparent',
                    border: 'none',
                    '& .MuiChip-label': { px: '4px' },
                  }}
                />
              )}
            </Stack>
            <LinearProgress
              variant='determinate'
              value={a.otp * 100}
              sx={{
                height: 4,
                borderRadius: '1px',
                bgcolor: t.lineSoft,
                '& .MuiLinearProgress-bar': {
                  bgcolor: t.ink,
                  borderRadius: '1px',
                },
              }}
            />
            <Typography
              sx={{
                fontFamily: monoFont,
                fontSize: 12,
                color: t.ink,
                textAlign: 'right',
              }}
            >
              {(a.otp * 100).toFixed(0)}%
            </Typography>
            <Typography
              sx={{
                fontFamily: monoFont,
                fontSize: 12,
                color: t.inkSoft,
                textAlign: 'right',
              }}
            >
              {a.avgDelay}m
            </Typography>
          </Box>
        );
      })}
    </Box>
  );
}

// ─── Flight switcher ──────────────────────────────────────────────────────────

function FlightSwitcher({
  flights,
  current,
  onPick,
  t,
}: {
  flights: Flight[];
  current: Flight;
  onPick: (f: Flight) => void;
  t: Tokens;
}) {
  return (
    <ToggleButtonGroup
      value={current.id}
      exclusive
      onChange={(_, newId: string | null) => {
        if (newId != null) {
          const f = flights.find((fl) => fl.id === newId);
          if (f) onPick(f);
        }
      }}
      sx={{
        display: 'flex',
        flexWrap: 'wrap',
        gap: 1,
        '& .MuiToggleButtonGroup-grouped': {
          border: `1px solid ${t.line} !important`,
          borderRadius: '2px !important',
          color: t.ink,
          fontFamily: monoFont,
          fontSize: 11,
          letterSpacing: '0.04em',
          px: '10px',
          py: '6px',
          textTransform: 'none',
          '&.Mui-selected': {
            bgcolor: t.ink,
            color: t.bg,
            borderColor: `${t.ink} !important`,
            '&:hover': { bgcolor: t.inkSoft },
          },
        },
      }}
    >
      {flights.map((f) => (
        <ToggleButton key={f.id} value={f.id}>
          {f.code} {f.number} · {f.from.code}→{f.to.code}
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
  t,
}: {
  title: string;
  value: string | number | null | undefined;
  subtitle: string;
  t: Tokens;
}) {
  return (
    <Box sx={{ borderLeft: `1px solid ${t.line}`, pl: '14px' }}>
      <Typography
        sx={{
          fontFamily: monoFont,
          fontSize: 9,
          color: t.inkMuted,
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
          color: t.ink,
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
          color: t.inkMuted,
          mt: '2px',
        }}
      >
        {subtitle}
      </Typography>
    </Box>
  );
}

function KpiStrip({ t }: { t: Tokens }) {
  const { data: pred } = useSuspenseQuery({
    queryKey: ['predictions', 'today'],
    queryFn: () =>
      apiFetch('/api/predictions/today').then(
        (r) => r.json() as Promise<PredictionSummary>,
      ),
    staleTime: 60 * 60 * 1000,
    retry: false,
  });
  const { data: drift } = useSuspenseQuery({
    queryKey: ['drift', 'summary'],
    queryFn: () =>
      apiFetch('/api/drift/summary').then(
        (r) => r.json() as Promise<DriftSummary>,
      ),
    staleTime: 60 * 60 * 1000,
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
      l: 'In flight today',
      v: nFlights ? nFlights.toLocaleString() : '—',
      s: 'flights scored',
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
    <Stack direction='row' spacing='28px' sx={{ mt: '28px' }}>
      {kpis.map((k, i) => (
        <KpiItem
          key={`kpi-${i}`}
          title={k.l}
          value={k.v}
          subtitle={k.s}
          t={t}
        />
      ))}
    </Stack>
  );
}

const KPI_FALLBACK = [
  { l: 'In flight today', v: '—', s: 'flights scored' },
  { l: 'Predicted on-time', v: '—', s: '- live rate' },
  { l: 'PSI alerts', v: '—', s: 'unknown' },
  { l: 'Model version', v: '—', s: 'champion' },
];

// ─── Hero section ─────────────────────────────────────────────────────────────

interface PredictBody {
  flight_id: string; // composite: {carrier}{flight_number}_{date}_{dep_time}
  origin: string;
  dest: string;
  carrier: string;
  route_key: string;
  tail_number?: string;
}

function getFlightCompositeId(flight: Flight) {
  return `${flight.code}${flight.number}_${new Date().toISOString().slice(0, 10).replace(/-/g, '')}`; // → "AA123_20240503"
}

interface HeroProps {
  t: Tokens;
  isDark: boolean;
  flight: Flight;
  onPickFlight: (f: Flight) => void;
  onPredict: (p: PredictResponse) => void;
}

function HeroSection({
  t,
  isDark,
  flight,
  onPickFlight,
  onPredict,
}: HeroProps) {
  const { mutate: predict, isPending } = useMutation({
    mutationFn: async (body: PredictBody) => {
      const res = await apiFetch('/predict', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      });
      if (res.ok) {
        const data = (await res.json()) as PredictResponse;
        return data;
      } else {
        throw new Error();
      }
    },
    onSuccess: (data) => {
      onPredict(data);
    },
  });

  const handlePredict = () => {
    const body: PredictBody = {
      flight_id: getFlightCompositeId(flight),
      origin: flight.from.code,
      dest: flight.to.code,
      carrier: flight.code,
      route_key: `${flight.from.code}-${flight.to.code}`,
    };
    predict(body);
  };

  return (
    <Box
      component='section'
      sx={{
        position: 'relative',
        p: '56px 56px 36px',
        borderBottom: `1px solid ${t.line}`,
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
                bgcolor: t.good,
                mr: 1,
                flexShrink: 0,
              }}
            />
            <Typography
              sx={{
                fontFamily: monoFont,
                fontSize: 11,
                color: t.inkMuted,
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
              color: t.ink,
            }}
          >
            Know whether
            <br />
            the flight{' '}
            <Box component='em' sx={{ fontStyle: 'italic', color: t.inkSoft }}>
              will
            </Box>{' '}
            hold —<br />
            before it pushes back.
          </Typography>

          <Typography
            sx={{
              mt: '20px',
              fontSize: 15,
              color: t.inkSoft,
              maxWidth: 480,
              lineHeight: 1.55,
              fontFamily: 'Inter, sans-serif',
              flexGrow: 1,
            }}
          >
            Holdline's ensemble model fuses METAR, TAF, ground-stop bulletins,
            fleet rotation, and 9 years of carrier OTP data into a calibrated
            probability — refreshed every 90 seconds.
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
                        t={t}
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
                      sx={{ borderLeft: `1px solid ${t.line}`, pl: '14px' }}
                    >
                      <Skeleton width={72} height={10} />
                      <Skeleton width={40} height={30} sx={{ mt: '6px' }} />
                      <Skeleton width={56} height={10} sx={{ mt: '6px' }} />
                    </Box>
                  ))}
                </Stack>
              }
            >
              <KpiStrip t={t} />
            </Suspense>
          </ErrorBoundary>
        </Box>

        {/* Prediction form */}
        <Paper
          variant='outlined'
          sx={{
            bgcolor: t.panelAlt,
            borderColor: t.lineSoft,
            borderRadius: '4px',
            p: 3,
          }}
        >
          <Typography
            sx={{
              fontFamily: monoFont,
              fontSize: 10,
              color: t.inkMuted,
              letterSpacing: '0.1em',
              textTransform: 'uppercase',
              mb: '14px',
            }}
          >
            Predict a flight
          </Typography>
          <Box
            sx={{
              display: 'grid',
              gridTemplateColumns: '1fr 1fr 1fr auto',
              border: `1px solid ${t.line}`,
              borderRadius: '4px',
              bgcolor: t.panel,
              overflow: 'hidden',
            }}
          >
            {[
              ['Carrier', flight.code],
              ['Number', flight.number],
              ['Route', `${flight.from.code}–${flight.to.code}`],
            ].map(([label, val]) => (
              <Box
                key={label}
                sx={{ p: '10px 14px', borderRight: `1px solid ${t.lineSoft}` }}
              >
                <Typography
                  sx={{
                    fontSize: 10,
                    color: t.inkMuted,
                    mb: '2px',
                    fontFamily: 'Inter, sans-serif',
                  }}
                >
                  {label}
                </Typography>
                <Typography
                  sx={{ fontFamily: monoFont, fontSize: 14, color: t.ink }}
                >
                  {val}
                </Typography>
              </Box>
            ))}
            <Button
              onClick={handlePredict}
              loading={isPending}
              loadingPosition='end'
              endIcon={'→'}
              variant='contained'
              disableElevation
              sx={{
                bgcolor: t.ink,
                color: t.bg,
                borderRadius: 0,
                fontSize: 13,
                px: '22px',
                fontWeight: 500,
                textTransform: 'none',
                '&:hover': { bgcolor: t.inkSoft },
                '&.Mui-disabled': { bgcolor: t.inkMuted, color: t.bg },
              }}
            >
              {/* {isPending ? '…' : 'Predict →'} */}
              Predict
            </Button>
          </Box>
          <Box sx={{ mt: '14px' }}>
            <FlightSwitcher
              flights={FLIGHTS}
              current={flight}
              onPick={onPickFlight}
              t={t}
            />
          </Box>
        </Paper>
      </Box>
    </Box>
  );
}

// ─── Prediction headline ──────────────────────────────────────────────────────

interface PredHeadlineProps {
  t: Tokens;
  flight: Flight;
  onTimeProb: number;
  verdict: { label: string; color: string };
  prediction: PredictResponse | null;
}

function PredictionHeadline({
  t,
  flight,
  onTimeProb,
  verdict,
  prediction,
}: PredHeadlineProps) {
  const delayP50 = prediction
    ? prediction.delay_probability > 0.5
      ? 30
      : 5
    : flight.delayMin.p50;
  const delayP90 = prediction
    ? prediction.delay_probability > 0.5
      ? 90
      : 20
    : flight.delayMin.p90;
  const cancelProb = flight.cancelProb;
  const confidence = Math.abs(onTimeProb - 0.5) * 2;

  return (
    <Box
      component='section'
      sx={{ p: '40px 56px', borderBottom: `1px solid ${t.line}` }}
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
              sx={{ fontFamily: monoFont, fontSize: 13, color: t.inkSoft }}
            >
              {flight.code} {flight.number}
            </Typography>
            <Typography
              sx={{
                fontSize: 13,
                color: t.inkMuted,
                fontFamily: 'Inter, sans-serif',
              }}
            >
              {flight.airline} · {flight.aircraft}
            </Typography>
          </Stack>
          <Stack direction='row' spacing='18px' sx={{ alignItems: 'center' }}>
            <Typography
              sx={{
                fontFamily: serifFont,
                fontSize: 56,
                lineHeight: 1,
                letterSpacing: '-0.03em',
                color: t.ink,
              }}
            >
              {flight.from.code}
            </Typography>
            <svg width='40' height='14' viewBox='0 0 40 14'>
              <line
                x1='0'
                y1='7'
                x2='36'
                y2='7'
                stroke={t.inkSoft}
                strokeWidth='1'
              />
              <polyline
                points='32,3 36,7 32,11'
                fill='none'
                stroke={t.inkSoft}
                strokeWidth='1'
              />
            </svg>
            <Typography
              sx={{
                fontFamily: serifFont,
                fontSize: 56,
                lineHeight: 1,
                letterSpacing: '-0.03em',
                color: t.ink,
              }}
            >
              {flight.to.code}
            </Typography>
          </Stack>
          <Typography
            sx={{
              mt: 1,
              fontSize: 13,
              color: t.inkSoft,
              fontFamily: 'Inter, sans-serif',
            }}
          >
            {flight.from.city} → {flight.to.city} · {flight.scheduled.date} ·
            dep {flight.scheduled.dep} {flight.from.tz}
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
                    bgcolor: t.accent,
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
                color: t.accent,
                borderColor: t.accent,
                borderRadius: '3px',
                height: 'auto',
                letterSpacing: '0.06em',
                '& .MuiChip-label': { px: '10px', py: '4px' },
                '& .MuiChip-icon': { color: t.accent, mr: 0 },
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
              color: t.inkMuted,
              letterSpacing: '0.12em',
              textTransform: 'uppercase',
              mb: 1,
            }}
          >
            On-time probability
          </Typography>
          <Box sx={{ position: 'relative', display: 'inline-block' }}>
            <ProbabilityArc prob={onTimeProb} t={t} size={180} />
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
                    color: t.ink,
                  }}
                >
                  {(onTimeProb * 100).toFixed(0)}
                  <Box component='span' sx={{ fontSize: 22, color: t.inkSoft }}>
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
              color: t.inkMuted,
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
                color: t.ink,
              }}
            >
              {delayP50}
            </Typography>
            <Typography
              sx={{
                fontSize: 14,
                color: t.inkSoft,
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
                  borderBottom: `1px solid ${t.lineSoft}`,
                  justifyContent: 'space-between',
                }}
              >
                <Typography
                  sx={{
                    fontSize: 12,
                    color: t.inkSoft,
                    fontFamily: 'Inter, sans-serif',
                  }}
                >
                  {label}
                </Typography>
                <Typography
                  sx={{ fontSize: 12, fontFamily: monoFont, color: t.inkSoft }}
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
              color: t.inkMuted,
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
              color: t.ink,
            }}
          >
            {confidence.toFixed(2)}
          </Typography>
          <Typography
            sx={{
              mt: 2,
              fontSize: 12,
              color: t.inkSoft,
              lineHeight: 1.5,
              fontFamily: 'Inter, sans-serif',
            }}
          >
            {prediction
              ? `Scored live. Brier ~0.06. ${prediction.features_complete ? 'All features resolved.' : 'Some features missing.'}`
              : 'Calibrated against 14d holdout. Brier 0.061.'}
          </Typography>
          <Button
            variant='outlined'
            sx={{
              mt: '14px',
              color: t.ink,
              borderColor: t.line,
              borderRadius: '2px',
              fontSize: 12,
              textTransform: 'none',
              fontFamily: 'Inter, sans-serif',
              '&:hover': { borderColor: t.ink, bgcolor: 'transparent' },
            }}
          >
            Inspect features →
          </Button>
        </Box>
      </Box>
    </Box>
  );
}

// ─── Feature attribution + route history ─────────────────────────────────────

function AttributionAndHistory({ t, flight }: { t: Tokens; flight: Flight }) {
  const avgOtp = Math.round(
    flight.history.reduce((a, b) => a + b, 0) / flight.history.length,
  );

  return (
    <Box
      component='section'
      sx={{
        p: '40px 56px',
        display: 'grid',
        gridTemplateColumns: '1.3fr 1fr',
        gap: 4,
        borderBottom: `1px solid ${t.line}`,
      }}
    >
      <Paper
        variant='outlined'
        sx={{
          bgcolor: t.panel,
          borderColor: t.lineSoft,
          borderRadius: '4px',
          p: 3,
        }}
      >
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
                color: t.inkMuted,
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
                color: t.ink,
              }}
            >
              What's driving this prediction
            </Typography>
          </Box>
          <Typography
            sx={{ fontSize: 11, color: t.inkMuted, fontFamily: monoFont }}
          >
            SHAP-equivalent · log-odds shift
          </Typography>
        </Stack>
        {flight.factors.map((f, i) => (
          <FactorBar key={i} factor={f} t={t} />
        ))}
      </Paper>

      <Paper
        variant='outlined'
        sx={{
          bgcolor: t.panel,
          borderColor: t.lineSoft,
          borderRadius: '4px',
          p: 3,
        }}
      >
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
                color: t.inkMuted,
                letterSpacing: '0.12em',
                textTransform: 'uppercase',
              }}
            >
              Route history · 14d
            </Typography>
            <Typography
              component='h3'
              sx={{
                fontFamily: serifFont,
                fontSize: 22,
                mt: '6px',
                fontWeight: 400,
                letterSpacing: '-0.01em',
                color: t.ink,
              }}
            >
              {flight.from.code} → {flight.to.code} on-time %
            </Typography>
          </Box>
          <Typography sx={{ fontFamily: monoFont, fontSize: 12, color: t.ink }}>
            {avgOtp}
            <Box component='span' sx={{ color: t.inkMuted }}>
              %
            </Box>
            <Box component='span' sx={{ ml: 1, color: t.inkMuted }}>
              avg
            </Box>
          </Typography>
        </Stack>
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
              <Box
                sx={{ position: 'relative', width: '100%', height: 160 }}
              ></Box>
            }
          >
            <RouteHistoryChart
              origin={flight.from.code}
              dest={flight.to.code}
              days={14}
              t={t}
            />
          </Suspense>
        </ErrorBoundary>

        <Stack
          direction='row'
          sx={{
            mt: '12px',
            fontFamily: monoFont,
            fontSize: 10,
            color: t.inkMuted,
            justifyContent: 'space-between',
          }}
        >
          <Typography
            sx={{ fontFamily: monoFont, fontSize: 10, color: t.inkMuted }}
          >
            14d ago
          </Typography>
          <Typography
            sx={{ fontFamily: monoFont, fontSize: 10, color: t.inkMuted }}
          >
            7d ago
          </Typography>
          <Typography
            sx={{ fontFamily: monoFont, fontSize: 10, color: t.inkMuted }}
          >
            today
          </Typography>
        </Stack>
      </Paper>
    </Box>
  );
}

interface RouteHistoryResponse {
  route_key: string;
  history: number[];
  days: number;
}

function RouteHistoryChart({
  origin,
  dest,
  days = 14,
  t,
}: {
  origin: string;
  dest: string;
  days?: number;
  t: Tokens;
}) {
  // apiFetch(`/api/routes/${flight.from.code}-${flight.to.code}/history?days=14`).then(r => r.json())
  const { data: historyData } = useSuspenseQuery({
    queryKey: ['routes', origin, dest, 'history', days],
    queryFn: () =>
      apiFetch(`/api/routes/${origin}-${dest}/history?days=${days}`).then(
        (r) => r.json() as Promise<RouteHistoryResponse>,
      ),
  });
  console.log('FLIGHT HISTORY: ', historyData);
  const data = historyData?.history || [];
  const w = 100;
  const h = 160;
  const stepX = w / (data.length - 1);
  const points = data.map((v, i) => [i * stepX, h - (v / 100) * h]);
  const areaD =
    points.map((p, i) => `${i === 0 ? 'M' : 'L'} ${p[0]} ${p[1]}`).join(' ') +
    ` L ${w} ${h} L 0 ${h} Z`;
  const lineD = points
    .map((p, i) => `${i === 0 ? 'M' : 'L'} ${p[0]} ${p[1]}`)
    .join(' ');

  return (
    <Box sx={{ position: 'relative', width: '100%', height: h }}>
      <svg
        viewBox={`0 0 ${w} ${h}`}
        preserveAspectRatio='none'
        width='100%'
        height={h}
        style={{ display: 'block' }}
      >
        {[0, 25, 50, 75, 100].map((y) => (
          <line
            key={y}
            x1='0'
            x2={w}
            y1={h - (y / 100) * h}
            y2={h - (y / 100) * h}
            stroke={t.lineSoft}
            strokeWidth='0.3'
          />
        ))}
        <line
          x1='0'
          x2={w}
          y1={h - 0.8 * h}
          y2={h - 0.8 * h}
          stroke={t.line}
          strokeWidth='0.4'
          strokeDasharray='2 1.5'
        />
        <path d={areaD} fill={t.lineSoft} />
        <path
          d={lineD}
          stroke={t.ink}
          strokeWidth='0.6'
          fill='none'
          vectorEffect='non-scaling-stroke'
        />
        {points.map((p, i) => (
          <circle
            key={i}
            cx={p[0]}
            cy={p[1]}
            r='0.8'
            fill={t.bg}
            stroke={t.ink}
            strokeWidth='0.4'
            vectorEffect='non-scaling-stroke'
          />
        ))}
      </svg>
      <Typography
        sx={{
          position: 'absolute',
          right: 0,
          top: h - 0.8 * h - 10,
          fontSize: 10,
          color: t.inkMuted,
          fontFamily: monoFont,
        }}
      >
        80% target
      </Typography>
    </Box>
  );
}

// ─── Weather + congestion strip ───────────────────────────────────────────────

function WeatherCongestionStrip({ t, flight }: { t: Tokens; flight: Flight }) {
  const cards = [
    {
      l: 'Origin weather',
      code: flight.from.code,
      top: 'VFR · clear',
      sub: 'wind 240@8 · vis 10sm',
      spark: [82, 84, 86, 85, 88, 90, 89, 91],
      col: t.good,
    },
    {
      l: 'Destination weather',
      code: flight.to.code,
      top: 'MVFR · scattered',
      sub: 'wind 290@14G22 · vis 6sm',
      spark: [88, 84, 80, 76, 72, 70, 68, 71],
      col: t.warn,
    },
    {
      l: 'Origin congestion',
      code: flight.from.code,
      top: 'Normal',
      sub: 'taxi 18m · queue 4',
      spark: [12, 14, 16, 15, 18, 17, 19, 18],
      col: t.ink,
    },
    {
      l: 'Dest congestion',
      code: flight.to.code,
      top: 'Elevated',
      sub: 'taxi 26m · queue 11',
      spark: [10, 12, 15, 18, 22, 26, 28, 30],
      col: t.warn,
    },
  ];
  return (
    <Box
      component='section'
      sx={{
        p: '40px 56px',
        display: 'grid',
        gridTemplateColumns: 'repeat(4, 1fr)',
        gap: 3,
        borderBottom: `1px solid ${t.line}`,
      }}
    >
      {cards.map((c, i) => (
        <StatCard
          key={`stat-${1}`}
          label={c.l}
          code={c.code}
          value={c.top}
          subtitle={c.sub}
          spark={c.spark}
          color={c.col}
          fill={t.lineSoft}
        />
      ))}
    </Box>
  );
}

// ─── Network + airline comparison ────────────────────────────────────────────

function NetworkAndAirline({ t, flight }: { t: Tokens; flight: Flight }) {
  return (
    <Box
      component='section'
      sx={{
        p: '40px 56px',
        display: 'grid',
        gridTemplateColumns: '1.3fr 1fr',
        gap: 4,
        borderBottom: `1px solid ${t.line}`,
      }}
    >
      <Paper
        variant='outlined'
        sx={{
          bgcolor: t.panel,
          borderColor: t.lineSoft,
          borderRadius: '4px',
          p: 3,
        }}
      >
        <Box sx={{ mb: '18px' }}>
          <Typography
            sx={{
              fontFamily: monoFont,
              fontSize: 10,
              color: t.inkMuted,
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
              color: t.ink,
            }}
          >
            Average delays across the system
          </Typography>
        </Box>
        <NetworkMap t={t} height={290} />
      </Paper>

      <Paper
        variant='outlined'
        sx={{
          bgcolor: t.panel,
          borderColor: t.lineSoft,
          borderRadius: '4px',
          p: 3,
        }}
      >
        <Box sx={{ mb: '18px' }}>
          <Typography
            sx={{
              fontFamily: monoFont,
              fontSize: 10,
              color: t.inkMuted,
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
              color: t.ink,
            }}
          >
            {flight.from.code} → {flight.to.code} · 30d OTP
          </Typography>
        </Box>
        <AirlineComparison t={t} currentCode={flight.code} />
      </Paper>
    </Box>
  );
}

// ─── Footer ───────────────────────────────────────────────────────────────────

function PageFooter({ t }: { t: Tokens }) {
  return (
    <Box
      component='footer'
      sx={{
        p: '32px 56px 56px',
        display: 'flex',
        justifyContent: 'space-between',
        alignItems: 'center',
      }}
    >
      <Typography
        sx={{
          fontSize: 12,
          color: t.inkMuted,
          fontFamily: 'Inter, sans-serif',
        }}
      >
        © 2026 Holdline · ML predictions are probabilistic, not guarantees.
      </Typography>
      <Stack direction='row' spacing='18px'>
        {['Status', 'Changelog', 'Pricing', 'API reference'].map((label) => (
          <Link
            key={label}
            underline='hover'
            sx={{
              fontSize: 12,
              color: t.inkMuted,
              cursor: 'pointer',
              fontFamily: 'Inter, sans-serif',
            }}
          >
            {label}
          </Link>
        ))}
      </Stack>
    </Box>
  );
}

// ─── Root component ───────────────────────────────────────────────────────────

function IndexAlt() {
  const [flight, setFlight] = useState<Flight>(FLIGHTS[0]);
  const [prediction, setPrediction] = useState<PredictResponse | null>(null);
  // const [predicting, setPredicting] = useState(false);
  const mode = useResolvedMode();
  const t = mode === 'dark' ? TOKENS.dark : TOKENS.light;

  const onTimeProb = prediction
    ? 1 - prediction.delay_probability
    : flight.onTimeProb;

  const verdict = useMemo(() => {
    if (onTimeProb >= 0.85) return { label: 'Likely on time', color: t.good };
    if (onTimeProb >= 0.65) return { label: 'Mild delay risk', color: t.warn };
    return { label: 'Elevated delay risk', color: t.bad };
  }, [onTimeProb, t]);

  useEffect(() => {
    setPrediction(null);
  }, [flight.id]);

  return (
    <Box sx={{ bgcolor: 'background.default', color: 'text.primary' }}>
      <HeroSection
        t={t}
        isDark={mode === 'dark'}
        flight={flight}
        onPickFlight={(f) => setFlight(f)}
        onPredict={(p) => setPrediction(p)}
        // predicting={predicting}
        // setPredicting={setPredicting}
      />
      <PredictionHeadline
        t={t}
        flight={flight}
        onTimeProb={onTimeProb}
        verdict={verdict}
        prediction={prediction}
      />
      <AttributionAndHistory t={t} flight={flight} />
      <WeatherCongestionStrip t={t} flight={flight} />
      <NetworkAndAirline t={t} flight={flight} />
      <PageFooter t={t} />
    </Box>
  );
}
