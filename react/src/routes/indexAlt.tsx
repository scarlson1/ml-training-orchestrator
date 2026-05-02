import { Skeleton } from '@mui/material';
import Box from '@mui/material/Box';
import { useColorScheme } from '@mui/material/styles';
import { useSuspenseQuery } from '@tanstack/react-query';
import { createFileRoute } from '@tanstack/react-router';
import { Suspense, useEffect, useMemo, useState } from 'react';
import { ErrorBoundary } from 'react-error-boundary';
import { apiFetch } from '~/api';
import { Globe } from '~/components/Globe';
import { monoFont, serifFont } from '~/config/themePrimitives';

export const Route = createFileRoute('/indexAlt')({
  component: IndexAlt,
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
  sched: { dep: string; arr: string; date: string };
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
    sched: { dep: '07:45', arr: '16:18', date: 'Thu, 1 May' },
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
    sched: { dep: '20:15', arr: '10:05', date: 'Thu, 1 May' },
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
    sched: { dep: '13:30', arr: '15:02', date: 'Thu, 1 May' },
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
    sched: { dep: '17:00', arr: '10:35', date: 'Thu, 1 May' },
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

function Sparkline({
  values,
  color,
  height = 28,
  width = 120,
  fill,
}: {
  values: number[];
  color: string;
  height?: number;
  width?: number;
  fill?: string;
}) {
  const max = Math.max(...values);
  const min = Math.min(...values);
  const norm = (v: number) => height - ((v - min) / (max - min || 1)) * height;
  const step = width / (values.length - 1);
  const d = values
    .map((v, i) => `${i === 0 ? 'M' : 'L'} ${i * step} ${norm(v)}`)
    .join(' ');
  const a = `${d} L ${width} ${height} L 0 ${height} Z`;

  return (
    <svg width={width} height={height} style={{ display: 'block' }}>
      {fill && <path d={a} fill={fill} />}
      <path d={d} stroke={color} strokeWidth='1.5' fill='none' />
    </svg>
  );
}

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
    <div
      style={{
        display: 'grid',
        gridTemplateColumns: '1fr 180px 40px',
        gap: 16,
        alignItems: 'center',
        padding: '10px 0',
        borderBottom: `1px solid ${t.lineSoft}`,
      }}
    >
      <div>
        <div style={{ fontSize: 13, color: t.ink, fontWeight: 500 }}>
          {factor.name}
        </div>
        <div style={{ fontSize: 11, color: t.inkMuted, marginTop: 2 }}>
          {factor.detail}
        </div>
      </div>
      <div
        style={{
          position: 'relative',
          height: 6,
          background: t.lineSoft,
          borderRadius: 1,
        }}
      >
        <div
          style={{
            position: 'absolute',
            top: 0,
            bottom: 0,
            left: '50%',
            width: 1,
            background: t.line,
          }}
        />
        <div
          style={{
            position: 'absolute',
            top: 0,
            bottom: 0,
            left: positive ? '50%' : `${50 - pct}%`,
            width: `${pct}%`,
            background: positive ? t.good : t.bad,
            borderRadius: 1,
          }}
        />
      </div>
      <div
        style={{
          fontFamily: monoFont,
          fontSize: 12,
          textAlign: 'right',
          color: positive ? t.good : t.bad,
        }}
      >
        {positive ? '+' : ''}
        {(v * 100).toFixed(0)}
      </div>
    </div>
  );
}

function RouteHistoryChart({ flight, t }: { flight: Flight; t: Tokens }) {
  const data = flight.history;
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
    <div style={{ position: 'relative', width: '100%', height: h }}>
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
      <div
        style={{
          position: 'absolute',
          right: 0,
          top: h - 0.8 * h - 10,
          fontSize: 10,
          color: t.inkMuted,
          fontFamily: monoFont,
        }}
      >
        80% target
      </div>
    </div>
  );
}

// ─── Network map ──────────────────────────────────────────────────────────────

function NetworkMap({ t, height = 280 }: { t: Tokens; height?: number }) {
  const statusColor = (s: 'green' | 'amber' | 'red') =>
    s === 'red' ? t.bad : s === 'amber' ? t.warn : t.good;
  return (
    <div
      style={{
        position: 'relative',
        width: '100%',
        height,
        background: t.panelAlt,
        borderRadius: 4,
        overflow: 'hidden',
        border: `1px solid ${t.lineSoft}`,
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
      <div
        style={{
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
      </div>
      <div
        style={{
          position: 'absolute',
          bottom: 10,
          right: 12,
          display: 'flex',
          gap: 10,
          fontSize: 10,
          color: t.inkSoft,
          fontFamily: monoFont,
        }}
      >
        {[
          ['green', t.good, '<15m'],
          ['amber', t.warn, '15–30m'],
          ['red', t.bad, '30m+'],
        ].map(([, col, label]) => (
          <span
            key={label as string}
            style={{ display: 'flex', gap: 4, alignItems: 'center' }}
          >
            <span
              style={{
                width: 6,
                height: 6,
                background: col as string,
                borderRadius: 6,
                display: 'inline-block',
              }}
            />
            {label}
          </span>
        ))}
      </div>
    </div>
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
    <div>
      {AIRLINE_COMPARISON.map((a) => {
        const isCurrent = a.code === currentCode;
        return (
          <div
            key={a.code}
            style={{
              display: 'grid',
              gridTemplateColumns: '20px 1fr 80px 56px 50px',
              gap: 12,
              alignItems: 'center',
              padding: '10px 0',
              borderBottom: `1px solid ${t.lineSoft}`,
              opacity: isCurrent ? 1 : 0.78,
            }}
          >
            <div
              style={{
                width: 18,
                height: 18,
                borderRadius: 2,
                background: t.chipBg,
                display: 'grid',
                placeItems: 'center',
                fontSize: 9,
                fontFamily: monoFont,
                color: t.inkSoft,
                fontWeight: 600,
              }}
            >
              {a.code}
            </div>
            <div
              style={{
                fontSize: 13,
                color: t.ink,
                fontWeight: isCurrent ? 600 : 400,
              }}
            >
              {a.airline}
              {isCurrent && (
                <span
                  style={{
                    marginLeft: 8,
                    fontSize: 10,
                    color: t.accent,
                    textTransform: 'uppercase',
                    letterSpacing: '0.08em',
                  }}
                >
                  current
                </span>
              )}
            </div>
            <div
              style={{
                position: 'relative',
                height: 4,
                background: t.lineSoft,
                borderRadius: 1,
              }}
            >
              <div
                style={{
                  position: 'absolute',
                  left: 0,
                  top: 0,
                  bottom: 0,
                  width: `${a.otp * 100}%`,
                  background: t.ink,
                  borderRadius: 1,
                }}
              />
            </div>
            <div
              style={{
                fontFamily: monoFont,
                fontSize: 12,
                color: t.ink,
                textAlign: 'right',
              }}
            >
              {(a.otp * 100).toFixed(0)}%
            </div>
            <div
              style={{
                fontFamily: monoFont,
                fontSize: 12,
                color: t.inkSoft,
                textAlign: 'right',
              }}
            >
              {a.avgDelay}m
            </div>
          </div>
        );
      })}
    </div>
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
    <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap' }}>
      {flights.map((f) => {
        const active = f.id === current.id;
        return (
          <button
            key={f.id}
            onClick={() => onPick(f)}
            style={{
              border: `1px solid ${active ? t.ink : t.line}`,
              background: active ? t.ink : 'transparent',
              color: active ? t.bg : t.ink,
              padding: '6px 10px',
              borderRadius: 2,
              fontFamily: monoFont,
              fontSize: 11,
              cursor: 'pointer',
              letterSpacing: '0.04em',
              transition: 'all 0.15s ease',
            }}
          >
            {f.code} {f.number} · {f.from.code}→{f.to.code}
          </button>
        );
      })}
    </div>
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
    <div style={{ borderLeft: `1px solid ${t.line}`, paddingLeft: 14 }}>
      <div
        style={{
          fontFamily: monoFont,
          fontSize: 9,
          color: t.inkMuted,
          letterSpacing: '0.12em',
          textTransform: 'uppercase',
        }}
      >
        {title}
      </div>
      <div
        style={{
          fontFamily: serifFont,
          fontSize: 26,
          color: t.ink,
          letterSpacing: '-0.02em',
          marginTop: 2,
        }}
      >
        {value}
      </div>
      <div
        style={{
          fontFamily: monoFont,
          fontSize: 10,
          color: t.inkMuted,
          marginTop: 2,
        }}
      >
        {subtitle}
      </div>
    </div>
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
    <div style={{ display: 'flex', gap: 28, marginTop: 28 }}>
      {kpis.map((k, i) => (
        <KpiItem
          key={`kpi-${i}`}
          title={k.l}
          value={k.v}
          subtitle={k.s}
          t={t}
        />
      ))}
    </div>
  );
}

// ─── Hero section ─────────────────────────────────────────────────────────────

interface HeroProps {
  t: Tokens;
  isDark: boolean;
  flight: Flight;
  onPickFlight: (f: Flight) => void;
  onPredict: (p: PredictResponse) => void;
  predicting: boolean;
  setPredicting: (v: boolean) => void;
}

function HeroSection({
  t,
  isDark,
  flight,
  onPickFlight,
  onPredict,
  predicting,
  setPredicting,
}: HeroProps) {
  const handlePredict = async () => {
    setPredicting(true);
    try {
      const body = {
        flight_id: `${flight.code}${flight.number}_${Date.now()}`,
        origin: flight.from.code,
        dest: flight.to.code,
        carrier: flight.code,
        tail_number: 'N00000',
        route_key: `${flight.from.code}-${flight.to.code}`,
      };
      const res = await apiFetch('/predict', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      });
      if (res.ok) {
        const data = (await res.json()) as PredictResponse;
        onPredict(data);
      }
    } finally {
      setPredicting(false);
    }
  };

  return (
    <section
      style={{
        position: 'relative',
        padding: '56px 56px 36px',
        borderBottom: `1px solid ${t.line}`,
        background: isDark
          ? 'radial-gradient(ellipse at 65% 50%, #0A1124 0%, #0F0F0E 65%)'
          : 'radial-gradient(ellipse at 65% 50%, #FFFFFF 0%, #FBFAF7 65%)',
        overflow: 'hidden',
      }}
    >
      {/* Globe — clips at right edge */}
      <div
        style={{
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
      </div>

      <div
        style={{
          position: 'relative',
          display: 'grid',
          gridTemplateColumns: '1.1fr 1fr',
          gap: 64,
          alignItems: 'end',
          minHeight: 520,
        }}
      >
        <div>
          <div
            style={{
              fontFamily: monoFont,
              fontSize: 11,
              color: t.inkMuted,
              letterSpacing: '0.12em',
              textTransform: 'uppercase',
              marginBottom: 16,
            }}
          >
            <span
              style={{
                display: 'inline-block',
                width: 6,
                height: 6,
                borderRadius: 6,
                background: t.good,
                marginRight: 8,
                verticalAlign: 'middle',
              }}
            />
            Live · ML flight delay prediction
          </div>
          <h1
            style={{
              fontFamily: serifFont,
              fontSize: 64,
              lineHeight: 0.98,
              letterSpacing: '-0.025em',
              margin: 0,
              fontWeight: 400,
              color: t.ink,
            }}
          >
            Know whether
            <br />
            the flight{' '}
            <em style={{ fontStyle: 'italic', color: t.inkSoft }}>will</em> hold
            —
            <br />
            before it pushes back.
          </h1>
          <p
            style={{
              marginTop: 20,
              fontSize: 15,
              color: t.inkSoft,
              maxWidth: 480,
              lineHeight: 1.55,
              fontFamily: 'Inter, sans-serif',
            }}
          >
            Holdline's ensemble model fuses METAR, TAF, ground-stop bulletins,
            fleet rotation, and 9 years of carrier OTP data into a calibrated
            probability — refreshed every 90 seconds.
          </p>

          <ErrorBoundary
            // FallbackComponent={ErrorFallback}
            fallbackRender={({ error, resetErrorBoundary }) => (
              <div style={{ display: 'flex', gap: 28, marginTop: 28 }}>
                {[
                  {
                    l: 'In flight today',
                    v: '—',
                    s: 'flights scored',
                  },
                  {
                    l: 'Predicted on-time',
                    v: '—',
                    s: '- live rate',
                  },
                  {
                    l: 'PSI alerts',
                    v: '—',
                    s: 'unknown',
                  },
                  {
                    l: 'Model version',
                    v: '—',
                    s: 'champion',
                  },
                ].map((k, i) => (
                  <KpiItem
                    key={`kpi-${i}`}
                    title={k.l}
                    value={k.v}
                    subtitle={k.s}
                    t={t}
                  />
                ))}
              </div>
            )}
          >
            <Suspense
              fallback={
                <div style={{ display: 'flex', gap: 28, marginTop: 28 }}>
                  {Array.from({ length: 4 }).map((_, i) => (
                    <div
                      key={`load-kpi-${i}`}
                      style={{
                        borderLeft: `1px solid ${t.line}`,
                        paddingLeft: 14,
                      }}
                    >
                      <Skeleton width={72} height={10} />
                      <Skeleton
                        width={40}
                        height={30}
                        style={{ marginTop: 6 }}
                      />
                      <Skeleton
                        width={56}
                        height={10}
                        style={{ marginTop: 6 }}
                      />
                    </div>
                  ))}
                </div>
              }
            >
              <KpiStrip t={t} />
            </Suspense>
          </ErrorBoundary>
        </div>

        {/* Prediction form */}
        <div
          style={{
            background: t.panelAlt,
            border: `1px solid ${t.lineSoft}`,
            borderRadius: 4,
            padding: 24,
          }}
        >
          <div
            style={{
              fontFamily: monoFont,
              fontSize: 10,
              color: t.inkMuted,
              letterSpacing: '0.1em',
              textTransform: 'uppercase',
              marginBottom: 14,
            }}
          >
            Predict a flight
          </div>
          <div
            style={{
              display: 'grid',
              gridTemplateColumns: '1fr 1fr 1fr auto',
              gap: 0,
              border: `1px solid ${t.line}`,
              borderRadius: 4,
              background: t.panel,
              overflow: 'hidden',
            }}
          >
            <div
              style={{
                padding: '10px 14px',
                borderRight: `1px solid ${t.lineSoft}`,
              }}
            >
              <div
                style={{
                  fontSize: 10,
                  color: t.inkMuted,
                  marginBottom: 2,
                  fontFamily: 'Inter, sans-serif',
                }}
              >
                Carrier
              </div>
              <div style={{ fontFamily: monoFont, fontSize: 14, color: t.ink }}>
                {flight.code}
              </div>
            </div>
            <div
              style={{
                padding: '10px 14px',
                borderRight: `1px solid ${t.lineSoft}`,
              }}
            >
              <div
                style={{
                  fontSize: 10,
                  color: t.inkMuted,
                  marginBottom: 2,
                  fontFamily: 'Inter, sans-serif',
                }}
              >
                Number
              </div>
              <div style={{ fontFamily: monoFont, fontSize: 14, color: t.ink }}>
                {flight.number}
              </div>
            </div>
            <div
              style={{
                padding: '10px 14px',
                borderRight: `1px solid ${t.lineSoft}`,
              }}
            >
              <div
                style={{
                  fontSize: 10,
                  color: t.inkMuted,
                  marginBottom: 2,
                  fontFamily: 'Inter, sans-serif',
                }}
              >
                Route
              </div>
              <div style={{ fontFamily: monoFont, fontSize: 14, color: t.ink }}>
                {flight.from.code}–{flight.to.code}
              </div>
            </div>
            <button
              onClick={handlePredict}
              disabled={predicting}
              style={{
                background: predicting ? t.inkMuted : t.ink,
                color: t.bg,
                border: 'none',
                padding: '0 22px',
                fontSize: 13,
                cursor: predicting ? 'not-allowed' : 'pointer',
                fontFamily: 'Inter, sans-serif',
                fontWeight: 500,
                transition: 'background 0.15s',
              }}
            >
              {predicting ? '…' : 'Predict →'}
            </button>
          </div>
          <div style={{ marginTop: 14 }}>
            <FlightSwitcher
              flights={FLIGHTS}
              current={flight}
              onPick={onPickFlight}
              t={t}
            />
          </div>
        </div>
      </div>
    </section>
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
    <section
      style={{ padding: '40px 56px', borderBottom: `1px solid ${t.line}` }}
    >
      <div
        style={{
          display: 'grid',
          gridTemplateColumns: '1.5fr 1fr 1fr 1fr',
          gap: 48,
          alignItems: 'start',
        }}
      >
        {/* flight identity */}
        <div>
          <div
            style={{
              display: 'flex',
              alignItems: 'baseline',
              gap: 18,
              marginBottom: 8,
            }}
          >
            <div
              style={{ fontFamily: monoFont, fontSize: 13, color: t.inkSoft }}
            >
              {flight.code} {flight.number}
            </div>
            <div
              style={{
                fontSize: 13,
                color: t.inkMuted,
                fontFamily: 'Inter, sans-serif',
              }}
            >
              {flight.airline} · {flight.aircraft}
            </div>
          </div>
          <div style={{ display: 'flex', alignItems: 'center', gap: 18 }}>
            <div
              style={{
                fontFamily: serifFont,
                fontSize: 56,
                lineHeight: 1,
                letterSpacing: '-0.03em',
                color: t.ink,
              }}
            >
              {flight.from.code}
            </div>
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
            <div
              style={{
                fontFamily: serifFont,
                fontSize: 56,
                lineHeight: 1,
                letterSpacing: '-0.03em',
                color: t.ink,
              }}
            >
              {flight.to.code}
            </div>
          </div>
          <div
            style={{
              marginTop: 8,
              fontSize: 13,
              color: t.inkSoft,
              fontFamily: 'Inter, sans-serif',
            }}
          >
            {flight.from.city} → {flight.to.city} · {flight.sched.date} · dep{' '}
            {flight.sched.dep} {flight.from.tz}
          </div>
          {prediction && (
            <div
              style={{
                marginTop: 14,
                display: 'inline-flex',
                alignItems: 'center',
                gap: 8,
                fontFamily: monoFont,
                fontSize: 10,
                color: t.accent,
                border: `1px solid ${t.accent}`,
                borderRadius: 3,
                padding: '4px 10px',
                letterSpacing: '0.06em',
              }}
            >
              <span
                style={{
                  width: 5,
                  height: 5,
                  borderRadius: '50%',
                  background: t.accent,
                  display: 'inline-block',
                }}
              />
              live ·{' '}
              {prediction.model_version
                ? `v${prediction.model_version.slice(0, 6)}`
                : 'model'}{' '}
              ·{' '}
              {prediction.features_complete
                ? 'features complete'
                : 'partial features'}
            </div>
          )}
        </div>

        {/* probability arc */}
        <div>
          <div
            style={{
              fontFamily: monoFont,
              fontSize: 10,
              color: t.inkMuted,
              letterSpacing: '0.12em',
              textTransform: 'uppercase',
              marginBottom: 8,
            }}
          >
            On-time probability
          </div>
          <div style={{ position: 'relative', display: 'inline-block' }}>
            <ProbabilityArc prob={onTimeProb} t={t} size={180} />
            <div
              style={{
                position: 'absolute',
                inset: 0,
                display: 'grid',
                placeItems: 'center',
                textAlign: 'center',
              }}
            >
              <div>
                <div
                  style={{
                    fontFamily: serifFont,
                    fontSize: 44,
                    fontWeight: 400,
                    lineHeight: 1,
                    letterSpacing: '-0.02em',
                    color: t.ink,
                  }}
                >
                  {(onTimeProb * 100).toFixed(0)}
                  <span style={{ fontSize: 22, color: t.inkSoft }}>%</span>
                </div>
                <div
                  style={{
                    fontSize: 11,
                    color: verdict.color,
                    marginTop: 6,
                    fontWeight: 500,
                    fontFamily: 'Inter, sans-serif',
                  }}
                >
                  {verdict.label}
                </div>
              </div>
            </div>
          </div>
        </div>

        {/* expected delay */}
        <div>
          <div
            style={{
              fontFamily: monoFont,
              fontSize: 10,
              color: t.inkMuted,
              letterSpacing: '0.12em',
              textTransform: 'uppercase',
              marginBottom: 8,
            }}
          >
            Expected delay
          </div>
          <div style={{ display: 'flex', alignItems: 'baseline', gap: 6 }}>
            <div
              style={{
                fontFamily: serifFont,
                fontSize: 44,
                lineHeight: 1,
                letterSpacing: '-0.02em',
                color: t.ink,
              }}
            >
              {delayP50}
            </div>
            <div
              style={{
                fontSize: 14,
                color: t.inkSoft,
                fontFamily: 'Inter, sans-serif',
              }}
            >
              min · p50
            </div>
          </div>
          <div
            style={{
              marginTop: 16,
              fontSize: 12,
              color: t.inkSoft,
              fontFamily: 'Inter, sans-serif',
            }}
          >
            {[
              ['p50', `+${delayP50}m`],
              ['p90', `+${delayP90}m`],
              ['cancel', `${(cancelProb * 100).toFixed(1)}%`],
            ].map(([label, val]) => (
              <div
                key={label}
                style={{
                  display: 'flex',
                  justifyContent: 'space-between',
                  padding: '4px 0',
                  borderBottom: `1px solid ${t.lineSoft}`,
                }}
              >
                <span>{label}</span>
                <span style={{ fontFamily: monoFont }}>{val}</span>
              </div>
            ))}
          </div>
        </div>

        {/* model confidence */}
        <div>
          <div
            style={{
              fontFamily: monoFont,
              fontSize: 10,
              color: t.inkMuted,
              letterSpacing: '0.12em',
              textTransform: 'uppercase',
              marginBottom: 8,
            }}
          >
            Model confidence
          </div>
          <div style={{ display: 'flex', alignItems: 'baseline', gap: 6 }}>
            <div
              style={{
                fontFamily: serifFont,
                fontSize: 44,
                lineHeight: 1,
                letterSpacing: '-0.02em',
                color: t.ink,
              }}
            >
              {confidence.toFixed(2)}
            </div>
          </div>
          <div
            style={{
              marginTop: 16,
              fontSize: 12,
              color: t.inkSoft,
              lineHeight: 1.5,
              fontFamily: 'Inter, sans-serif',
            }}
          >
            {prediction
              ? `Scored live. Brier ~0.06. ${prediction.features_complete ? 'All features resolved.' : 'Some features missing.'}`
              : 'Calibrated against 14d holdout. Brier 0.061.'}
          </div>
          <button
            style={{
              marginTop: 14,
              background: 'transparent',
              color: t.ink,
              border: `1px solid ${t.line}`,
              padding: '6px 12px',
              borderRadius: 2,
              fontSize: 12,
              cursor: 'pointer',
              fontFamily: 'Inter, sans-serif',
            }}
          >
            Inspect features →
          </button>
        </div>
      </div>
    </section>
  );
}

// ─── Feature attribution + route history ─────────────────────────────────────

function AttributionAndHistory({ t, flight }: { t: Tokens; flight: Flight }) {
  const avgOtp = Math.round(
    flight.history.reduce((a, b) => a + b, 0) / flight.history.length,
  );
  return (
    <section
      style={{
        padding: '40px 56px',
        display: 'grid',
        gridTemplateColumns: '1.3fr 1fr',
        gap: 32,
        borderBottom: `1px solid ${t.line}`,
      }}
    >
      <div
        style={{
          background: t.panel,
          border: `1px solid ${t.lineSoft}`,
          borderRadius: 4,
          padding: 24,
        }}
      >
        <div
          style={{
            display: 'flex',
            justifyContent: 'space-between',
            alignItems: 'baseline',
            marginBottom: 18,
          }}
        >
          <div>
            <div
              style={{
                fontFamily: monoFont,
                fontSize: 10,
                color: t.inkMuted,
                letterSpacing: '0.12em',
                textTransform: 'uppercase',
              }}
            >
              Feature attribution
            </div>
            <h3
              style={{
                fontFamily: serifFont,
                fontSize: 22,
                margin: '6px 0 0',
                fontWeight: 400,
                letterSpacing: '-0.01em',
                color: t.ink,
              }}
            >
              What's driving this prediction
            </h3>
          </div>
          <div
            style={{ fontSize: 11, color: t.inkMuted, fontFamily: monoFont }}
          >
            SHAP-equivalent · log-odds shift
          </div>
        </div>
        {flight.factors.map((f, i) => (
          <FactorBar key={i} factor={f} t={t} />
        ))}
      </div>

      <div
        style={{
          background: t.panel,
          border: `1px solid ${t.lineSoft}`,
          borderRadius: 4,
          padding: 24,
        }}
      >
        <div
          style={{
            display: 'flex',
            justifyContent: 'space-between',
            alignItems: 'baseline',
            marginBottom: 18,
          }}
        >
          <div>
            <div
              style={{
                fontFamily: monoFont,
                fontSize: 10,
                color: t.inkMuted,
                letterSpacing: '0.12em',
                textTransform: 'uppercase',
              }}
            >
              Route history · 14d
            </div>
            <h3
              style={{
                fontFamily: serifFont,
                fontSize: 22,
                margin: '6px 0 0',
                fontWeight: 400,
                letterSpacing: '-0.01em',
                color: t.ink,
              }}
            >
              {flight.from.code} → {flight.to.code} on-time %
            </h3>
          </div>
          <div style={{ fontFamily: monoFont, fontSize: 12, color: t.ink }}>
            {avgOtp}
            <span style={{ color: t.inkMuted }}>%</span>
            <span style={{ marginLeft: 8, color: t.inkMuted }}>avg</span>
          </div>
        </div>
        <RouteHistoryChart flight={flight} t={t} />
        <div
          style={{
            display: 'flex',
            justifyContent: 'space-between',
            marginTop: 12,
            fontFamily: monoFont,
            fontSize: 10,
            color: t.inkMuted,
          }}
        >
          <span>14d ago</span>
          <span>7d ago</span>
          <span>today</span>
        </div>
      </div>
    </section>
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
    <section
      style={{
        padding: '40px 56px',
        display: 'grid',
        gridTemplateColumns: 'repeat(4, 1fr)',
        gap: 24,
        borderBottom: `1px solid ${t.line}`,
      }}
    >
      {cards.map((c, i) => (
        <div
          key={i}
          style={{
            background: t.panel,
            border: `1px solid ${t.lineSoft}`,
            borderRadius: 4,
            padding: 18,
          }}
        >
          <div
            style={{
              display: 'flex',
              justifyContent: 'space-between',
              alignItems: 'baseline',
            }}
          >
            <div
              style={{
                fontFamily: monoFont,
                fontSize: 10,
                color: t.inkMuted,
                letterSpacing: '0.1em',
                textTransform: 'uppercase',
              }}
            >
              {c.l}
            </div>
            <div
              style={{ fontFamily: monoFont, fontSize: 11, color: t.inkSoft }}
            >
              {c.code}
            </div>
          </div>
          <div
            style={{
              fontFamily: serifFont,
              fontSize: 22,
              marginTop: 8,
              color: c.col,
              letterSpacing: '-0.01em',
            }}
          >
            {c.top}
          </div>
          <div
            style={{
              fontFamily: monoFont,
              fontSize: 11,
              color: t.inkSoft,
              marginTop: 4,
            }}
          >
            {c.sub}
          </div>
          <div style={{ marginTop: 14 }}>
            <Sparkline
              values={c.spark}
              color={c.col}
              fill={t.lineSoft}
              width={200}
              height={30}
            />
          </div>
        </div>
      ))}
    </section>
  );
}

// ─── Network + airline comparison ────────────────────────────────────────────

function NetworkAndAirline({ t, flight }: { t: Tokens; flight: Flight }) {
  return (
    <section
      style={{
        padding: '40px 56px',
        display: 'grid',
        gridTemplateColumns: '1.3fr 1fr',
        gap: 32,
        borderBottom: `1px solid ${t.line}`,
      }}
    >
      <div
        style={{
          background: t.panel,
          border: `1px solid ${t.lineSoft}`,
          borderRadius: 4,
          padding: 24,
        }}
      >
        <div style={{ marginBottom: 18 }}>
          <div
            style={{
              fontFamily: monoFont,
              fontSize: 10,
              color: t.inkMuted,
              letterSpacing: '0.12em',
              textTransform: 'uppercase',
            }}
          >
            Network · simulated
          </div>
          <h3
            style={{
              fontFamily: serifFont,
              fontSize: 22,
              margin: '6px 0 0',
              fontWeight: 400,
              letterSpacing: '-0.01em',
              color: t.ink,
            }}
          >
            Average delays across the system
          </h3>
        </div>
        <NetworkMap t={t} height={290} />
      </div>

      <div
        style={{
          background: t.panel,
          border: `1px solid ${t.lineSoft}`,
          borderRadius: 4,
          padding: 24,
        }}
      >
        <div style={{ marginBottom: 18 }}>
          <div
            style={{
              fontFamily: monoFont,
              fontSize: 10,
              color: t.inkMuted,
              letterSpacing: '0.12em',
              textTransform: 'uppercase',
            }}
          >
            Carrier comparison
          </div>
          <h3
            style={{
              fontFamily: serifFont,
              fontSize: 22,
              margin: '6px 0 0',
              fontWeight: 400,
              letterSpacing: '-0.01em',
              color: t.ink,
            }}
          >
            {flight.from.code} → {flight.to.code} · 30d OTP
          </h3>
        </div>
        <AirlineComparison t={t} currentCode={flight.code} />
      </div>
    </section>
  );
}

// ─── Footer ───────────────────────────────────────────────────────────────────

function PageFooter({ t }: { t: Tokens }) {
  return (
    <footer
      style={{
        padding: '32px 56px 56px',
        display: 'flex',
        justifyContent: 'space-between',
        alignItems: 'center',
        fontSize: 12,
        color: t.inkMuted,
        fontFamily: 'Inter, sans-serif',
      }}
    >
      <div>
        © 2026 Holdline · ML predictions are probabilistic, not guarantees.
      </div>
      <div style={{ display: 'flex', gap: 18 }}>
        {['Status', 'Changelog', 'Pricing', 'API reference'].map((label) => (
          <span key={label} style={{ cursor: 'pointer' }}>
            {label}
          </span>
        ))}
      </div>
    </footer>
  );
}

// ─── Root component ───────────────────────────────────────────────────────────

function IndexAlt() {
  const [flight, setFlight] = useState<Flight>(FLIGHTS[0]);
  const [prediction, setPrediction] = useState<PredictResponse | null>(null);
  const [predicting, setPredicting] = useState(false);
  const { mode } = useColorScheme();
  const isDark = mode === 'dark';
  const t = isDark ? TOKENS.dark : TOKENS.light;

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
        isDark={isDark}
        flight={flight}
        onPickFlight={(f) => setFlight(f)}
        onPredict={(p) => setPrediction(p)}
        predicting={predicting}
        setPredicting={setPredicting}
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
