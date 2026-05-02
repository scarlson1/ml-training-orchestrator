import { createFileRoute } from '@tanstack/react-router';
import { useState, useMemo, useEffect, useRef, Suspense } from 'react';
import Box from '@mui/material/Box';
import Typography from '@mui/material/Typography';
import { useSuspenseQuery } from '@tanstack/react-query';
import { useColorScheme } from '@mui/material/styles';
import { apiFetch } from '~/api';
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
      { name: 'Origin congestion', value: -0.12, detail: 'SFO ground stop risk · low' },
      { name: 'Destination weather', value: -0.08, detail: 'JFK · scattered TS after 18:00z' },
      { name: 'Carrier on-time history', value: +0.21, detail: 'AX route 7-day OTP 88%' },
      { name: 'Aircraft rotation', value: +0.05, detail: 'Inbound from PDX · on time' },
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
      { name: 'Origin congestion', value: -0.28, detail: 'ORD evening bank · high traffic' },
      { name: 'Destination weather', value: -0.04, detail: 'LHR · light rain, CAT I' },
      { name: 'Carrier on-time history', value: +0.11, detail: 'NB transatl. 30-day OTP 71%' },
      { name: 'Aircraft rotation', value: -0.14, detail: 'Inbound LAX delayed 18m' },
      { name: 'Day of week', value: 0, detail: 'Thursday · neutral' },
      { name: 'Time of day', value: -0.09, detail: 'Evening departure · congested' },
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
      { name: 'Destination weather', value: -0.34, detail: 'DEN · convective SIGMET, gusts 38kt' },
      { name: 'Carrier on-time history', value: +0.04, detail: 'MR 7-day OTP 74%' },
      { name: 'Aircraft rotation', value: -0.11, detail: 'Inbound MCO · delayed 22m' },
      { name: 'Day of week', value: -0.02, detail: 'Thursday · neutral' },
      { name: 'Time of day', value: -0.06, detail: 'Afternoon thunderstorm window' },
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
      { name: 'Origin congestion', value: +0.08, detail: 'NRT · light traffic' },
      { name: 'Destination weather', value: +0.06, detail: 'LAX · clear, marine layer 06z' },
      { name: 'Carrier on-time history', value: +0.23, detail: 'PE NRT-LAX 30-day OTP 93%' },
      { name: 'Aircraft rotation', value: +0.09, detail: 'Aircraft on stand 4h+' },
      { name: 'Day of week', value: +0.01, detail: 'Thursday · neutral' },
      { name: 'Time of day', value: +0.04, detail: 'Evening departure · favorable' },
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

// ─── Globe math ───────────────────────────────────────────────────────────────

const D2R = Math.PI / 180;

function project(lat: number, lon: number, rotLon: number, rotLat: number, R: number) {
  const phi = lat * D2R;
  const lam = (lon - rotLon) * D2R;
  const tilt = rotLat * D2R;
  const cosPhi = Math.cos(phi);
  const sinPhi = Math.sin(phi);
  const cosLam = Math.cos(lam);
  const sinLam = Math.sin(lam);
  const cosT = Math.cos(tilt);
  const sinT = Math.sin(tilt);
  const x = cosPhi * sinLam;
  const yRaw = sinPhi;
  const zRaw = cosPhi * cosLam;
  const y = yRaw * cosT - zRaw * sinT;
  const z = yRaw * sinT + zRaw * cosT;
  return { x: x * R, y: -y * R, z, visible: z > 0 };
}

function slerp(a: { lat: number; lon: number }, b: { lat: number; lon: number }, t: number) {
  const lat1 = a.lat * D2R, lon1 = a.lon * D2R;
  const lat2 = b.lat * D2R, lon2 = b.lon * D2R;
  const x1 = Math.cos(lat1) * Math.cos(lon1);
  const y1 = Math.cos(lat1) * Math.sin(lon1);
  const z1 = Math.sin(lat1);
  const x2 = Math.cos(lat2) * Math.cos(lon2);
  const y2 = Math.cos(lat2) * Math.sin(lon2);
  const z2 = Math.sin(lat2);
  const dot = Math.max(-1, Math.min(1, x1 * x2 + y1 * y2 + z1 * z2));
  const omega = Math.acos(dot);
  if (omega < 1e-6) return { lat: a.lat, lon: a.lon };
  const sinO = Math.sin(omega);
  const k1 = Math.sin((1 - t) * omega) / sinO;
  const k2 = Math.sin(t * omega) / sinO;
  return {
    lat: Math.asin(k1 * z1 + k2 * z2) / D2R,
    lon: Math.atan2(k1 * y1 + k2 * y2, k1 * x1 + k2 * x2) / D2R,
  };
}

// Rough continent polygons for land dot generation
const LAND_POLYGONS: [number, number][][] = [
  [[-168,68],[-150,72],[-100,72],[-78,68],[-60,52],[-58,46],[-66,44],[-70,42],[-76,38],[-82,30],[-90,26],[-98,22],[-110,22],[-118,32],[-124,40],[-128,52],[-138,60],[-156,62],[-168,68]],
  [[-46,82],[-22,82],[-18,72],[-32,60],[-50,62],[-56,68],[-46,82]],
  [[-82,12],[-72,12],[-58,8],[-46,0],[-36,-6],[-34,-22],[-44,-32],[-58,-40],[-66,-52],[-74,-54],[-72,-44],[-72,-30],[-78,-18],[-82,-6],[-82,12]],
  [[-10,58],[2,60],[14,66],[28,70],[32,62],[30,52],[26,44],[14,40],[2,40],[-10,44],[-12,52],[-10,58]],
  [[-18,32],[-2,36],[12,36],[26,34],[36,30],[44,12],[52,12],[50,-2],[40,-16],[34,-30],[20,-34],[14,-22],[8,-4],[-2,4],[-12,12],[-18,20],[-18,32]],
  [[28,70],[60,76],[100,78],[140,72],[160,64],[170,60],[150,50],[140,42],[130,34],[122,22],[110,18],[104,10],[100,2],[110,-6],[100,-12],[78,8],[68,22],[58,26],[46,34],[36,40],[30,52],[28,70]],
  [[96,4],[120,4],[140,-4],[150,-10],[130,-12],[110,-8],[100,-2],[96,4]],
  [[114,-12],[134,-12],[148,-18],[154,-26],[150,-38],[140,-38],[124,-34],[114,-22],[114,-12]],
];

function pointInPolygon(lat: number, lon: number, poly: [number, number][]) {
  let inside = false;
  for (let i = 0, j = poly.length - 1; i < poly.length; j = i++) {
    const [xi, yi] = poly[i];
    const [xj, yj] = poly[j];
    const intersect = yi > lat !== yj > lat && lon < ((xj - xi) * (lat - yi)) / (yj - yi + 1e-9) + xi;
    if (intersect) inside = !inside;
  }
  return inside;
}

function buildLandDots() {
  const dots: { lat: number; lon: number }[] = [];
  const step = 4;
  for (let lat = -82; lat <= 82; lat += step) {
    const lonStep = step / Math.max(0.15, Math.cos(lat * D2R));
    for (let lon = -180; lon <= 180; lon += lonStep) {
      for (const p of LAND_POLYGONS) {
        if (pointInPolygon(lat, lon, p)) {
          dots.push({ lat, lon });
          break;
        }
      }
    }
  }
  return dots;
}

const LAND_DOTS = buildLandDots();

const GLOBE_AIRPORTS = [
  { code: 'SFO', lat: 37.62, lon: -122.38 },
  { code: 'LAX', lat: 33.94, lon: -118.4 },
  { code: 'SEA', lat: 47.45, lon: -122.31 },
  { code: 'DEN', lat: 39.86, lon: -104.67 },
  { code: 'ORD', lat: 41.98, lon: -87.91 },
  { code: 'DFW', lat: 32.9, lon: -97.04 },
  { code: 'ATL', lat: 33.64, lon: -84.43 },
  { code: 'MIA', lat: 25.79, lon: -80.29 },
  { code: 'JFK', lat: 40.64, lon: -73.78 },
  { code: 'BOS', lat: 42.36, lon: -71.01 },
  { code: 'MEX', lat: 19.43, lon: -99.07 },
  { code: 'GRU', lat: -23.43, lon: -46.48 },
  { code: 'LHR', lat: 51.47, lon: -0.45 },
  { code: 'CDG', lat: 49.0, lon: 2.55 },
  { code: 'FRA', lat: 50.04, lon: 8.55 },
  { code: 'AMS', lat: 52.31, lon: 4.76 },
  { code: 'IST', lat: 41.28, lon: 28.74 },
  { code: 'DXB', lat: 25.25, lon: 55.36 },
  { code: 'SIN', lat: 1.36, lon: 103.99 },
  { code: 'HKG', lat: 22.31, lon: 113.92 },
  { code: 'NRT', lat: 35.77, lon: 140.39 },
  { code: 'SYD', lat: -33.94, lon: 151.18 },
  { code: 'JNB', lat: -26.13, lon: 28.24 },
];

const GLOBE_FLIGHTS = [
  { from: 'SFO', to: 'JFK', code: 'AX2104', status: 'ok' as const },
  { from: 'JFK', to: 'LHR', code: 'BE100', status: 'ok' as const },
  { from: 'ORD', to: 'LHR', code: 'NB418', status: 'warn' as const },
  { from: 'NRT', to: 'LAX', code: 'PE12', status: 'ok' as const },
  { from: 'ATL', to: 'DEN', code: 'MR906', status: 'bad' as const },
  { from: 'DXB', to: 'SIN', code: 'EK354', status: 'ok' as const },
  { from: 'CDG', to: 'JFK', code: 'AF22', status: 'ok' as const },
  { from: 'FRA', to: 'HKG', code: 'LH720', status: 'warn' as const },
  { from: 'SYD', to: 'LAX', code: 'QF11', status: 'ok' as const },
  { from: 'GRU', to: 'LHR', code: 'IB6824', status: 'warn' as const },
  { from: 'HKG', to: 'SFO', code: 'CX870', status: 'ok' as const },
  { from: 'IST', to: 'JFK', code: 'TK11', status: 'ok' as const },
];

// ─── Globe component ──────────────────────────────────────────────────────────

interface GlobeProps {
  isDark: boolean;
  size?: number;
}

function HoldlineGlobe({ isDark, size = 560 }: GlobeProps) {
  const [rot, setRot] = useState(0);
  const [tick, setTick] = useState(0);
  const [hovered, setHovered] = useState<string | null>(null);
  const rafRef = useRef<number>(0);
  const lastRef = useRef(performance.now());

  useEffect(() => {
    let alive = true;
    const loop = (t: number) => {
      if (!alive) return;
      const dt = (t - lastRef.current) / 1000;
      lastRef.current = t;
      setRot((r) => (r + dt * 4) % 360);
      setTick((k) => k + dt);
      rafRef.current = requestAnimationFrame(loop);
    };
    rafRef.current = requestAnimationFrame(loop);
    return () => {
      alive = false;
      cancelAnimationFrame(rafRef.current);
    };
  }, []);

  const tilt = -18;
  const R = size * 0.42;
  const cx = size / 2;
  const cy = size / 2;

  const pal = isDark
    ? {
        sphereTop: '#0d1830',
        sphereBot: '#060A18',
        sphereStroke: '#1B2A4A',
        dot: '#5B7DC2',
        dotDim: '#2B3D66',
        grid: '#1B2A4A',
        arcOk: '#6FE3A0',
        arcWarn: '#F0B86E',
        arcBad: '#FF7A66',
        city: '#FFFFFF',
        label: '#B8C4DD',
        labelDim: '#5B6B8E',
        glow: '#7DA8FF',
      }
    : {
        sphereTop: '#F4F2EC',
        sphereBot: '#E7E3D9',
        sphereStroke: '#D5D0C2',
        dot: '#8C8470',
        dotDim: '#C9C2AE',
        grid: '#D5D0C2',
        arcOk: '#1F7A3F',
        arcWarn: '#B5701B',
        arcBad: '#B23B2A',
        city: '#1A1A18',
        label: '#3A382F',
        labelDim: '#8F8C82',
        glow: '#1A1A18',
      };

  const dotsProj = LAND_DOTS.map((d) => project(d.lat, d.lon, rot, tilt, R));
  const frontDots = dotsProj.filter((d) => d.visible);
  const backDots = dotsProj.filter((d) => !d.visible);

  // Graticule
  const graticule: { pts: ReturnType<typeof project>[]; type: string }[] = [];
  for (let lat = -60; lat <= 60; lat += 30) {
    const pts = [];
    for (let lon = -180; lon <= 180; lon += 6) pts.push(project(lat, lon, rot, tilt, R));
    graticule.push({ pts, type: 'parallel' });
  }
  for (let lon = -180; lon < 180; lon += 30) {
    const pts = [];
    for (let lat = -85; lat <= 85; lat += 5) pts.push(project(lat, lon, rot, tilt, R));
    graticule.push({ pts, type: 'meridian' });
  }

  // Airport projections
  const airportProj = GLOBE_AIRPORTS.map((a) => ({ ...a, proj: project(a.lat, a.lon, rot, tilt, R) }));

  type ArcPoint = { x: number; y: number; z: number; u: number };
  interface FlightArc {
    from: string;
    to: string;
    code: string;
    status: 'ok' | 'warn' | 'bad';
    pathPoints: ArcPoint[];
    headIdx: number;
    trailStart: number;
    key: string;
  }

  // Flight arcs with animated head
  const flightArcs: FlightArc[] = GLOBE_FLIGHTS.flatMap((f, i) => {
    const a = GLOBE_AIRPORTS.find((x) => x.code === f.from);
    const b = GLOBE_AIRPORTS.find((x) => x.code === f.to);
    if (!a || !b) return [];
    const period = 7 + (i % 5);
    const offset = (i * 1.7) % period;
    const phase = ((tick + offset) % period) / period;
    const segments = 64;
    const pathPoints: ArcPoint[] = [];
    for (let s = 0; s <= segments; s++) {
      const u = s / segments;
      const ll = slerp(a, b, u);
      const p = project(ll.lat, ll.lon, rot, tilt, R);
      const lift = 1 + 0.18 * Math.sin(Math.PI * u);
      pathPoints.push({ x: p.x * lift, y: p.y * lift, z: p.z * lift, u });
    }
    const trailLen = 0.32;
    const headIdx = Math.floor(phase * (pathPoints.length - 1));
    const trailStart = Math.max(0, Math.floor((phase - trailLen) * (pathPoints.length - 1)));
    return [{ from: f.from, to: f.to, code: f.code, status: f.status, pathPoints, headIdx, trailStart, key: f.code + i }];
  });

  const arcColor = (status: 'ok' | 'warn' | 'bad') =>
    status === 'ok' ? pal.arcOk : status === 'warn' ? pal.arcWarn : pal.arcBad;

  return (
    <div style={{ position: 'relative', width: size, height: size, userSelect: 'none' }}>
      <svg
        width={size}
        height={size}
        viewBox={`0 0 ${size} ${size}`}
        style={{ display: 'block', overflow: 'visible' }}
      >
        <defs>
          <radialGradient id="alt-sphere" cx="40%" cy="35%" r="75%">
            <stop offset="0%" stopColor={pal.sphereTop} />
            <stop offset="80%" stopColor={pal.sphereBot} />
            <stop offset="100%" stopColor={isDark ? '#000' : pal.sphereBot} />
          </radialGradient>
          <radialGradient id="alt-glow" cx="50%" cy="50%" r="50%">
            <stop offset="80%" stopColor={pal.glow} stopOpacity="0" />
            <stop offset="92%" stopColor={pal.glow} stopOpacity={isDark ? '0.18' : '0.04'} />
            <stop offset="100%" stopColor={pal.glow} stopOpacity="0" />
          </radialGradient>
        </defs>

        <circle cx={cx} cy={cy} r={R * 1.18} fill="url(#alt-glow)" />
        <circle cx={cx} cy={cy} r={R} fill="url(#alt-sphere)" stroke={pal.sphereStroke} strokeWidth="1" />

        <g transform={`translate(${cx} ${cy})`}>
          {/* graticule */}
          {graticule.map((g, gi) => {
            const segs: ReturnType<typeof project>[][] = [];
            let cur: ReturnType<typeof project>[] = [];
            g.pts.forEach((p) => {
              if (!p.visible) {
                if (cur.length) { segs.push(cur); cur = []; }
              } else {
                cur.push(p);
              }
            });
            if (cur.length) segs.push(cur);
            return segs.map((seg, si) => (
              <polyline
                key={`g${gi}:${si}`}
                points={seg.map((p) => `${p.x},${p.y}`).join(' ')}
                stroke={pal.grid}
                strokeWidth="0.4"
                fill="none"
                opacity="0.55"
              />
            ));
          })}

          {/* back hemisphere dots */}
          {backDots.map((d, i) => (
            <circle key={`b${i}`} cx={d.x} cy={d.y} r="0.5" fill={pal.dotDim} opacity="0.18" />
          ))}

          {/* front hemisphere dots */}
          {frontDots.map((d, i) => (
            <circle key={`f${i}`} cx={d.x} cy={d.y} r="0.9" fill={pal.dot} opacity={0.35 + 0.55 * d.z} />
          ))}

          {/* flight arcs */}
          {flightArcs.map((arc) => {
            const col = arcColor(arc.status);
            const pts = arc.pathPoints;

            // Faint base path segments (front hemisphere only)
            const baseSegs: typeof pts[] = [];
            let bcur: typeof pts = [];
            pts.forEach((p) => {
              if (p.z > -0.05) bcur.push(p);
              else if (bcur.length) { baseSegs.push(bcur); bcur = []; }
            });
            if (bcur.length) baseSegs.push(bcur);

            // Glowing trail segments
            const trailPts = pts.slice(arc.trailStart, arc.headIdx + 1).filter((p) => p.z > -0.05);

            const head = pts[arc.headIdx];

            return (
              <g key={arc.key}>
                {baseSegs.map((seg, si) => (
                  <polyline
                    key={`ba${si}`}
                    points={seg.map((p) => `${p.x},${p.y}`).join(' ')}
                    stroke={col}
                    strokeWidth="0.6"
                    fill="none"
                    opacity="0.18"
                  />
                ))}
                {trailPts.length > 1 &&
                  trailPts.slice(1).map((p, j) => {
                    const a = trailPts[j];
                    const frac = (j + 1) / trailPts.length;
                    return (
                      <g key={`tr${j}`}>
                        <line x1={a.x} y1={a.y} x2={p.x} y2={p.y} stroke={col} strokeWidth="1.4" strokeLinecap="round" opacity={0.15 + 0.85 * frac} />
                        <line x1={a.x} y1={a.y} x2={p.x} y2={p.y} stroke={col} strokeWidth="3" strokeLinecap="round" opacity={(0.15 + 0.85 * frac) * 0.18} />
                      </g>
                    );
                  })}
                {head && head.z > 0 && (
                  <g>
                    <circle cx={head.x} cy={head.y} r="3.2" fill={col} opacity="0.18" />
                    <circle cx={head.x} cy={head.y} r="1.6" fill={col} />
                    <circle cx={head.x} cy={head.y} r="0.6" fill="#fff" />
                  </g>
                )}
              </g>
            );
          })}

          {/* airports */}
          {airportProj
            .filter((a) => a.proj.visible)
            .map((a) => {
              const p = a.proj;
              const isHov = hovered === a.code;
              const showLabel = isHov || ['SFO', 'JFK', 'LHR', 'NRT', 'DXB', 'SYD', 'GRU'].includes(a.code);
              return (
                <g
                  key={a.code}
                  onMouseEnter={() => setHovered(a.code)}
                  onMouseLeave={() => setHovered(null)}
                >
                  <circle cx={p.x} cy={p.y} r="1.6" fill={pal.city} />
                  <circle cx={p.x} cy={p.y} r="3.5" fill="none" stroke={pal.city} strokeWidth="0.6" opacity={isHov ? 0.9 : 0.35} />
                  {showLabel && (
                    <text
                      x={p.x + 6}
                      y={p.y + 2.5}
                      fontSize="9"
                      fill={isHov ? pal.label : pal.labelDim}
                      fontFamily={monoFont}
                      letterSpacing="0.06em"
                    >
                      {a.code}
                    </text>
                  )}
                </g>
              );
            })}
        </g>
      </svg>
    </div>
  );
}

// ─── Small atoms ──────────────────────────────────────────────────────────────

function Sparkline({ values, color, height = 28, width = 120, fill }: { values: number[]; color: string; height?: number; width?: number; fill?: string }) {
  const max = Math.max(...values);
  const min = Math.min(...values);
  const norm = (v: number) => height - ((v - min) / (max - min || 1)) * height;
  const step = width / (values.length - 1);
  const d = values.map((v, i) => `${i === 0 ? 'M' : 'L'} ${i * step} ${norm(v)}`).join(' ');
  const a = `${d} L ${width} ${height} L 0 ${height} Z`;
  return (
    <svg width={width} height={height} style={{ display: 'block' }}>
      {fill && <path d={a} fill={fill} />}
      <path d={d} stroke={color} strokeWidth="1.5" fill="none" />
    </svg>
  );
}

function ProbabilityArc({ prob, t, size = 200 }: { prob: number; t: Tokens; size?: number }) {
  const r = size / 2 - 18;
  const cx = size / 2;
  const cy = size / 2;
  const startA = Math.PI * 0.8;
  const endA = Math.PI * 0.2 + Math.PI * 2;
  const total = endA - startA;
  const ang = startA + total * prob;
  const polar = (a: number): [number, number] => [cx + r * Math.cos(a), cy + r * Math.sin(a)];
  const [x1, y1] = polar(startA);
  const [x2, y2] = polar(ang);
  const [bx, by] = polar(endA);
  const largeArc = ang - startA > Math.PI ? 1 : 0;
  return (
    <svg width={size} height={size} style={{ display: 'block' }}>
      <path
        d={`M ${x1} ${y1} A ${r} ${r} 0 1 1 ${bx} ${by}`}
        stroke={t.lineSoft}
        strokeWidth="2"
        fill="none"
      />
      <path
        d={`M ${x1} ${y1} A ${r} ${r} 0 ${largeArc} 1 ${x2} ${y2}`}
        stroke={t.ink}
        strokeWidth="2"
        fill="none"
        strokeLinecap="round"
      />
      {[0, 0.25, 0.5, 0.75, 1].map((p, i) => {
        const a = startA + total * p;
        const [tx, ty] = polar(a);
        const [tx2, ty2] = [cx + (r - 6) * Math.cos(a), cy + (r - 6) * Math.sin(a)];
        return <line key={i} x1={tx} y1={ty} x2={tx2} y2={ty2} stroke={t.line} strokeWidth="1" />;
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
        <div style={{ fontSize: 13, color: t.ink, fontWeight: 500 }}>{factor.name}</div>
        <div style={{ fontSize: 11, color: t.inkMuted, marginTop: 2 }}>{factor.detail}</div>
      </div>
      <div style={{ position: 'relative', height: 6, background: t.lineSoft, borderRadius: 1 }}>
        <div style={{ position: 'absolute', top: 0, bottom: 0, left: '50%', width: 1, background: t.line }} />
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
        {positive ? '+' : ''}{(v * 100).toFixed(0)}
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
  const areaD = points.map((p, i) => `${i === 0 ? 'M' : 'L'} ${p[0]} ${p[1]}`).join(' ') + ` L ${w} ${h} L 0 ${h} Z`;
  const lineD = points.map((p, i) => `${i === 0 ? 'M' : 'L'} ${p[0]} ${p[1]}`).join(' ');
  return (
    <div style={{ position: 'relative', width: '100%', height: h }}>
      <svg viewBox={`0 0 ${w} ${h}`} preserveAspectRatio="none" width="100%" height={h} style={{ display: 'block' }}>
        {[0, 25, 50, 75, 100].map((y) => (
          <line key={y} x1="0" x2={w} y1={h - (y / 100) * h} y2={h - (y / 100) * h} stroke={t.lineSoft} strokeWidth="0.3" />
        ))}
        <line x1="0" x2={w} y1={h - 0.8 * h} y2={h - 0.8 * h} stroke={t.line} strokeWidth="0.4" strokeDasharray="2 1.5" />
        <path d={areaD} fill={t.lineSoft} />
        <path d={lineD} stroke={t.ink} strokeWidth="0.6" fill="none" vectorEffect="non-scaling-stroke" />
        {points.map((p, i) => (
          <circle key={i} cx={p[0]} cy={p[1]} r="0.8" fill={t.bg} stroke={t.ink} strokeWidth="0.4" vectorEffect="non-scaling-stroke" />
        ))}
      </svg>
      <div style={{ position: 'absolute', right: 0, top: h - 0.8 * h - 10, fontSize: 10, color: t.inkMuted, fontFamily: monoFont }}>
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
      <svg viewBox="0 0 100 60" width="100%" height="100%" preserveAspectRatio="xMidYMid meet">
        {[10, 20, 30, 40, 50].map((y) => (
          <line key={y} x1="0" x2="100" y1={y} y2={y} stroke={t.line} strokeWidth="0.08" />
        ))}
        {[20, 40, 60, 80].map((x) => (
          <line key={x} x1={x} x2={x} y1="0" y2="60" stroke={t.line} strokeWidth="0.08" />
        ))}
        {[
          ['SFO', 'JFK'], ['LAX', 'ORD'], ['ATL', 'BOS'], ['DEN', 'ATL'],
          ['ORD', 'DCA'], ['SEA', 'MIA'], ['DFW', 'JFK'],
        ].map(([ac, bc], i) => {
          const A = NETWORK_AIRPORTS.find((n) => n.code === ac);
          const B = NETWORK_AIRPORTS.find((n) => n.code === bc);
          if (!A || !B) return null;
          const x1 = A.x * 100, y1 = A.y * 60;
          const x2 = B.x * 100, y2 = B.y * 60;
          const mx = (x1 + x2) / 2, my = (y1 + y2) / 2 - 8;
          return (
            <path key={i} d={`M ${x1} ${y1} Q ${mx} ${my} ${x2} ${y2}`} stroke={t.line} strokeWidth="0.15" fill="none" strokeDasharray="0.6 0.6" />
          );
        })}
        {NETWORK_AIRPORTS.map((n) => {
          const col = statusColor(n.status);
          return (
            <g key={n.code}>
              <circle cx={n.x * 100} cy={n.y * 60} r={1.2 + n.delay / 60} fill={col} opacity="0.18" />
              <circle cx={n.x * 100} cy={n.y * 60} r="0.7" fill={col} />
              <text x={n.x * 100 + 1.4} y={n.y * 60 + 0.5} fontSize="1.6" fill={t.inkSoft} fontFamily={monoFont}>{n.code}</text>
            </g>
          );
        })}
      </svg>
      <div style={{ position: 'absolute', top: 10, left: 10, fontSize: 10, color: t.inkMuted, fontFamily: monoFont, letterSpacing: '0.06em', textTransform: 'uppercase' }}>
        Live · simulated
      </div>
      <div style={{ position: 'absolute', bottom: 10, right: 12, display: 'flex', gap: 10, fontSize: 10, color: t.inkSoft, fontFamily: monoFont }}>
        {[['green', t.good, '<15m'], ['amber', t.warn, '15–30m'], ['red', t.bad, '30m+']].map(([, col, label]) => (
          <span key={label as string} style={{ display: 'flex', gap: 4, alignItems: 'center' }}>
            <span style={{ width: 6, height: 6, background: col as string, borderRadius: 6, display: 'inline-block' }} />
            {label}
          </span>
        ))}
      </div>
    </div>
  );
}

// ─── Airline comparison ───────────────────────────────────────────────────────

function AirlineComparison({ t, currentCode }: { t: Tokens; currentCode: string }) {
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
            <div style={{ width: 18, height: 18, borderRadius: 2, background: t.chipBg, display: 'grid', placeItems: 'center', fontSize: 9, fontFamily: monoFont, color: t.inkSoft, fontWeight: 600 }}>
              {a.code}
            </div>
            <div style={{ fontSize: 13, color: t.ink, fontWeight: isCurrent ? 600 : 400 }}>
              {a.airline}
              {isCurrent && (
                <span style={{ marginLeft: 8, fontSize: 10, color: t.accent, textTransform: 'uppercase', letterSpacing: '0.08em' }}>current</span>
              )}
            </div>
            <div style={{ position: 'relative', height: 4, background: t.lineSoft, borderRadius: 1 }}>
              <div style={{ position: 'absolute', left: 0, top: 0, bottom: 0, width: `${a.otp * 100}%`, background: t.ink, borderRadius: 1 }} />
            </div>
            <div style={{ fontFamily: monoFont, fontSize: 12, color: t.ink, textAlign: 'right' }}>{(a.otp * 100).toFixed(0)}%</div>
            <div style={{ fontFamily: monoFont, fontSize: 12, color: t.inkSoft, textAlign: 'right' }}>{a.avgDelay}m</div>
          </div>
        );
      })}
    </div>
  );
}

// ─── Flight switcher ──────────────────────────────────────────────────────────

function FlightSwitcher({ flights, current, onPick, t }: { flights: Flight[]; current: Flight; onPick: (f: Flight) => void; t: Tokens }) {
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

function KpiStrip({ t }: { t: Tokens }) {
  const { data: pred } = useSuspenseQuery({
    queryKey: ['predictions', 'today'],
    queryFn: () => apiFetch('/api/predictions/today').then((r) => r.json() as Promise<PredictionSummary>),
    staleTime: 60 * 60 * 1000,
  });
  const { data: drift } = useSuspenseQuery({
    queryKey: ['drift', 'summary'],
    queryFn: () => apiFetch('/api/drift/summary').then((r) => r.json() as Promise<DriftSummary>),
    staleTime: 60 * 60 * 1000,
  });

  const nFlights = pred?.n_flights_today ?? 0;
  const onTimePct = pred?.positive_rate_today != null ? (1 - pred.positive_rate_today) * 100 : null;
  const alerts = drift?.psi_breaches ?? 0;
  const modelVer = pred?.model_version ?? '—';

  const kpis = [
    { l: 'In flight today', v: nFlights ? nFlights.toLocaleString() : '—', s: 'flights scored' },
    { l: 'Predicted on-time', v: onTimePct != null ? `${onTimePct.toFixed(0)}%` : '—', s: '↓ live rate' },
    { l: 'PSI alerts', v: String(alerts), s: alerts > 0 ? 'drift detected' : 'all clear' },
    { l: 'Model version', v: modelVer ? `v${modelVer.slice(0, 6)}` : '—', s: 'champion' },
  ];

  return (
    <div style={{ display: 'flex', gap: 28, marginTop: 28 }}>
      {kpis.map((k, i) => (
        <div key={i} style={{ borderLeft: `1px solid ${t.line}`, paddingLeft: 14 }}>
          <div style={{ fontFamily: monoFont, fontSize: 9, color: t.inkMuted, letterSpacing: '0.12em', textTransform: 'uppercase' }}>{k.l}</div>
          <div style={{ fontFamily: serifFont, fontSize: 26, color: t.ink, letterSpacing: '-0.02em', marginTop: 2 }}>{k.v}</div>
          <div style={{ fontFamily: monoFont, fontSize: 10, color: t.inkMuted, marginTop: 2 }}>{k.s}</div>
        </div>
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

function HeroSection({ t, isDark, flight, onPickFlight, onPredict, predicting, setPredicting }: HeroProps) {
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
        }}
      >
        <HoldlineGlobe isDark={isDark} size={720} />
      </div>

      <div style={{ position: 'relative', display: 'grid', gridTemplateColumns: '1.1fr 1fr', gap: 64, alignItems: 'end', minHeight: 520 }}>
        <div>
          <div style={{ fontFamily: monoFont, fontSize: 11, color: t.inkMuted, letterSpacing: '0.12em', textTransform: 'uppercase', marginBottom: 16 }}>
            <span style={{ display: 'inline-block', width: 6, height: 6, borderRadius: 6, background: t.good, marginRight: 8, verticalAlign: 'middle' }} />
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
            <em style={{ fontStyle: 'italic', color: t.inkSoft }}>will</em> hold —
            <br />
            before it pushes back.
          </h1>
          <p style={{ marginTop: 20, fontSize: 15, color: t.inkSoft, maxWidth: 480, lineHeight: 1.55, fontFamily: 'Inter, sans-serif' }}>
            Holdline's ensemble model fuses METAR, TAF, ground-stop bulletins, fleet rotation,
            and 9 years of carrier OTP data into a calibrated probability — refreshed every 90 seconds.
          </p>

          <Suspense
            fallback={
              <div style={{ display: 'flex', gap: 28, marginTop: 28 }}>
                {[0, 1, 2, 3].map((i) => (
                  <div key={i} style={{ borderLeft: `1px solid ${t.line}`, paddingLeft: 14 }}>
                    <div style={{ fontFamily: monoFont, fontSize: 9, color: t.inkMuted, letterSpacing: '0.12em', textTransform: 'uppercase', marginBottom: 4 }}>loading</div>
                    <div style={{ fontFamily: serifFont, fontSize: 26, color: t.lineSoft }}>——</div>
                  </div>
                ))}
              </div>
            }
          >
            <KpiStrip t={t} />
          </Suspense>
        </div>

        {/* Prediction form */}
        <div style={{ background: t.panelAlt, border: `1px solid ${t.lineSoft}`, borderRadius: 4, padding: 24 }}>
          <div style={{ fontFamily: monoFont, fontSize: 10, color: t.inkMuted, letterSpacing: '0.1em', textTransform: 'uppercase', marginBottom: 14 }}>
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
            <div style={{ padding: '10px 14px', borderRight: `1px solid ${t.lineSoft}` }}>
              <div style={{ fontSize: 10, color: t.inkMuted, marginBottom: 2, fontFamily: 'Inter, sans-serif' }}>Carrier</div>
              <div style={{ fontFamily: monoFont, fontSize: 14, color: t.ink }}>{flight.code}</div>
            </div>
            <div style={{ padding: '10px 14px', borderRight: `1px solid ${t.lineSoft}` }}>
              <div style={{ fontSize: 10, color: t.inkMuted, marginBottom: 2, fontFamily: 'Inter, sans-serif' }}>Number</div>
              <div style={{ fontFamily: monoFont, fontSize: 14, color: t.ink }}>{flight.number}</div>
            </div>
            <div style={{ padding: '10px 14px', borderRight: `1px solid ${t.lineSoft}` }}>
              <div style={{ fontSize: 10, color: t.inkMuted, marginBottom: 2, fontFamily: 'Inter, sans-serif' }}>Route</div>
              <div style={{ fontFamily: monoFont, fontSize: 14, color: t.ink }}>{flight.from.code}–{flight.to.code}</div>
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
            <FlightSwitcher flights={FLIGHTS} current={flight} onPick={onPickFlight} t={t} />
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

function PredictionHeadline({ t, flight, onTimeProb, verdict, prediction }: PredHeadlineProps) {
  const delayP50 = prediction ? (prediction.delay_probability > 0.5 ? 30 : 5) : flight.delayMin.p50;
  const delayP90 = prediction ? (prediction.delay_probability > 0.5 ? 90 : 20) : flight.delayMin.p90;
  const cancelProb = flight.cancelProb;
  const confidence = Math.abs(onTimeProb - 0.5) * 2;

  return (
    <section style={{ padding: '40px 56px', borderBottom: `1px solid ${t.line}` }}>
      <div style={{ display: 'grid', gridTemplateColumns: '1.5fr 1fr 1fr 1fr', gap: 48, alignItems: 'start' }}>
        {/* flight identity */}
        <div>
          <div style={{ display: 'flex', alignItems: 'baseline', gap: 18, marginBottom: 8 }}>
            <div style={{ fontFamily: monoFont, fontSize: 13, color: t.inkSoft }}>{flight.code} {flight.number}</div>
            <div style={{ fontSize: 13, color: t.inkMuted, fontFamily: 'Inter, sans-serif' }}>{flight.airline} · {flight.aircraft}</div>
          </div>
          <div style={{ display: 'flex', alignItems: 'center', gap: 18 }}>
            <div style={{ fontFamily: serifFont, fontSize: 56, lineHeight: 1, letterSpacing: '-0.03em', color: t.ink }}>{flight.from.code}</div>
            <svg width="40" height="14" viewBox="0 0 40 14">
              <line x1="0" y1="7" x2="36" y2="7" stroke={t.inkSoft} strokeWidth="1" />
              <polyline points="32,3 36,7 32,11" fill="none" stroke={t.inkSoft} strokeWidth="1" />
            </svg>
            <div style={{ fontFamily: serifFont, fontSize: 56, lineHeight: 1, letterSpacing: '-0.03em', color: t.ink }}>{flight.to.code}</div>
          </div>
          <div style={{ marginTop: 8, fontSize: 13, color: t.inkSoft, fontFamily: 'Inter, sans-serif' }}>
            {flight.from.city} → {flight.to.city} · {flight.sched.date} · dep {flight.sched.dep} {flight.from.tz}
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
              <span style={{ width: 5, height: 5, borderRadius: '50%', background: t.accent, display: 'inline-block' }} />
              live · {prediction.model_version ? `v${prediction.model_version.slice(0, 6)}` : 'model'} · {prediction.features_complete ? 'features complete' : 'partial features'}
            </div>
          )}
        </div>

        {/* probability arc */}
        <div>
          <div style={{ fontFamily: monoFont, fontSize: 10, color: t.inkMuted, letterSpacing: '0.12em', textTransform: 'uppercase', marginBottom: 8 }}>
            On-time probability
          </div>
          <div style={{ position: 'relative', display: 'inline-block' }}>
            <ProbabilityArc prob={onTimeProb} t={t} size={180} />
            <div style={{ position: 'absolute', inset: 0, display: 'grid', placeItems: 'center', textAlign: 'center' }}>
              <div>
                <div style={{ fontFamily: serifFont, fontSize: 44, fontWeight: 400, lineHeight: 1, letterSpacing: '-0.02em', color: t.ink }}>
                  {(onTimeProb * 100).toFixed(0)}<span style={{ fontSize: 22, color: t.inkSoft }}>%</span>
                </div>
                <div style={{ fontSize: 11, color: verdict.color, marginTop: 6, fontWeight: 500, fontFamily: 'Inter, sans-serif' }}>{verdict.label}</div>
              </div>
            </div>
          </div>
        </div>

        {/* expected delay */}
        <div>
          <div style={{ fontFamily: monoFont, fontSize: 10, color: t.inkMuted, letterSpacing: '0.12em', textTransform: 'uppercase', marginBottom: 8 }}>
            Expected delay
          </div>
          <div style={{ display: 'flex', alignItems: 'baseline', gap: 6 }}>
            <div style={{ fontFamily: serifFont, fontSize: 44, lineHeight: 1, letterSpacing: '-0.02em', color: t.ink }}>{delayP50}</div>
            <div style={{ fontSize: 14, color: t.inkSoft, fontFamily: 'Inter, sans-serif' }}>min · p50</div>
          </div>
          <div style={{ marginTop: 16, fontSize: 12, color: t.inkSoft, fontFamily: 'Inter, sans-serif' }}>
            {[['p50', `+${delayP50}m`], ['p90', `+${delayP90}m`], ['cancel', `${(cancelProb * 100).toFixed(1)}%`]].map(([label, val]) => (
              <div key={label} style={{ display: 'flex', justifyContent: 'space-between', padding: '4px 0', borderBottom: `1px solid ${t.lineSoft}` }}>
                <span>{label}</span>
                <span style={{ fontFamily: monoFont }}>{val}</span>
              </div>
            ))}
          </div>
        </div>

        {/* model confidence */}
        <div>
          <div style={{ fontFamily: monoFont, fontSize: 10, color: t.inkMuted, letterSpacing: '0.12em', textTransform: 'uppercase', marginBottom: 8 }}>
            Model confidence
          </div>
          <div style={{ display: 'flex', alignItems: 'baseline', gap: 6 }}>
            <div style={{ fontFamily: serifFont, fontSize: 44, lineHeight: 1, letterSpacing: '-0.02em', color: t.ink }}>
              {confidence.toFixed(2)}
            </div>
          </div>
          <div style={{ marginTop: 16, fontSize: 12, color: t.inkSoft, lineHeight: 1.5, fontFamily: 'Inter, sans-serif' }}>
            {prediction ? `Scored live. Brier ~0.06. ${prediction.features_complete ? 'All features resolved.' : 'Some features missing.'}` : 'Calibrated against 14d holdout. Brier 0.061.'}
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
  const avgOtp = Math.round(flight.history.reduce((a, b) => a + b, 0) / flight.history.length);
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
      <div style={{ background: t.panel, border: `1px solid ${t.lineSoft}`, borderRadius: 4, padding: 24 }}>
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'baseline', marginBottom: 18 }}>
          <div>
            <div style={{ fontFamily: monoFont, fontSize: 10, color: t.inkMuted, letterSpacing: '0.12em', textTransform: 'uppercase' }}>Feature attribution</div>
            <h3 style={{ fontFamily: serifFont, fontSize: 22, margin: '6px 0 0', fontWeight: 400, letterSpacing: '-0.01em', color: t.ink }}>
              What's driving this prediction
            </h3>
          </div>
          <div style={{ fontSize: 11, color: t.inkMuted, fontFamily: monoFont }}>SHAP-equivalent · log-odds shift</div>
        </div>
        {flight.factors.map((f, i) => (
          <FactorBar key={i} factor={f} t={t} />
        ))}
      </div>

      <div style={{ background: t.panel, border: `1px solid ${t.lineSoft}`, borderRadius: 4, padding: 24 }}>
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'baseline', marginBottom: 18 }}>
          <div>
            <div style={{ fontFamily: monoFont, fontSize: 10, color: t.inkMuted, letterSpacing: '0.12em', textTransform: 'uppercase' }}>Route history · 14d</div>
            <h3 style={{ fontFamily: serifFont, fontSize: 22, margin: '6px 0 0', fontWeight: 400, letterSpacing: '-0.01em', color: t.ink }}>
              {flight.from.code} → {flight.to.code} on-time %
            </h3>
          </div>
          <div style={{ fontFamily: monoFont, fontSize: 12, color: t.ink }}>
            {avgOtp}<span style={{ color: t.inkMuted }}>%</span>
            <span style={{ marginLeft: 8, color: t.inkMuted }}>avg</span>
          </div>
        </div>
        <RouteHistoryChart flight={flight} t={t} />
        <div style={{ display: 'flex', justifyContent: 'space-between', marginTop: 12, fontFamily: monoFont, fontSize: 10, color: t.inkMuted }}>
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
    { l: 'Origin weather', code: flight.from.code, top: 'VFR · clear', sub: 'wind 240@8 · vis 10sm', spark: [82, 84, 86, 85, 88, 90, 89, 91], col: t.good },
    { l: 'Destination weather', code: flight.to.code, top: 'MVFR · scattered', sub: 'wind 290@14G22 · vis 6sm', spark: [88, 84, 80, 76, 72, 70, 68, 71], col: t.warn },
    { l: 'Origin congestion', code: flight.from.code, top: 'Normal', sub: 'taxi 18m · queue 4', spark: [12, 14, 16, 15, 18, 17, 19, 18], col: t.ink },
    { l: 'Dest congestion', code: flight.to.code, top: 'Elevated', sub: 'taxi 26m · queue 11', spark: [10, 12, 15, 18, 22, 26, 28, 30], col: t.warn },
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
        <div key={i} style={{ background: t.panel, border: `1px solid ${t.lineSoft}`, borderRadius: 4, padding: 18 }}>
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'baseline' }}>
            <div style={{ fontFamily: monoFont, fontSize: 10, color: t.inkMuted, letterSpacing: '0.1em', textTransform: 'uppercase' }}>{c.l}</div>
            <div style={{ fontFamily: monoFont, fontSize: 11, color: t.inkSoft }}>{c.code}</div>
          </div>
          <div style={{ fontFamily: serifFont, fontSize: 22, marginTop: 8, color: c.col, letterSpacing: '-0.01em' }}>{c.top}</div>
          <div style={{ fontFamily: monoFont, fontSize: 11, color: t.inkSoft, marginTop: 4 }}>{c.sub}</div>
          <div style={{ marginTop: 14 }}>
            <Sparkline values={c.spark} color={c.col} fill={t.lineSoft} width={200} height={30} />
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
      <div style={{ background: t.panel, border: `1px solid ${t.lineSoft}`, borderRadius: 4, padding: 24 }}>
        <div style={{ marginBottom: 18 }}>
          <div style={{ fontFamily: monoFont, fontSize: 10, color: t.inkMuted, letterSpacing: '0.12em', textTransform: 'uppercase' }}>Network · simulated</div>
          <h3 style={{ fontFamily: serifFont, fontSize: 22, margin: '6px 0 0', fontWeight: 400, letterSpacing: '-0.01em', color: t.ink }}>
            Average delays across the system
          </h3>
        </div>
        <NetworkMap t={t} height={290} />
      </div>

      <div style={{ background: t.panel, border: `1px solid ${t.lineSoft}`, borderRadius: 4, padding: 24 }}>
        <div style={{ marginBottom: 18 }}>
          <div style={{ fontFamily: monoFont, fontSize: 10, color: t.inkMuted, letterSpacing: '0.12em', textTransform: 'uppercase' }}>Carrier comparison</div>
          <h3 style={{ fontFamily: serifFont, fontSize: 22, margin: '6px 0 0', fontWeight: 400, letterSpacing: '-0.01em', color: t.ink }}>
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
      <div>© 2026 Holdline · ML predictions are probabilistic, not guarantees.</div>
      <div style={{ display: 'flex', gap: 18 }}>
        {['Status', 'Changelog', 'Pricing', 'API reference'].map((label) => (
          <span key={label} style={{ cursor: 'pointer' }}>{label}</span>
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

  const onTimeProb = prediction ? 1 - prediction.delay_probability : flight.onTimeProb;

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
