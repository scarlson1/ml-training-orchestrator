import { useEffect, useRef } from 'react';

import { monoFont } from '~/config/themePrimitives';

// ─── Globe math ───────────────────────────────────────────────────────────────

const D2R = Math.PI / 180;

function project(
  lat: number,
  lon: number,
  rotLon: number,
  rotLat: number,
  R: number,
) {
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

function slerp(
  a: { lat: number; lon: number },
  b: { lat: number; lon: number },
  t: number,
) {
  const lat1 = a.lat * D2R,
    lon1 = a.lon * D2R;
  const lat2 = b.lat * D2R,
    lon2 = b.lon * D2R;
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
  [
    [-168, 68],
    [-150, 72],
    [-100, 72],
    [-78, 68],
    [-60, 52],
    [-58, 46],
    [-66, 44],
    [-70, 42],
    [-76, 38],
    [-82, 30],
    [-90, 26],
    [-98, 22],
    [-110, 22],
    [-118, 32],
    [-124, 40],
    [-128, 52],
    [-138, 60],
    [-156, 62],
    [-168, 68],
  ],
  [
    [-46, 82],
    [-22, 82],
    [-18, 72],
    [-32, 60],
    [-50, 62],
    [-56, 68],
    [-46, 82],
  ],
  [
    [-82, 12],
    [-72, 12],
    [-58, 8],
    [-46, 0],
    [-36, -6],
    [-34, -22],
    [-44, -32],
    [-58, -40],
    [-66, -52],
    [-74, -54],
    [-72, -44],
    [-72, -30],
    [-78, -18],
    [-82, -6],
    [-82, 12],
  ],
  [
    [-10, 58],
    [2, 60],
    [14, 66],
    [28, 70],
    [32, 62],
    [30, 52],
    [26, 44],
    [14, 40],
    [2, 40],
    [-10, 44],
    [-12, 52],
    [-10, 58],
  ],
  [
    [-18, 32],
    [-2, 36],
    [12, 36],
    [26, 34],
    [36, 30],
    [44, 12],
    [52, 12],
    [50, -2],
    [40, -16],
    [34, -30],
    [20, -34],
    [14, -22],
    [8, -4],
    [-2, 4],
    [-12, 12],
    [-18, 20],
    [-18, 32],
  ],
  [
    [28, 70],
    [60, 76],
    [100, 78],
    [140, 72],
    [160, 64],
    [170, 60],
    [150, 50],
    [140, 42],
    [130, 34],
    [122, 22],
    [110, 18],
    [104, 10],
    [100, 2],
    [110, -6],
    [100, -12],
    [78, 8],
    [68, 22],
    [58, 26],
    [46, 34],
    [36, 40],
    [30, 52],
    [28, 70],
  ],
  [
    [96, 4],
    [120, 4],
    [140, -4],
    [150, -10],
    [130, -12],
    [110, -8],
    [100, -2],
    [96, 4],
  ],
  [
    [114, -12],
    [134, -12],
    [148, -18],
    [154, -26],
    [150, -38],
    [140, -38],
    [124, -34],
    [114, -22],
    [114, -12],
  ],
];

function pointInPolygon(lat: number, lon: number, poly: [number, number][]) {
  let inside = false;
  for (let i = 0, j = poly.length - 1; i < poly.length; j = i++) {
    const [xi, yi] = poly[i];
    const [xj, yj] = poly[j];
    const intersect =
      yi > lat !== yj > lat &&
      lon < ((xj - xi) * (lat - yi)) / (yj - yi + 1e-9) + xi;
    if (intersect) inside = !inside;
  }
  return inside;
}

// ─── Globe component (canvas — zero React re-renders during animation) ────────

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

interface GlobeProps {
  isDark: boolean;
  size?: number;
}

const KEY_AIRPORTS = new Set(['SFO', 'JFK', 'LHR', 'NRT', 'DXB', 'SYD', 'GRU']);

// Pre-compute great-circle path points for each flight (static, only depends on airport positions)
const GLOBE_ARC_PATHS: { lat: number; lon: number }[][] = GLOBE_FLIGHTS.map(
  (f) => {
    const a = GLOBE_AIRPORTS.find((x) => x.code === f.from);
    const b = GLOBE_AIRPORTS.find((x) => x.code === f.to);
    if (!a || !b) return [];
    const pts: { lat: number; lon: number }[] = [];
    for (let s = 0; s <= 64; s++) pts.push(slerp(a, b, s / 64));
    return pts;
  },
);

export function Globe({ isDark, size = 560 }: GlobeProps) {
  const canvasRef = useRef<HTMLCanvasElement>(null);
  const rotRef = useRef(0);
  const tickRef = useRef(0);
  const lastTimeRef = useRef(0);
  const hoveredRef = useRef<string | null>(null);
  const rafRef = useRef<number>(0);
  // Re-run draw loop when theme changes
  const isDarkRef = useRef(isDark);
  isDarkRef.current = isDark;

  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas) return;
    const ctx = canvas.getContext('2d')!;
    if (!ctx) return;

    const tilt = -18;
    const R = size * 0.42;
    const cx = size / 2;
    const cy = size / 2;

    function getPalette(dark: boolean) {
      return dark
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
            labelDim: '#8F8C82',
            glow: '#1A1A18',
          };
    }

    function draw(time: number) {
      const dt =
        lastTimeRef.current === 0 ? 0 : (time - lastTimeRef.current) / 1000;
      lastTimeRef.current = time;
      rotRef.current = (rotRef.current + dt * 4) % 360;
      tickRef.current += dt;

      const rot = rotRef.current;
      const tick = tickRef.current;
      const pal = getPalette(isDarkRef.current);

      ctx.clearRect(0, 0, size, size);

      // ── atmosphere glow ──
      const glowGrad = ctx.createRadialGradient(
        cx,
        cy,
        R * 0.8,
        cx,
        cy,
        R * 1.18,
      );
      glowGrad.addColorStop(0, 'transparent');
      glowGrad.addColorStop(0.7, 'transparent');
      glowGrad.addColorStop(
        1,
        isDarkRef.current ? `${pal.glow}30` : `${pal.glow}0A`,
      );
      ctx.fillStyle = glowGrad;
      ctx.beginPath();
      ctx.arc(cx, cy, R * 1.18, 0, Math.PI * 2);
      ctx.fill();

      // ── sphere ──
      const sphereGrad = ctx.createRadialGradient(
        cx * 0.8,
        cy * 0.7,
        0,
        cx,
        cy,
        R,
      );
      sphereGrad.addColorStop(0, pal.sphereTop);
      sphereGrad.addColorStop(0.8, pal.sphereBot);
      sphereGrad.addColorStop(1, isDarkRef.current ? '#000' : pal.sphereBot);
      ctx.beginPath();
      ctx.arc(cx, cy, R, 0, Math.PI * 2);
      ctx.fillStyle = sphereGrad;
      ctx.fill();
      ctx.strokeStyle = pal.sphereStroke;
      ctx.lineWidth = 1;
      ctx.stroke();

      // ── graticule ──
      ctx.strokeStyle = pal.grid;
      ctx.lineWidth = 0.4;
      ctx.globalAlpha = 0.55;
      for (let lat = -60; lat <= 60; lat += 30) {
        ctx.beginPath();
        let drawing = false;
        for (let lon = -180; lon <= 180; lon += 6) {
          const p = project(lat, lon, rot, tilt, R);
          if (!p.visible) {
            drawing = false;
            continue;
          }
          if (!drawing) {
            ctx.moveTo(cx + p.x, cy + p.y);
            drawing = true;
          } else ctx.lineTo(cx + p.x, cy + p.y);
        }
        ctx.stroke();
      }
      for (let lon = -180; lon < 180; lon += 30) {
        ctx.beginPath();
        let drawing = false;
        for (let lat = -85; lat <= 85; lat += 5) {
          const p = project(lat, lon, rot, tilt, R);
          if (!p.visible) {
            drawing = false;
            continue;
          }
          if (!drawing) {
            ctx.moveTo(cx + p.x, cy + p.y);
            drawing = true;
          } else ctx.lineTo(cx + p.x, cy + p.y);
        }
        ctx.stroke();
      }
      ctx.globalAlpha = 1;

      // ── land dots ──
      // Back hemisphere (dim)
      ctx.fillStyle = pal.dotDim;
      ctx.globalAlpha = 0.18;
      for (const d of LAND_DOTS) {
        const p = project(d.lat, d.lon, rot, tilt, R);
        if (p.visible) continue;
        ctx.beginPath();
        ctx.arc(cx + p.x, cy + p.y, 0.5, 0, Math.PI * 2);
        ctx.fill();
      }
      // Front hemisphere
      ctx.fillStyle = pal.dot;
      for (const d of LAND_DOTS) {
        const p = project(d.lat, d.lon, rot, tilt, R);
        if (!p.visible) continue;
        ctx.globalAlpha = 0.35 + 0.55 * p.z;
        ctx.beginPath();
        ctx.arc(cx + p.x, cy + p.y, 0.9, 0, Math.PI * 2);
        ctx.fill();
      }
      ctx.globalAlpha = 1;

      // ── flight arcs ──
      GLOBE_FLIGHTS.forEach((f, i) => {
        const lls = GLOBE_ARC_PATHS[i];
        if (!lls.length) return;
        const col =
          f.status === 'ok'
            ? pal.arcOk
            : f.status === 'warn'
              ? pal.arcWarn
              : pal.arcBad;
        const period = 7 + (i % 5);
        const offset = (i * 1.7) % period;
        const phase = ((tick + offset) % period) / period;
        const trailLen = 0.32;
        const nPts = lls.length; // 65 points
        const headIdx = Math.floor(phase * (nPts - 1));
        const trailStart = Math.max(
          0,
          Math.floor((phase - trailLen) * (nPts - 1)),
        );

        // Project arc points on the fly
        const proj = lls.map((ll, s) => {
          const u = s / (nPts - 1);
          const p = project(ll.lat, ll.lon, rot, tilt, R);
          const lift = 1 + 0.18 * Math.sin(Math.PI * u);
          return { x: p.x * lift, y: p.y * lift, z: p.z * lift };
        });

        // Faint base path
        ctx.strokeStyle = col;
        ctx.lineWidth = 0.6;
        ctx.globalAlpha = 0.18;
        ctx.beginPath();
        let drawing = false;
        for (const p of proj) {
          if (p.z <= -0.05) {
            drawing = false;
            continue;
          }
          if (!drawing) {
            ctx.moveTo(cx + p.x, cy + p.y);
            drawing = true;
          } else ctx.lineTo(cx + p.x, cy + p.y);
        }
        ctx.stroke();

        // Glowing trail
        for (let j = trailStart + 1; j <= headIdx; j++) {
          const a = proj[j - 1],
            b = proj[j];
          if (a.z <= -0.05 || b.z <= -0.05) continue;
          const frac = (j - trailStart) / Math.max(1, headIdx - trailStart);
          // Core line
          ctx.globalAlpha = 0.15 + 0.85 * frac;
          ctx.strokeStyle = col;
          ctx.lineWidth = 1.4;
          ctx.beginPath();
          ctx.moveTo(cx + a.x, cy + a.y);
          ctx.lineTo(cx + b.x, cy + b.y);
          ctx.stroke();
          // Bloom
          ctx.globalAlpha = (0.15 + 0.85 * frac) * 0.18;
          ctx.lineWidth = 3;
          ctx.stroke();
        }

        // Plane head
        const head = proj[headIdx];
        if (head && head.z > 0) {
          ctx.globalAlpha = 0.18;
          ctx.fillStyle = col;
          ctx.beginPath();
          ctx.arc(cx + head.x, cy + head.y, 3.2, 0, Math.PI * 2);
          ctx.fill();
          ctx.globalAlpha = 1;
          ctx.fillStyle = col;
          ctx.beginPath();
          ctx.arc(cx + head.x, cy + head.y, 1.6, 0, Math.PI * 2);
          ctx.fill();
          ctx.fillStyle = '#fff';
          ctx.beginPath();
          ctx.arc(cx + head.x, cy + head.y, 0.6, 0, Math.PI * 2);
          ctx.fill();
        }
        ctx.globalAlpha = 1;
      });

      // ── airports ──
      const hov = hoveredRef.current;
      ctx.font = `9px ${monoFont}`;
      for (const a of GLOBE_AIRPORTS) {
        const p = project(a.lat, a.lon, rot, tilt, R);
        if (!p.visible) continue;
        const isHov = hov === a.code;

        // Outer ring
        ctx.strokeStyle = pal.city;
        ctx.lineWidth = 0.6;
        ctx.globalAlpha = isHov ? 0.9 : 0.35;
        ctx.beginPath();
        ctx.arc(cx + p.x, cy + p.y, 3.5, 0, Math.PI * 2);
        ctx.stroke();

        // Center dot
        ctx.globalAlpha = 1;
        ctx.fillStyle = pal.city;
        ctx.beginPath();
        ctx.arc(cx + p.x, cy + p.y, 1.6, 0, Math.PI * 2);
        ctx.fill();

        // Label
        if (isHov || KEY_AIRPORTS.has(a.code)) {
          ctx.fillStyle = isHov ? pal.city : pal.labelDim;
          ctx.globalAlpha = isHov ? 1 : 0.8;
          ctx.fillText(a.code, cx + p.x + 6, cy + p.y + 3);
        }
      }
      ctx.globalAlpha = 1;

      rafRef.current = requestAnimationFrame(draw);
    }

    // Mouse hover — update ref, no setState
    const onMouseMove = (e: MouseEvent) => {
      const rect = canvas.getBoundingClientRect();
      const mx = (e.clientX - rect.left) * (size / rect.width);
      const my = (e.clientY - rect.top) * (size / rect.height);
      const rot = rotRef.current;
      let found: string | null = null;
      for (const a of GLOBE_AIRPORTS) {
        const p = project(a.lat, a.lon, rot, tilt, R);
        if (!p.visible) continue;
        const dx = mx - (cx + p.x);
        const dy = my - (cy + p.y);
        if (dx * dx + dy * dy < 64) {
          found = a.code;
          break;
        }
      }
      hoveredRef.current = found;
    };
    const onMouseLeave = () => {
      hoveredRef.current = null;
    };
    canvas.addEventListener('mousemove', onMouseMove);
    canvas.addEventListener('mouseleave', onMouseLeave);

    rafRef.current = requestAnimationFrame(draw);
    return () => {
      cancelAnimationFrame(rafRef.current);
      canvas.removeEventListener('mousemove', onMouseMove);
      canvas.removeEventListener('mouseleave', onMouseLeave);
    };
  }, [size]); // only re-bind on size change; isDark is read via ref each frame

  return (
    <canvas
      ref={canvasRef}
      width={size}
      height={size}
      style={{ display: 'block', userSelect: 'none' }}
    />
  );
}
