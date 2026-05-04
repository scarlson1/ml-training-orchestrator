import { Box, Paper, Stack, Typography } from '@mui/material';
import { monoFont } from '~/config/themePrimitives';
import type { Tokens } from '~/config/tmpTheme';

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

export function NetworkMap({
  t,
  height = 280,
}: {
  t: Tokens;
  height?: number;
}) {
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
