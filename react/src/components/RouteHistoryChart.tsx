import { Box, Stack, Typography, useTheme } from '@mui/material';
import { useSuspenseQuery } from '@tanstack/react-query';
import { routeHistoryOptions } from '~/api/queryOptions';
import { monoFont, serifFont } from '~/config/themePrimitives';

export function RouteHistoryChart({
  origin,
  dest,
  days = 14,
}: {
  origin: string;
  dest: string;
  days?: number;
}) {
  const p = useTheme().vars.palette;
  const { data: historyData } = useSuspenseQuery(
    routeHistoryOptions(origin, dest, days),
  );
  const data = historyData?.history || [];

  const avgOtp = Math.round(
    data.reduce((a, b) => a + b, 0) / (data.length || 1),
  );

  const w = 100;
  const h = 160;
  const validData = data.filter(Number.isFinite);

  if (validData.length < 2) {
    return (
      <Box
        sx={{
          position: 'relative',
          width: '100%',
          height: h,
          display: 'grid',
          placeItems: 'center',
          color: p.text.disabled,
          fontFamily: monoFont,
          fontSize: 12,
        }}
      >
        No route history available
      </Box>
    );
  }
  const stepX = w / (validData.length - 1);
  const points = validData.map((v, i) => [i * stepX, h - (v / 100) * h]);

  const lineD = points
    .map(([x, y], index) => `${index === 0 ? 'M' : 'L'} ${x} ${y}`)
    .join(' ');

  const areaD = `${lineD} L ${w} ${h} L 0 ${h} Z`;

  return (
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
              color: p.text.primary,
            }}
          >
            {origin} → {dest} on-time %
          </Typography>
        </Box>
        <Typography
          sx={{ fontFamily: monoFont, fontSize: 12, color: p.text.primary }}
        >
          {avgOtp}
          <Box component='span' sx={{ color: p.text.disabled }}>
            %
          </Box>
          <Box component='span' sx={{ ml: 1, color: p.text.disabled }}>
            avg
          </Box>
        </Typography>
      </Stack>
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
              stroke={p.custom.lineSoft}
              strokeWidth='0.3'
            />
          ))}
          <line
            x1='0'
            x2={w}
            y1={h - 0.8 * h}
            y2={h - 0.8 * h}
            stroke={p.divider}
            strokeWidth='0.4'
            strokeDasharray='2 1.5'
          />
          <path d={areaD} fill={p.custom.lineSoft} />
          <path
            d={lineD}
            stroke={p.text.primary}
            strokeWidth='0.6'
            fill='none'
            vectorEffect='non-scaling-stroke'
          />
          {points.map((pt, i) => (
            <circle
              key={i}
              cx={pt[0]}
              cy={pt[1]}
              r='0.8'
              fill={p.background.default}
              stroke={p.text.primary}
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
            color: p.text.disabled,
            fontFamily: monoFont,
          }}
        >
          80% target
        </Typography>
      </Box>
      <Stack
        direction='row'
        sx={{
          mt: '12px',
          fontFamily: monoFont,
          fontSize: 10,
          color: p.text.disabled,
          justifyContent: 'space-between',
        }}
      >
        <Typography
          sx={{ fontFamily: monoFont, fontSize: 10, color: p.text.disabled }}
        >
          14d prior
        </Typography>
        <Typography
          sx={{ fontFamily: monoFont, fontSize: 10, color: p.text.disabled }}
        >
          7d prior
        </Typography>
        <Typography
          sx={{ fontFamily: monoFont, fontSize: 10, color: p.text.disabled }}
        >
          {historyData?.data_as_of ?? 'latest'}
        </Typography>
      </Stack>
    </>
  );
}
