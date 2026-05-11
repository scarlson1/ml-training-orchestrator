import { Box, Stack, Typography, useTheme } from '@mui/material';
import { useSuspenseQuery } from '@tanstack/react-query';
import { carrierRouteHistoryOptions } from '~/api/queryOptions';
import { monoFont } from '~/config/themePrimitives';

export function CarrierDelayTrend({
  origin,
  dest,
  carrier,
  days = 30,
}: {
  origin: string;
  dest: string;
  carrier: string;
  days?: number;
}) {
  const p = useTheme().vars.palette;
  const { data } = useSuspenseQuery(
    carrierRouteHistoryOptions(origin, dest, carrier, days),
  );
  const rows = data?.rows ?? [];

  const w = 100;
  const h = 140;
  const MAX_DELAY_MIN = 90;

  if (rows.length < 2) {
    return (
      <Box
        sx={{
          height: h,
          display: 'grid',
          placeItems: 'center',
          color: p.text.disabled,
          fontFamily: monoFont,
          fontSize: 12,
        }}
      >
        No carrier history available
      </Box>
    );
  }

  const stepX = w / (rows.length - 1);

  const probaPoints = rows.map((r, i) => [
    i * stepX,
    h - r.avg_delay_proba * h,
  ]);

  const actualPoints = rows
    .map((r, i) =>
      r.avg_actual_delay_min != null
        ? [
            i * stepX,
            h - Math.min(r.avg_actual_delay_min / MAX_DELAY_MIN, 1) * h,
          ]
        : null,
    )
    .filter((pt): pt is [number, number] => pt !== null);

  const toPath = (pts: number[][]) =>
    pts.map(([x, y], i) => `${i === 0 ? 'M' : 'L'} ${x} ${y}`).join(' ');

  return (
    <>
      <Box sx={{ position: 'relative', width: '100%', height: h }}>
        <svg
          viewBox={`0 0 ${w} ${h}`}
          preserveAspectRatio='none'
          width='100%'
          height={h}
          style={{ display: 'block' }}
        >
          {[0, 25, 50, 75, 100].map((pct) => (
            <line
              key={pct}
              x1='0'
              x2={w}
              y1={h - (pct / 100) * h}
              y2={h - (pct / 100) * h}
              stroke={p.custom.lineSoft}
              strokeWidth='0.3'
            />
          ))}
          <line
            x1='0'
            x2={w}
            y1={h * 0.5}
            y2={h * 0.5}
            stroke={p.divider}
            strokeWidth='0.4'
            strokeDasharray='2 1.5'
          />
          {actualPoints.length >= 2 && (
            <path
              d={toPath(actualPoints)}
              stroke={p.warning.main}
              strokeWidth='0.6'
              fill='none'
              vectorEffect='non-scaling-stroke'
              strokeDasharray='1.5 1'
            />
          )}
          <path
            d={toPath(probaPoints)}
            stroke={p.text.primary}
            strokeWidth='0.6'
            fill='none'
            vectorEffect='non-scaling-stroke'
          />
          {probaPoints.map((pt, i) => (
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
            left: 0,
            top: h * 0.5 - 10,
            fontSize: 10,
            color: p.warning.main,
            fontFamily: monoFont,
            opacity: 0.6,
          }}
        >
          45m
        </Typography>
        <Typography
          sx={{
            position: 'absolute',
            right: 0,
            top: h * 0.5 - 10,
            fontSize: 10,
            color: p.text.disabled,
            fontFamily: monoFont,
          }}
        >
          50%
        </Typography>
      </Box>

      <Stack direction='row' spacing={2} sx={{ mt: '10px' }}>
        <Stack direction='row' spacing='6px' sx={{ alignItems: 'center' }}>
          <Box
            sx={{
              width: 16,
              height: 1.5,
              bgcolor: p.text.primary,
              borderRadius: '1px',
            }}
          />
          <Typography
            sx={{ fontFamily: monoFont, fontSize: 10, color: p.text.disabled }}
          >
            predicted delay prob
          </Typography>
        </Stack>
        <Stack direction='row' spacing='6px' sx={{ alignItems: 'center' }}>
          <Box
            sx={{
              width: 16,
              height: 0,
              borderTop: `1.5px dashed ${p.warning.main}`,
            }}
          />
          <Typography
            sx={{ fontFamily: monoFont, fontSize: 10, color: p.text.disabled }}
          >
            actual delay (norm. 90m)
          </Typography>
        </Stack>
      </Stack>

      <Stack
        direction='row'
        sx={{
          mt: '8px',
          fontFamily: monoFont,
          fontSize: 10,
          color: p.text.disabled,
          justifyContent: 'space-between',
        }}
      >
        <Typography
          sx={{ fontFamily: monoFont, fontSize: 10, color: p.text.disabled }}
        >
          30d prior
        </Typography>
        <Typography
          sx={{ fontFamily: monoFont, fontSize: 10, color: p.text.disabled }}
        >
          {data?.data_as_of ?? 'latest'}
        </Typography>
      </Stack>
      <Typography variant='caption' color='textSecondary'>
        Actual delay is normalized from 0 to 90 minutes (e.g. 45 minute delay =
        50% on the chart)
      </Typography>
    </>
  );
}
