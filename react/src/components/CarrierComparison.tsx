import {
  Avatar,
  Box,
  Chip,
  LinearProgress,
  Stack,
  Typography,
} from '@mui/material';
import { useSuspenseQuery } from '@tanstack/react-query';
import { carrierComparisonOptions } from '~/api/queryOptions';
import { monoFont } from '~/config/themePrimitives';
import type { Tokens } from '~/config/tmpTheme';

const AIRLINE_COMPARISON = [
  { carrier: 'Pacific', code: 'PE', otp: 0.93, avg_delay: 4 },
  { carrier: 'Axiom Air', code: 'AX', otp: 0.86, avg_delay: 8 },
  { carrier: 'Skybridge', code: 'SB', otp: 0.79, avg_delay: 14 },
  { carrier: 'Northbound', code: 'NB', otp: 0.71, avg_delay: 22 },
  { carrier: 'Meridian', code: 'MR', otp: 0.68, avg_delay: 28 },
];

export function CarrierComparison({
  t,
  // currentCode,
  currentCarrier,
  origin,
  dest,
  days = 14,
}: {
  t: Tokens;
  currentCarrier?: string | null;
  // currentCode: string;
  origin: string;
  dest: string;
  days?: number;
}) {
  const { data } = useSuspenseQuery(
    carrierComparisonOptions(origin, dest, days),
  );
  console.log('carrier comparison: ', data);

  const comparisonData =
    !data?.carriers?.length && import.meta.env.DEV
      ? AIRLINE_COMPARISON
      : data?.carriers || [];

  return (
    <Box>
      {comparisonData.map((a) => {
        // const isCurrent = a.code === currentCode;
        const isCurrent = a.carrier === currentCarrier;

        return (
          <Box
            key={a.carrier}
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
              {/* {a.code} */}
              TODO: carrier code
            </Avatar>
            <Stack direction='row' sx={{ alignItems: 'center' }}>
              <Typography
                sx={{
                  fontSize: 13,
                  color: t.ink,
                  fontWeight: isCurrent ? 600 : 400,
                }}
              >
                {a.carrier}
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
              {a.avg_delay}m
            </Typography>
          </Box>
        );
      })}
    </Box>
  );
}
