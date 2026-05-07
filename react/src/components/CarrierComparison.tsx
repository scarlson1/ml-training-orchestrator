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
import { useTheme } from '@mui/material/styles';
import { monoFont } from '~/config/themePrimitives';
import { carrierCodeToName } from '~/utils/misc';

const AIRLINE_COMPARISON = [
  { carrier: 'Delta', code: 'DL', otp: 0.93, avg_delay: 4 },
  { carrier: 'Southwest', code: 'SW', otp: 0.86, avg_delay: 8 },
  { carrier: 'American Airlines', code: 'AA', otp: 0.79, avg_delay: 14 },
  { carrier: 'Jet Blue', code: 'JB', otp: 0.71, avg_delay: 22 },
  { carrier: 'Spirit', code: 'SA', otp: 0.68, avg_delay: 28 },
];

export function CarrierComparison({
  // currentCode,
  currentCarrier,
  origin,
  dest,
  days = 30,
}: {
  currentCarrier?: string | null;
  origin: string;
  dest: string;
  days?: number;
}) {
  const p = useTheme().vars.palette;
  const { data } = useSuspenseQuery(
    carrierComparisonOptions(origin, dest, days),
  );
  // console.log('carrier comparison: ', data);

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
              borderBottom: `1px solid ${p.custom.lineSoft}`,
              opacity: isCurrent ? 1 : 0.78,
            }}
          >
            <Avatar
              variant='rounded'
              sx={{
                width: 18,
                height: 18,
                borderRadius: '2px',
                bgcolor: p.custom.chipBg,
                color: p.text.secondary,
                fontFamily: monoFont,
                fontSize: 9,
                fontWeight: 600,
              }}
            >
              {a.carrier}
            </Avatar>
            <Stack direction='row' sx={{ alignItems: 'center' }}>
              <Typography
                sx={{
                  fontSize: 13,
                  color: p.text.primary,
                  fontWeight: isCurrent ? 600 : 400,
                }}
              >
                {carrierCodeToName(a.carrier)}
              </Typography>
              {isCurrent && (
                <Chip
                  label='current'
                  size='small'
                  sx={{
                    ml: 1,
                    height: 16,
                    fontSize: 10,
                    color: p.primary.main,
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
                bgcolor: p.custom.lineSoft,
                '& .MuiLinearProgress-bar': {
                  bgcolor: p.text.primary,
                  borderRadius: '1px',
                },
              }}
            />
            <Typography
              sx={{
                fontFamily: monoFont,
                fontSize: 12,
                color: p.text.primary,
                textAlign: 'right',
              }}
            >
              {(a.otp * 100).toFixed(0)}%
            </Typography>
            <Typography
              sx={{
                fontFamily: monoFont,
                fontSize: 12,
                color: p.text.secondary,
                textAlign: 'right',
              }}
            >
              {Math.ceil(a.avg_delay)}m
            </Typography>
          </Box>
        );
      })}
      {data?.data_as_of && (
        <Typography
          sx={{
            mt: '10px',
            fontFamily: monoFont,
            fontSize: 10,
            color: p.text.disabled,
            letterSpacing: '0.08em',
          }}
        >
          Data as of {data.data_as_of}
        </Typography>
      )}
    </Box>
  );
}
