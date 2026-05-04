import { Box, Paper, Stack, Typography } from '@mui/material';
import { monoFont, serifFont } from '~/config/themePrimitives';
import { TOKENS } from '~/config/tmpTheme';
import { useResolvedMode } from '~/hooks/useResolvedMode';

interface StatCardProps {
  label: string;
  code: string;
  value: string | number | null | undefined;
  subtitle: string;
  spark: number[];
  color: string;
  fill: string;
}

export const StatCard = ({
  label,
  code,
  value,
  subtitle,
  spark,
  color,
  fill,
}: StatCardProps) => {
  const mode = useResolvedMode();
  const t = mode === 'dark' ? TOKENS.dark : TOKENS.light;

  return (
    <Paper
      variant='outlined'
      sx={{
        bgcolor: t.panel,
        borderColor: t.lineSoft,
        borderRadius: '4px',
        p: '18px',
      }}
    >
      <Stack
        direction='row'
        sx={{ justifyContent: 'space-between', alignItems: 'baseline' }}
      >
        <Typography
          sx={{
            fontFamily: monoFont,
            fontSize: 10,
            color: t.inkMuted,
            letterSpacing: '0.1em',
            textTransform: 'uppercase',
          }}
        >
          {label}
        </Typography>
        <Typography
          sx={{ fontFamily: monoFont, fontSize: 11, color: t.inkSoft }}
        >
          {code}
        </Typography>
      </Stack>
      <Typography
        sx={{
          fontFamily: serifFont,
          fontSize: 22,
          mt: 1,
          color,
          letterSpacing: '-0.01em',
        }}
      >
        {value}
      </Typography>
      <Typography
        sx={{
          fontFamily: monoFont,
          fontSize: 11,
          color: t.inkSoft,
          mt: '4px',
        }}
      >
        {subtitle}
      </Typography>
      <Box sx={{ mt: '14px' }}>
        <Sparkline values={spark} color={color} fill={fill} height={30} />
      </Box>
    </Paper>
  );
};

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
    <svg
      width='100%'
      height={height}
      viewBox={`0 0 ${width} ${height}`}
      preserveAspectRatio='none'
      style={{ display: 'block' }}
    >
      {fill && <path d={a} fill={fill} />}
      <path
        d={d}
        stroke={color}
        strokeWidth='1.5'
        fill='none'
        vectorEffect='non-scaling-stroke'
      />
    </svg>
  );
}
