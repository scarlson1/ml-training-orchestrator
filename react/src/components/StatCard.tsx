import { Box, Paper, Stack, Typography } from '@mui/material';
import { monoFont, serifFont } from '~/config/themePrimitives';
import { useResolvedMode } from '~/hooks/useResolvedMode';

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
