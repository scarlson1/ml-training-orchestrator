type SparklineProps = {
  values: number[];
  color?: string;
  fillColor?: string;
  height?: number;
  width?: number;
};

export function Sparkline({
  values,
  color = 'currentColor',
  fillColor,
  height = 32,
  width = 120,
}: SparklineProps) {
  if (values.length < 2) return null;
  const max = Math.max(...values);
  const min = Math.min(...values);
  const range = max - min || 1;
  const norm = (v: number) => height - ((v - min) / range) * height;
  const step = width / (values.length - 1);
  const line = values
    .map((v, i) => `${i === 0 ? 'M' : 'L'} ${i * step} ${norm(v)}`)
    .join(' ');
  const area = `${line} L ${width} ${height} L 0 ${height} Z`;

  return (
    <svg
      width={width}
      height={height}
      style={{ display: 'block', overflow: 'visible' }}
    >
      {fillColor && <path d={area} fill={fillColor} />}
      <path d={line} stroke={color} strokeWidth='1.5' fill='none' />
    </svg>
  );
}
