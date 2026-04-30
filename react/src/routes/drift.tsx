import Typography from '@mui/material/Typography';
import { useSuspenseQuery } from '@tanstack/react-query';
import { createFileRoute } from '@tanstack/react-router';
import { apiFetch } from '~/api';

/*
Drift heatmap data (features × dates):

SELECT
  report_date,
  feature_name,
  psi_score,
  kl_divergence,
  rank,
  is_breached,
  model_version
FROM drift_metrics
WHERE report_date BETWEEN :start_date AND :end_date
ORDER BY report_date DESC, rank ASC;
*/

export const Route = createFileRoute('/drift')({
  validateSearch: (search: Record<string, unknown>) => ({
    start: (search.start as string) ?? undefined,
    end: (search.end as string) ?? undefined,
  }),
  component: RouteComponent,
});

function RouteComponent() {
  const { start, end } = Route.useSearch();

  const params = new URLSearchParams();
  if (start) params.set('start', start);
  if (end) params.set('end', end);

  const { data } = useSuspenseQuery({
    queryKey: ['drift-metrics', { start, end }],
    queryFn: () =>
      apiFetch(`/api/drift/metrics?${params}`).then((r) => r.json()),
    staleTime: 60 * 60 * 1000, // report runs once daily
  });

  return (
    <>
      <Typography variant='h3' gutterBottom>
        Drift Metrics
      </Typography>
      <Typography variant='body2'>
        <pre>{JSON.stringify(data, null, 2)}</pre>
      </Typography>
    </>
  );
}
