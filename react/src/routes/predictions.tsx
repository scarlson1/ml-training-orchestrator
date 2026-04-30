import Typography from '@mui/material/Typography';
import { useSuspenseQuery } from '@tanstack/react-query';
import { createFileRoute } from '@tanstack/react-router';
import { apiFetch } from '~/api';

/*
Daily predictions summary

-- mart_predictions is a DuckDB view, so this needs DuckDB not Postgres.
-- Expose via a /api/predictions?start=&end= endpoint backed by DuckDB.
SELECT
  score_date,
  model_version,
  COUNT(*)                                       AS n_flights,
  AVG(predicted_delay_proba)                     AS avg_proba,
  AVG(predicted_is_delayed::int)                 AS positive_rate,
  COUNT(*) FILTER (WHERE actual_is_delayed IS NOT NULL) AS n_with_actuals
FROM mart_predictions
WHERE score_date >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY score_date, model_version
ORDER BY score_date DESC;

*/

export const Route = createFileRoute('/predictions')({
  component: RouteComponent,
});

function RouteComponent() {
  const { data } = useSuspenseQuery({
    queryKey: ['predictions'],
    queryFn: () => apiFetch(`/api/predictions`).then((r) => r.json()),
    staleTime: 60 * 60 * 1000, // report runs once daily
  });

  return (
    <>
      <Typography variant='h3' gutterBottom>
        Predictions
      </Typography>
      <Typography variant='body2'>
        <pre>{JSON.stringify(data, null, 2)}</pre>
      </Typography>
    </>
  );
}
