import Typography from '@mui/material/Typography';
import { useSuspenseQuery } from '@tanstack/react-query';
import { createFileRoute } from '@tanstack/react-router';
import { apiFetch } from '~/api';

// Model Registry
// Table of all MLflow model versions: version number, champion/challenger badge, ROC-AUC, dataset hash, registered date. Line chart of AUC across versions.

/*
Model registry with AUC trend:

-- All versions ordered newest first (cross-reference MLflow via its REST API
-- for run_id → AUC; or store AUC in a postgres model_versions table)
-- Using live_accuracy as a proxy until you have a model_versions table:
SELECT
  model_version,
  AVG(roc_auc)    AS avg_roc_auc,
  MAX(score_date) AS last_scored,
  SUM(n_flights)  AS total_flights_scored
FROM live_accuracy
GROUP BY model_version
ORDER BY MAX(score_date) DESC;
*/

const Models = () => {
  const { data } = useSuspenseQuery({
    queryKey: ['models'],
    queryFn: () => apiFetch(`/api/model-stats`).then((r) => r.json()),
    staleTime: 60 * 60 * 1000, // report runs once daily
  });

  return (
    <>
      <Typography variant='h3' gutterBottom>
        Models
      </Typography>
      <Typography variant='body2'>
        <pre>{JSON.stringify(data, null, 2)}</pre>
      </Typography>
    </>
  );
};

export const Route = createFileRoute('/models')({
  component: Models,
});
