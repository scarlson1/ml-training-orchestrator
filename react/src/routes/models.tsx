import Typography from '@mui/material/Typography';
import { useSuspenseQuery } from '@tanstack/react-query';
import { createFileRoute } from '@tanstack/react-router';
import { apiFetch } from '~/api';

// Model Registry
// Table of all MLflow model versions: version number, champion/challenger badge, ROC-AUC, dataset hash, registered date. Line chart of AUC across versions.

export const Route = createFileRoute('/models')({
  component: Models,
});

function Models() {
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
      <Typography variant='body2' component='div'>
        <pre>{JSON.stringify(data, null, 2)}</pre>
      </Typography>
    </>
  );
}
