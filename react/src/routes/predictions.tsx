import Typography from '@mui/material/Typography';
import { useSuspenseQuery } from '@tanstack/react-query';
import { createFileRoute } from '@tanstack/react-router';
import { apiFetch } from '~/api';

/*
Daily scoring summary: flights scored, positive rate, null feature rows. Helps catch upstream feature pipeline failures before they hit accuracy metrics.
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
      <Typography variant='body2' component='div'>
        <pre>{JSON.stringify(data, null, 2)}</pre>
      </Typography>
    </>
  );
}
