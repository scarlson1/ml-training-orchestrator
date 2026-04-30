import Typography from '@mui/material/Typography';
import { useSuspenseQuery } from '@tanstack/react-query';
import { createFileRoute } from '@tanstack/react-router';
import { apiFetch } from '~/api';

// Time series of ROC-AUC, F1, precision/recall per model version (once BTS actuals arrive ~60d later). Also shows positive_rate vs actual_positive_rate to surface label shift.

export const Route = createFileRoute('/accuracy')({
    component: Accuracy,
});

function Accuracy() {
    const { data } = useSuspenseQuery({
        queryKey: ['accuracy'],
        queryFn: () => apiFetch(`/api/accuracy`).then((r) => r.json()),
        staleTime: 60 * 60 * 1000, // report runs once daily
    });

    return (
        <>
            <Typography variant='h3' gutterBottom>
                Accuracy
            </Typography>
            <Typography variant='body2'>
                <pre>{JSON.stringify(data, null, 2)}</pre>
            </Typography>
        </>
    );
}
