// src/routes/index.tsx
import { OpenInNewRounded } from '@mui/icons-material';
import Button from '@mui/material/Button';
import Card from '@mui/material/Card';
import CardContent from '@mui/material/CardContent';
import Stack from '@mui/material/Stack';
import Typography from '@mui/material/Typography';
import { useSuspenseQuery } from '@tanstack/react-query';
import { createFileRoute } from '@tanstack/react-router';
import { apiFetch } from '~/api';

// const RouterButton = createLink(MUILink);

export const Route = createFileRoute('/')({
  component: Home,
});

// The "at a glance" health page. Champion model card, today's prediction count, PSI breach banner if any feature is currently breached.

const _links = [
  {
    name: 'Dagster',
    href: import.meta.env.VITE_DAGSTER_URL,
  },
  {
    name: 'Mlflow',
    href: import.meta.env.VITE_MLFLOW_DASHBOARD_URL,
  },
  {
    name: 'S3 Storage',
    href: import.meta.env.VITE_S3_DASHBOARD_URL,
  },
];

function Home() {
  return (
    <>
      <Stack direction='row' spacing={1}>
        {_links.map((l) => (
          <Button
            href={l.href}
            rel='noreferrer noopener'
            target='_blank'
            endIcon={<OpenInNewRounded fontSize='small' />}
            key={l.href}
          >
            {l.name}
          </Button>
        ))}
      </Stack>
      <ChampionModelCard />
      <TodayPredictionCount />
      <PsiBreachBanner />
    </>
  );
}

function ChampionModelCard() {
  return (
    <Card>
      <CardContent>
        <Typography>{`Model ${'TODO'}`}</Typography>
      </CardContent>
    </Card>
  );
}

function TodayPredictionCount() {
  const { data } = useSuspenseQuery({
    queryKey: ['predictions', 'today'],
    queryFn: () => apiFetch(`/api/predictions/today`).then((r) => r.json()),
    staleTime: 60 * 60 * 1000,
  });

  return (
    <>
      <Typography variant='h5' gutterBottom>
        Todays Prediction Summary
      </Typography>
      <Typography variant='body2' component='div'>
        <pre>{JSON.stringify(data, null, 2)}</pre>
      </Typography>
    </>
  );
}

function PsiBreachBanner() {
  const { data } = useSuspenseQuery({
    queryKey: ['drift', 'summary'],
    queryFn: () => apiFetch(`/api/drift/summary`).then((r) => r.json()),
    staleTime: 60 * 60 * 1000,
  });

  return (
    <>
      <Typography variant='h5' gutterBottom>
        Drift Summary
      </Typography>
      <Typography variant='body2' component='div'>
        <pre>{JSON.stringify(data, null, 2)}</pre>
      </Typography>
    </>
  );
}

// import * as fs from 'node:fs';

// const filePath = 'count.txt';

// async function readCount() {
//     return parseInt(
//         await fs.promises.readFile(filePath, 'utf-8').catch(() => '0'),
//     );
// }

// const getCount = createServerFn({
//     method: 'GET',
// }).handler(() => {
//     return readCount();
// });

// const updateCount = createServerFn({ method: 'POST' })
//     .inputValidator((d: number) => d)
//     .handler(async ({ data }) => {
//         const count = await readCount();
//         await fs.promises.writeFile(filePath, `${count + data}`);
//     });

// export const Route = createFileRoute('/')({
//     component: Home,
//     loader: async () => await getCount(),
// });

// function Home() {
//     const router = useRouter();
//     const state = Route.useLoaderData();

//     return (
//         <button
//             type='button'
//             onClick={() => {
//                 updateCount({ data: 1 }).then(() => {
//                     router.invalidate();
//                 });
//             }}
//         >
//             Add 1 to {state}?
//         </button>
//     );
// }
