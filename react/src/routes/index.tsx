// src/routes/index.tsx
import Card from '@mui/material/Card';
import CardContent from '@mui/material/CardContent';
import Typography from '@mui/material/Typography';
import { useSuspenseQuery } from '@tanstack/react-query';
import { createFileRoute } from '@tanstack/react-router';
import { apiFetch } from '~/api';

export const Route = createFileRoute('/')({
  component: Home,
});

// The "at a glance" health page. Champion model card, today's prediction count, PSI breach banner if any feature is currently breached.

function Home() {
  const { data } = useSuspenseQuery({
    queryKey: ['models'],
    queryFn: () => apiFetch(`/api/model-stats`).then((r) => r.json()),
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
  return <Typography>TODO: todays prediction count</Typography>;
}

function PsiBreachBanner() {
  return <Typography>TODO: PsiBreachBanner</Typography>;
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
