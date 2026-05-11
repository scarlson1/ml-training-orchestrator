import { Box, Paper, Stack, Typography } from '@mui/material';
import { useTheme } from '@mui/material/styles';
import { useSuspenseQuery } from '@tanstack/react-query';
import { useMemo } from 'react';
import { networkDelayOptions } from '~/api/queryOptions';
import { monoFont } from '~/config/themePrimitives';

// refactor to dynamically query airports based on the frequency of routes from the "active" origin airport?
// refactor steps below component

const LON_MIN = -125;
const LON_MAX = -66;

const LAT_MIN = 24;
const LAT_MAX = 50;

export function NetworkMap({
  height = 280,
  days = 7,
}: {
  height?: number;
  days?: number;
}) {
  const p = useTheme().vars.palette;
  const { data } = useSuspenseQuery(networkDelayOptions(days));

  const networkAirports = useMemo(() => {
    if (!data?.rows?.length) return [];
    return data.rows.map((a) => {
      const x =
        a.longitude != null
          ? (a?.longitude - LON_MIN) / (LON_MAX - LON_MIN)
          : 0; // ~-125 to -65 for CONUS
      const y =
        a.latitude != null
          ? 1 - (a?.latitude - LAT_MIN) / (LAT_MAX - LAT_MIN)
          : 0; // ~24 to 50

      return { ...a, x, y };
    });
  }, [data]);

  const networkPaths = useMemo(() => {
    const airports = networkAirports.filter(
      (airport) => Number.isFinite(airport.x) && Number.isFinite(airport.y),
    );

    const seen = new Set<string>();

    return airports.flatMap((airport) => {
      const nearest = airports
        .filter((candidate) => candidate.origin !== airport.origin)
        .map((candidate) => ({
          airport: candidate,
          distance:
            (airport.x - candidate.x) ** 2 + (airport.y - candidate.y) ** 2,
        }))
        .sort((a, b) => a.distance - b.distance)
        .slice(0, 2);

      return nearest.flatMap(({ airport: candidate }) => {
        const key = [airport.origin, candidate.origin].sort().join('-');

        if (seen.has(key)) return [];

        seen.add(key);

        return [
          {
            key,
            from: airport,
            to: candidate,
          },
        ];
      });
    });
  }, [networkAirports]);

  const statusColor = (s: 'green' | 'amber' | 'red') =>
    s === 'red'
      ? p.error.main
      : s === 'amber'
        ? p.warning.main
        : p.success.main;

  return (
    <Paper
      variant='outlined'
      sx={{
        position: 'relative',
        width: '100%',
        height,
        bgcolor: p.custom.panelAlt,
        borderColor: p.custom.lineSoft,
        borderRadius: '4px',
        overflow: 'hidden',
      }}
    >
      <svg
        viewBox='0 0 100 60'
        width='100%'
        height='100%'
        preserveAspectRatio='xMidYMid meet'
      >
        {/* grid dotted lines */}
        {[10, 20, 30, 40, 50].map((y) => (
          <line
            key={y}
            x1='0'
            x2='100'
            y1={y}
            y2={y}
            stroke={p.divider}
            strokeWidth='0.08'
          />
        ))}
        {[20, 40, 60, 80].map((x) => (
          <line
            key={x}
            x1={x}
            x2={x}
            y1='0'
            y2='60'
            stroke={p.divider}
            strokeWidth='0.08'
          />
        ))}

        {/* hub connection dotted lines */}
        {networkPaths.map(({ key, from, to }) => {
          const x1 = from.x * 100;
          const y1 = from.y * 60;
          const x2 = to.x * 100;
          const y2 = to.y * 60;

          const mx = (x1 + x2) / 2;
          const my = (y1 + y2) / 2 - 8;

          return (
            <path
              key={key}
              d={`M ${x1} ${y1} Q ${mx} ${my} ${x2} ${y2}`}
              stroke={p.divider}
              strokeWidth='0.15'
              fill='none'
              strokeDasharray='0.6 0.6'
            />
          );
        })}
        {networkAirports.map((n) => {
          const col = statusColor(
            n.status_indicator as 'red' | 'green' | 'amber',
          );
          return (
            <g key={n.origin}>
              <circle
                cx={n.x * 100}
                cy={n.y * 60}
                r={1.2 + n.avg_delay_min / 60}
                fill={col}
                opacity='0.18'
              />
              <circle cx={n.x * 100} cy={n.y * 60} r='0.7' fill={col} />
              <text
                x={n.x * 100 + 1.4}
                y={n.y * 60 + 0.5}
                fontSize='1.6'
                fill={p.text.secondary}
                fontFamily={monoFont}
              >
                {n.origin}
              </text>
            </g>
          );
        })}
      </svg>

      <Typography
        sx={{
          position: 'absolute',
          top: 10,
          left: 10,
          fontSize: 10,
          color: p.text.disabled,
          fontFamily: monoFont,
          letterSpacing: '0.06em',
          textTransform: 'uppercase',
        }}
      >
        Live · simulated
      </Typography>

      {/* legend */}
      <Stack
        direction='row'
        spacing='10px'
        sx={{ position: 'absolute', bottom: 10, right: 12 }}
      >
        {[
          [p.success.main, '<15m'],
          [p.warning.main, '15–30m'],
          [p.error.main, '30m+'],
        ].map(([col, label]) => (
          <Stack
            key={label}
            direction='row'
            spacing='4px'
            sx={{ alignItems: 'center' }}
          >
            <Box
              sx={{
                width: 6,
                height: 6,
                bgcolor: col,
                borderRadius: '50%',
                flexShrink: 0,
              }}
            />
            <Typography
              sx={{
                fontSize: 10,
                color: p.text.secondary,
                fontFamily: monoFont,
              }}
            >
              {label}
            </Typography>
          </Stack>
        ))}
      </Stack>
    </Paper>
  );
}

// Yes, I’d make it contextual. A static “top airports” network is fine as a dashboard decoration, but a map that changes when the user enters `BNA` is more useful: it turns the prediction input into “what does this origin’s route network look like right now?”

// I’d do a hybrid:

// - If the user has entered an origin, show the **10 most frequent routes from that origin**.
// - If no origin is selected yet, show the **10 most frequent routes system-wide**.
// - Keep this separate from `/predict`; the map should update from the selected/input origin, not only after a successful prediction.

// **Backend Schema**

// Add response models like:

// ```python
// class NetworkRoute(BaseModel):
//     origin: str
//     dest: str
//     origin_latitude: float | None
//     origin_longitude: float | None
//     dest_latitude: float | None
//     dest_longitude: float | None
//     otp: float
//     avg_delay_min: float
//     status_indicator: str
//     total_flights: int

// class NetworkRoutesResponse(BaseModel):
//     origin: str | None
//     routes: list[NetworkRoute]
//     data_as_of: str | None
// ```

// **Backend Endpoint**

// This assumes `airport_coordinates` is already registered in `get_duckdb()`.

// ```python
// @app.get('/api/network/routes', response_model=NetworkRoutesResponse, tags=['api'])
// async def network_routes(
//     origin: str | None = None,
//     days: int = 30,
//     limit: int = 10,
//     duck: duckdb.DuckDBPyConnection = Depends(get_duckdb),
// ) -> NetworkRoutesResponse:
//     normalized_origin = origin.upper() if origin else None

//     def _query() -> tuple[list[NetworkRoute], str | None]:
//         if normalized_origin:
//             where_clause = 'AND mp.origin = ?'
//             params: list[Any] = [days, normalized_origin, limit]
//         else:
//             where_clause = ''
//             params = [days, limit]

//         df = duck.execute(
//             f"""
//             WITH route_stats AS (
//                 SELECT
//                     mp.origin,
//                     mp.dest,

//                     AVG((1 - mp.predicted_is_delayed::int)) AS otp,

//                     AVG(
//                         CASE
//                             WHEN mp.predicted_is_delayed THEN mp.predicted_delay_proba * 60
//                             ELSE 0
//                         END
//                     ) AS avg_delay_min,

//                     CASE
//                         WHEN AVG(
//                             CASE
//                                 WHEN mp.predicted_is_delayed THEN mp.predicted_delay_proba * 60
//                                 ELSE 0
//                             END
//                         ) < 15 THEN 'green'

//                         WHEN AVG(
//                             CASE
//                                 WHEN mp.predicted_is_delayed THEN mp.predicted_delay_proba * 60
//                                 ELSE 0
//                             END
//                         ) < 30 THEN 'amber'

//                         ELSE 'red'
//                     END AS status_indicator,

//                     COUNT(*) AS total_flights,
//                     MAX(mp.score_date)::text AS data_as_of

//                 FROM mart_predictions mp
//                 WHERE CAST(mp.score_date AS DATE) >= (
//                     SELECT MAX(CAST(score_date AS DATE)) FROM mart_predictions
//                 ) - INTERVAL (? || ' days')
//                 AND mp.origin IS NOT NULL
//                 AND mp.dest IS NOT NULL
//                 {where_clause}
//                 GROUP BY mp.origin, mp.dest
//             )

//             SELECT
//                 rs.origin,
//                 rs.dest,

//                 origin_airport.latitude_deg AS origin_latitude,
//                 origin_airport.longitude_deg AS origin_longitude,
//                 dest_airport.latitude_deg AS dest_latitude,
//                 dest_airport.longitude_deg AS dest_longitude,

//                 rs.otp,
//                 rs.avg_delay_min,
//                 rs.status_indicator,
//                 rs.total_flights,
//                 rs.data_as_of

//             FROM route_stats rs
//             LEFT JOIN airport_coordinates origin_airport
//                 ON rs.origin = origin_airport.iata_code
//             LEFT JOIN airport_coordinates dest_airport
//                 ON rs.dest = dest_airport.iata_code
//             ORDER BY rs.total_flights DESC
//             LIMIT ?
//             """,
//             params,
//         ).df()

//         rows = cast(list[dict[str, Any]], df.to_dict('records'))
//         data_as_of = rows[0]['data_as_of'] if rows else None

//         routes = [
//             NetworkRoute(
//                 origin=r['origin'],
//                 dest=r['dest'],
//                 origin_latitude=r['origin_latitude'],
//                 origin_longitude=r['origin_longitude'],
//                 dest_latitude=r['dest_latitude'],
//                 dest_longitude=r['dest_longitude'],
//                 otp=r['otp'],
//                 avg_delay_min=r['avg_delay_min'],
//                 status_indicator=r['status_indicator'],
//                 total_flights=r['total_flights'],
//             )
//             for r in rows
//         ]

//         return routes, data_as_of

//     result, data_as_of = await asyncio.to_thread(_query)

//     return NetworkRoutesResponse(
//         origin=normalized_origin,
//         routes=result,
//         data_as_of=data_as_of,
//     )
// ```

// **Frontend Query Option**

// ```ts
// export interface NetworkRoute {
//   origin: string;
//   dest: string;
//   origin_latitude: number | null;
//   origin_longitude: number | null;
//   dest_latitude: number | null;
//   dest_longitude: number | null;
//   otp: number;
//   avg_delay_min: number;
//   status_indicator: 'green' | 'amber' | 'red';
//   total_flights: number;
// }

// export interface NetworkRoutesResponse {
//   origin: string | null;
//   routes: NetworkRoute[];
//   data_as_of: string | null;
// }

// export const networkRoutesOptions = ({
//   origin,
//   days = 30,
//   limit = 10,
// }: {
//   origin?: string | null;
//   days?: number;
//   limit?: number;
// }) =>
//   queryOptions({
//     queryKey: ['routes', 'network-routes', { origin, days, limit }],
//     queryFn: () =>
//       apiFetch<NetworkRoutesResponse>(
//         '/api/network/routes',
//         {},
//         {
//           ...(origin ? { origin } : {}),
//           days: days.toString(),
//           limit: limit.toString(),
//         },
//       ),
//     staleTime: 60 * 10 * 1000,
//     gcTime: 1000 * 60 * 10,
//   });
// ```

// **NetworkMap Props**

// ```tsx
// export function NetworkMap({
//   height = 280,
//   days = 30,
//   origin,
// }: {
//   height?: number;
//   days?: number;
//   origin?: string | null;
// }) {
//   const p = useTheme().vars.palette;
//   const { data } = useSuspenseQuery(
//     networkRoutesOptions({ origin, days, limit: 10 }),
//   );
// ```

// **Build Nodes From Routes**

// ```tsx
// const toPoint = (latitude: number | null, longitude: number | null) => {
//   if (latitude == null || longitude == null) return null;

//   return {
//     x: (longitude - LON_MIN) / (LON_MAX - LON_MIN),
//     y: 1 - (latitude - LAT_MIN) / (LAT_MAX - LAT_MIN),
//   };
// };

// const networkNodes = useMemo(() => {
//   const nodes = new Map<
//     string,
//     {
//       code: string;
//       x: number;
//       y: number;
//       status_indicator: 'green' | 'amber' | 'red';
//       avg_delay_min: number;
//     }
//   >();

//   for (const route of data.routes) {
//     const originPoint = toPoint(
//       route.origin_latitude,
//       route.origin_longitude,
//     );
//     const destPoint = toPoint(route.dest_latitude, route.dest_longitude);

//     if (originPoint) {
//       nodes.set(route.origin, {
//         code: route.origin,
//         ...originPoint,
//         status_indicator: route.status_indicator,
//         avg_delay_min: route.avg_delay_min,
//       });
//     }

//     if (destPoint) {
//       nodes.set(route.dest, {
//         code: route.dest,
//         ...destPoint,
//         status_indicator: route.status_indicator,
//         avg_delay_min: route.avg_delay_min,
//       });
//     }
//   }

//   return [...nodes.values()];
// }, [data.routes]);
// ```

// **Draw Route Paths**

// Replace the synthetic path generation with:

// ```tsx
// {data.routes.map((route) => {
//   const from = toPoint(route.origin_latitude, route.origin_longitude);
//   const to = toPoint(route.dest_latitude, route.dest_longitude);

//   if (!from || !to) return null;

//   const x1 = from.x * 100;
//   const y1 = from.y * 60;
//   const x2 = to.x * 100;
//   const y2 = to.y * 60;

//   const mx = (x1 + x2) / 2;
//   const my = (y1 + y2) / 2 - 8;

//   return (
//     <path
//       key={`${route.origin}-${route.dest}`}
//       d={`M ${x1} ${y1} Q ${mx} ${my} ${x2} ${y2}`}
//       stroke={statusColor(route.status_indicator)}
//       strokeWidth={0.12 + Math.min(route.total_flights / 5000, 0.35)}
//       fill='none'
//       opacity='0.5'
//       strokeDasharray='0.6 0.6'
//     />
//   );
// })}
// ```

// **Draw Nodes**

// Update your node loop to use `networkNodes`:

// ```tsx
// {networkNodes.map((node) => {
//   const col = statusColor(node.status_indicator);

//   return (
//     <g key={node.code}>
//       <circle
//         cx={node.x * 100}
//         cy={node.y * 60}
//         r={1.2 + node.avg_delay_min / 60}
//         fill={col}
//         opacity='0.18'
//       />
//       <circle cx={node.x * 100} cy={node.y * 60} r='0.7' fill={col} />
//       <text
//         x={node.x * 100 + 1.4}
//         y={node.y * 60 + 0.5}
//         fontSize='1.6'
//         fill={p.text.secondary}
//         fontFamily={monoFont}
//       >
//         {node.code}
//       </text>
//     </g>
//   );
// })}
// ```

// **Pass Origin From Index**

// Where you render the map:

// ```tsx
// <NetworkMap height={290} origin={flight?.origin || null} />
// ```

// If `flight.origin` is only set after a successful prediction, the map updates after submit. If you want it to update as the user types, lift the form origin state up from `PredictDelay` into `index.tsx`, then pass that value instead.

// My recommendation: start with `flight?.origin` after prediction/sample selection. It is less jumpy and maps to the user’s current flight context.
