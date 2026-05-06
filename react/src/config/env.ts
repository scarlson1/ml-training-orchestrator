import { z } from 'zod';

const envSchema = z.object({
  AIRLABS_KEY: z.string(),
  //   NODE_ENV: z.enum(['development', 'production', 'test']),
});

const clientEnvSchema = z.object({
  VITE_APP_NAME: z.string(),
  VITE_API_URL: z.url(),
  VITE_DAGSTER_URL: z.url(),
  VITE_S3_DASHBOARD_URL: z.url(),
  VITE_MLFLOW_DASHBOARD_URL: z.url(),
});

// Validate server environment
// NOTE: Module-level parse runs at module load. Fine for Node.js;
// on Cloudflare Workers (and other edge runtimes) `process.env` is
// empty at module load, so wrap this in a function and call it
// inside `.handler()` instead:
//
//   export const getServerEnv = () => envSchema.parse(process.env)
//
// Then read `getServerEnv()` per-request from server functions/middleware.
export const serverEnv = envSchema.parse(process.env);

// Validate client environment (build-time, always safe)
export const clientEnv = clientEnvSchema.parse(import.meta.env);
