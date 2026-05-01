# BMO Flight Delay Prediction React App

## Deploy

[Tanstack Start on Vercel](https://vercel.com/docs/frameworks/full-stack/tanstack-start)

```bash
pnpm i nitro
```

```typescript
// {...}
import { nitro } from 'nitro/vite';

export default defineConfig({
  plugins: [tanstackStart(), nitro(), viteReact()],
});
```

After deploy ensure FastAPI Cors policy is configure to accept requests from the Vercel domain.
