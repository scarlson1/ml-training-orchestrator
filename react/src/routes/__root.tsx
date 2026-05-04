// src/routes/__root.tsx
/// <reference types="vite/client" />
import '@fontsource-variable/inter/index.css';
import '@fontsource-variable/jetbrains-mono/index.css';
import '@fontsource-variable/source-serif-4/index.css';

import CssBaseline from '@mui/material/CssBaseline';
import { ThemeProvider } from '@mui/material/styles';
import { TanStackDevtools } from '@tanstack/react-devtools';
import { QueryClientProvider } from '@tanstack/react-query';
import { ReactQueryDevtoolsPanel } from '@tanstack/react-query-devtools';
import { QueryClient } from '@tanstack/react-query';
import {
  createRootRouteWithContext,
  HeadContent,
  Outlet,
  Scripts,
} from '@tanstack/react-router';

interface RouterContext {
  queryClient: QueryClient;
}
import { TanStackRouterDevtoolsPanel } from '@tanstack/react-router-devtools';
import { type ReactNode, Suspense } from 'react';
import { ErrorBoundary } from 'react-error-boundary';
import { AppShell } from '~/components/AppShell';
import { ErrorFallback } from '~/components/ErrorFallback';
import { NotFound } from '~/components/NotFound';
import { queryClient } from '~/config/queryClient';
import { theme } from '~/config/theme';

export const Route = createRootRouteWithContext<RouterContext>()({
  head: () => ({
    meta: [
      { charSet: 'utf-8' },
      { name: 'viewport', content: 'width=device-width, initial-scale=1' },
      { title: 'Holdline · ML Flight Delay Prediction' },
    ],
    links: [
      {
        rel: 'icon',
        type: 'image/svg+xml',
        href: "data:image/svg+xml,<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 18 18'><rect x='1' y='8' width='16' height='2' fill='%232B6BFF'/><rect x='8' y='1' width='2' height='16' fill='%232B6BFF'/><circle cx='9' cy='9' r='2.5' fill='none' stroke='%232B6BFF' stroke-width='1.5'/></svg>",
      },
    ],
  }),
  component: RootComponent,
  notFoundComponent: NotFound,
});

function RootComponent() {
  return (
    <RootDocument>
      <ErrorBoundary FallbackComponent={ErrorFallback}>
        <Suspense fallback={null}>
          <Outlet />
        </Suspense>
      </ErrorBoundary>
    </RootDocument>
  );
}

function RootDocument({ children }: Readonly<{ children: ReactNode }>) {
  return (
    <html>
      <ThemeProvider theme={theme}>
        <QueryClientProvider client={queryClient}>
          <CssBaseline />
          <head>
            <HeadContent />
          </head>
          <body>
            <AppShell>{children}</AppShell>
            <Scripts />
            <TanStackDevtools
              plugins={[
                {
                  name: 'TanStack Query',
                  render: <ReactQueryDevtoolsPanel />,
                },
                {
                  name: 'TanStack Router',
                  render: <TanStackRouterDevtoolsPanel />,
                },
              ]}
            />
          </body>
        </QueryClientProvider>
      </ThemeProvider>
    </html>
  );
}
