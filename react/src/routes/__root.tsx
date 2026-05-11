// src/routes/__root.tsx
/// <reference types="vite/client" />
import '@fontsource-variable/inter/index.css';
import '@fontsource-variable/jetbrains-mono/index.css';
import '@fontsource-variable/source-serif-4/index.css';

import CssBaseline from '@mui/material/CssBaseline';
import { ThemeProvider } from '@mui/material/styles';
import { TanStackDevtools } from '@tanstack/react-devtools';
import { formDevtoolsPlugin } from '@tanstack/react-form-devtools';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { ReactQueryDevtoolsPanel } from '@tanstack/react-query-devtools';
import {
  createRootRouteWithContext,
  HeadContent,
  Outlet,
  Scripts,
} from '@tanstack/react-router';
import { TanStackRouterDevtoolsPanel } from '@tanstack/react-router-devtools';
import { type ReactNode, Suspense } from 'react';
import { ErrorBoundary } from 'react-error-boundary';
import { Toaster as Sonner } from 'sonner';
import { AppShell } from '~/components/AppShell';
import { ErrorFallback } from '~/components/ErrorFallback';
import { NotFound } from '~/components/NotFound';
import { queryClient } from '~/config/queryClient';
import { theme } from '~/config/theme';
import { useResolvedMode } from '~/hooks/useResolvedMode';

interface RouterContext {
  queryClient: QueryClient;
}

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
        href: '/favicon.svg',
      },
      {
        rel: 'icon',
        type: 'image/png',
        href: '/favicon.png',
      },
      {
        rel: 'apple-touch-icon',
        href: '/apple-touch-icon.png',
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

function SonnerToaster() {
  const mode = useResolvedMode();

  return <Sonner theme={mode} richColors closeButton />;
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
            <SonnerToaster />

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
                formDevtoolsPlugin(),
              ]}
            />
          </body>
        </QueryClientProvider>
      </ThemeProvider>
    </html>
  );
}
