// src/router.tsx
import { createRouter } from '@tanstack/react-router';
import { queryClient } from './config/queryClient';
import { routeTree } from './routeTree.gen';

export function getRouter() {
  const router = createRouter({
    routeTree,
    scrollRestoration: true,
    context: { queryClient },
  });

  return router;
}
