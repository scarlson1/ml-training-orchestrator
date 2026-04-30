import type { FallbackProps } from 'react-error-boundary';

// TODO: handle common errors with resets

export function ErrorFallback({ error }: FallbackProps) {
  // Call resetErrorBoundary() to reset the error boundary and retry the render.

  let msg =
    error instanceof Error ? error.message : 'An unknown error occurred.';

  return (
    <div role='alert'>
      <p>Something went wrong:</p>
      <pre style={{ color: 'red' }}>{msg}</pre>
    </div>
  );
}
