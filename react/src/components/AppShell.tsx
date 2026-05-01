import { CircularProgress, Fade } from '@mui/material';
import Box from '@mui/material/Box';
import Typography from '@mui/material/Typography';
import { Link, useRouterState } from '@tanstack/react-router';
import { Suspense, type ReactNode } from 'react';
import { ToggleThemeMode } from '~/components/ToggleThemeMode';
import { monoFont, serifFont } from '~/config/themePrimitives';

const NAV = [
  { label: 'Dashboard', to: '/' },
  { label: 'Predictions', to: '/predictions' },
  { label: 'Models', to: '/models' },
  { label: 'Drift', to: '/drift' },
  { label: 'Accuracy', to: '/accuracy' },
] as const;

export function AppShell({ children }: { children: ReactNode }) {
  return (
    <Box
      sx={{
        minHeight: '100vh',
        bgcolor: 'background.default',
        color: 'text.primary',
      }}
    >
      <AppHeader />
      <Box component='main'>
        <Suspense
          fallback={
            <Fade in={true}>
              <CircularProgress size={20} />
            </Fade>
          }
        >
          {children}
        </Suspense>
      </Box>
    </Box>
  );
}

function AppHeader() {
  return (
    <Box
      component='header'
      sx={{
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'space-between',
        px: 7,
        height: 56,
        borderBottom: '1px solid',
        borderColor: 'divider',
        position: 'sticky',
        top: 0,
        bgcolor: 'background.default',
        zIndex: 100,
      }}
    >
      <Box sx={{ display: 'flex', alignItems: 'center', gap: 4 }}>
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1.25 }}>
          <LogoMark />
          <Typography
            sx={{
              fontFamily: serifFont,
              fontSize: 17,
              fontWeight: 500,
              letterSpacing: '-0.01em',
              lineHeight: 1,
            }}
          >
            BMO Flight Prediction
          </Typography>
        </Box>
        <Box
          component='nav'
          sx={{ display: 'flex', alignItems: 'center', gap: 3 }}
        >
          {NAV.map(({ label, to }) => (
            <NavLink key={to} to={to} label={label} />
          ))}
        </Box>
      </Box>
      <Box sx={{ display: 'flex', alignItems: 'center', gap: 1.5 }}>
        <StatusPill />
        <ToggleThemeMode />
      </Box>
    </Box>
  );
}

function NavLink({ to, label }: { to: string; label: string }) {
  const { location } = useRouterState();
  const isActive =
    to === '/' ? location.pathname === '/' : location.pathname.startsWith(to);
  return (
    <Link to={to} style={{ textDecoration: 'none', color: 'inherit' }}>
      <Typography
        component='span'
        sx={{
          fontSize: 13,
          color: isActive ? 'text.primary' : 'text.secondary',
          fontWeight: isActive ? 500 : 400,
          cursor: 'pointer',
          '&:hover': { color: 'text.primary' },
          transition: 'color 0.15s',
        }}
      >
        {label}
      </Typography>
    </Link>
  );
}

function LogoMark() {
  return (
    <Box
      component='svg'
      width={16}
      height={16}
      viewBox='0 0 18 18'
      sx={{ color: 'text.primary', flexShrink: 0 }}
    >
      <rect x='1' y='8' width='16' height='2' fill='currentColor' />
      <rect x='8' y='1' width='2' height='16' fill='currentColor' />
      <circle
        cx='9'
        cy='9'
        r='2.5'
        fill='none'
        stroke='currentColor'
        strokeWidth='1.5'
      />
    </Box>
  );
}

function StatusPill() {
  return (
    <Box
      sx={{
        fontFamily: monoFont,
        fontSize: 11,
        color: 'text.disabled',
        px: 1.25,
        py: 0.5,
        border: '1px solid',
        borderColor: 'divider',
        borderRadius: '999px',
        display: 'flex',
        alignItems: 'center',
        gap: 0.75,
        userSelect: 'none',
      }}
    >
      <Box
        sx={{
          width: 6,
          height: 6,
          borderRadius: '50%',
          bgcolor: 'success.main',
          flexShrink: 0,
        }}
      />
      model v4.3.1 · live
    </Box>
  );
}
