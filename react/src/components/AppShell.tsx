import { GitHub, OpenInNewRounded } from '@mui/icons-material';
import {
  CircularProgress,
  Fade,
  ListItemText,
  Skeleton,
  Stack,
} from '@mui/material';
import Box from '@mui/material/Box';
import Button from '@mui/material/Button';
import Menu from '@mui/material/Menu';
import MenuItem from '@mui/material/MenuItem';
import Typography from '@mui/material/Typography';
import { useSuspenseQuery } from '@tanstack/react-query';
import { Link, useRouterState } from '@tanstack/react-router';
import { Suspense, useState, type ReactNode } from 'react';
import { ErrorBoundary } from 'react-error-boundary';
import { modelInfoOptions } from '~/api/queryOptions';
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
        display: 'flex',
        flexDirection: 'column',
      }}
    >
      <AppHeader />
      <Box component='main' sx={{ flex: 1 }}>
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
      <AppFooter />
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
        // bgcolor: 'background.default',
        // bgcolor: (theme) => alpha(theme.vars.palette.background.default, 0.6),
        bgcolor: (theme) =>
          `color-mix(in srgb, ${(theme.vars || theme).palette.background.default}, transparent 92%)`,
        backdropFilter: 'blur(10px)',
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
            Flight Prediction
          </Typography>
        </Box>
        <Box
          component='nav'
          sx={{ display: 'flex', alignItems: 'center', gap: 3 }}
        >
          {NAV.map(({ label, to }) => (
            <NavLink key={to} to={to} label={label} />
          ))}
          <ExternalLinksMenu />
        </Box>
      </Box>
      <Box sx={{ display: 'flex', alignItems: 'center', gap: 1.5 }}>
        <ErrorBoundary fallback={<PillComponent env='--' />}>
          <Suspense
            fallback={<Skeleton variant='rounded' height={20} width={72} />}
          >
            <StatusPill />
          </Suspense>
        </ErrorBoundary>
        <ToggleThemeMode />
      </Box>
    </Box>
  );
}

function NavLink({ to, label }: { to: string; label: string }) {
  const { location } = useRouterState();
  const isActive =
    to === '/' ? location.pathname === '/' : location.pathname.startsWith(to);
  // better matching built into tanstack ??

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
  const { data } = useSuspenseQuery(modelInfoOptions);
  console.log(data);

  return <PillComponent version={data?.model_version} env='live' />;
}

function PillComponent({
  version,
  env = 'live',
}: {
  version?: string;
  env?: string | null;
}) {
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
      {`model v${version || '--'} · ${env}`}
    </Box>
  );
}

const links = [
  { label: 'Dagster', href: import.meta.env.VITE_DAGSTER_URL },
  { label: 'MLflow', href: import.meta.env.VITE_MLFLOW_DASHBOARD_URL },
  { label: 'S3 Storage', href: import.meta.env.VITE_S3_DASHBOARD_URL },
];

function ExternalLinksMenu() {
  const [anchorEl, setAnchorEl] = useState<null | HTMLElement>(null);
  const open = Boolean(anchorEl);

  const handleClick = (event: React.MouseEvent<HTMLButtonElement>) => {
    setAnchorEl(event.currentTarget);
  };
  const handleClose = () => {
    setAnchorEl(null);
  };

  return (
    <div>
      <Button
        id='ext-button'
        aria-controls={open ? 'basic-menu' : undefined}
        aria-haspopup='true'
        aria-expanded={open ? 'true' : undefined}
        onClick={handleClick}
        color='inherit'
        sx={{ color: 'text.secondary' }}
      >
        Monitoring
      </Button>
      <Menu
        id='external-menu'
        anchorEl={anchorEl}
        open={open}
        onClose={handleClose}
        slotProps={{
          list: {
            'aria-labelledby': 'ext-button',
          },
        }}
      >
        {links.map((l) => (
          <MenuItem
            key={l.href}
            component='a'
            href={l.href}
            target='_blank'
            rel='noopener noreferrer'
            onClick={handleClose}
          >
            <ListItemText sx={{ mr: 2 }}>{l.label}</ListItemText>
            <Typography variant='body2' sx={{ color: 'text.secondary' }}>
              <OpenInNewRounded fontSize='inherit' />
            </Typography>
          </MenuItem>
        ))}
      </Menu>
    </div>
  );
}

// ─── Footer ───────────────────────────────────────────────────────────────────

function AppFooter() {
  return (
    <Box
      component='footer'
      sx={{
        // p: '32px 56px 56px',
        px: { xs: 2, sm: 4, md: 5 },
        py: { xs: 1.5, sm: 2 },
        display: 'flex',
        justifyContent: 'space-between',
        alignItems: 'center',
        borderTop: (theme) => `1px solid ${theme.palette.divider}`,
        bgcolor: (theme) =>
          `color-mix(in srgb, ${(theme.vars || theme).palette.background.default}, transparent 92%)`,
      }}
    >
      <Typography
        sx={{
          fontSize: 12,
          color: 'disabled',
          fontFamily: 'Inter, sans-serif',
        }}
      >
        © 2026 Holdline
      </Typography>
      <Stack direction='row' spacing='18px'>
        <Button
          component='a'
          href='https://github.com/scarlson1/ml-training-orchestrator'
          rel='noopener noreferrer'
          target='_blank'
          startIcon={<GitHub fontSize='small' />}
          // endIcon={'↗'}
          size='small'
          color='inherit'
        >
          Github
        </Button>
        {/* {['Status', 'Changelog', 'Pricing', 'API reference'].map((label) => (
          <Link
            key={label}
            underline='hover'
            sx={{
              fontSize: 12,
              color: p.text.disabled,
              cursor: 'pointer',
              fontFamily: 'Inter, sans-serif',
            }}
          >
            {label}
          </Link>
        ))} */}
      </Stack>
    </Box>
  );
}
