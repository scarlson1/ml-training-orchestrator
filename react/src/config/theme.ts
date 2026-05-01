import { createTheme } from '@mui/material/styles';
import {
  holdlineColorSchemes,
  holdlineShape,
  holdlineTypography,
} from '~/config/themePrimitives';

export const theme = createTheme({
  colorSchemes: holdlineColorSchemes,
  cssVariables: { colorSchemeSelector: 'data', cssVarPrefix: '' },
  shape: holdlineShape,
  typography: holdlineTypography,
  components: {
    MuiButtonBase: {
      defaultProps: {
        disableRipple: true,
      },
    },
    MuiButton: {
      styleOverrides: {
        root: {
          borderRadius: holdlineShape.borderRadius,
          boxShadow: 'none',
          '&:hover': {
            boxShadow: 'none',
          },
        },
      },
    },
    MuiCard: {
      styleOverrides: {
        root: {
          borderRadius: holdlineShape.borderRadius,
          boxShadow: 'none',
          backgroundImage: 'none',
        },
      },
    },
    MuiPaper: {
      styleOverrides: {
        root: {
          borderRadius: holdlineShape.borderRadius,
          backgroundImage: 'none',
        },
      },
    },
    MuiChip: {
      styleOverrides: {
        root: {
          borderRadius: 2,
          fontFamily: "'JetBrains Mono Variable', 'JetBrains Mono', monospace",
          fontSize: '0.6875rem',
          letterSpacing: '0.04em',
        },
      },
    },
    MuiTooltip: {
      styleOverrides: {
        tooltip: {
          borderRadius: holdlineShape.borderRadius,
          fontSize: '0.75rem',
        },
      },
    },
    MuiCssBaseline: {
      styleOverrides: {
        body: {
          WebkitFontSmoothing: 'antialiased',
          MozOsxFontSmoothing: 'grayscale',
        },
      },
    },
    MuiDivider: {
      styleOverrides: {
        root: ({ theme }) => ({
          borderColor: theme.palette.divider,
        }),
      },
    },
    MuiTableCell: {
      styleOverrides: {
        head: {
          fontFamily: "'JetBrains Mono Variable', 'JetBrains Mono', monospace",
          fontSize: '0.625rem',
          fontWeight: 500,
          letterSpacing: '0.12em',
          textTransform: 'uppercase',
        },
      },
    },
  },
});
