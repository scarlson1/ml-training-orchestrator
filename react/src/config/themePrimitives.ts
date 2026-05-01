import { alpha } from '@mui/material/styles';

// ─── Holdline editorial theme ─────────────────────────────────────────────────
// Warm parchment palette. Light+dark. Inter body, Source Serif 4 display,
// JetBrains Mono for numeric/data labels.

export const holdlineColorSchemes = {
  light: {
    palette: {
      primary: {
        main: '#2B6BFF',
        light: '#7DA8FF',
        dark: '#1A4FCC',
        contrastText: '#FBFAF7',
      },
      secondary: {
        main: '#5C5A52',
        light: '#8F8C82',
        dark: '#1A1A18',
        contrastText: '#FBFAF7',
      },
      success: {
        main: '#1F7A3F',
        light: '#67C28E',
        dark: '#0F4020',
        contrastText: '#FBFAF7',
      },
      warning: {
        main: '#B5701B',
        light: '#E8B065',
        dark: '#7A4A0F',
        contrastText: '#FBFAF7',
      },
      error: {
        main: '#B23B2A',
        light: '#E88370',
        dark: '#7A2518',
        contrastText: '#FBFAF7',
      },
      info: {
        main: '#2B6BFF',
        light: '#7DA8FF',
        dark: '#1A4FCC',
        contrastText: '#FBFAF7',
      },
      background: {
        default: '#FBFAF7',
        paper: '#FFFFFF',
      },
      text: {
        primary: '#1A1A18',
        secondary: '#5C5A52',
        disabled: '#8F8C82',
      },
      divider: '#E7E3D9',
      action: {
        hover: alpha('#1A1A18', 0.04),
        selected: alpha('#1A1A18', 0.08),
        disabled: alpha('#1A1A18', 0.26),
        disabledBackground: alpha('#1A1A18', 0.06),
      },
    },
  },
  dark: {
    palette: {
      primary: {
        main: '#7DA8FF',
        light: '#B3CCFF',
        dark: '#2B6BFF',
        contrastText: '#0F0F0E',
      },
      secondary: {
        main: '#A09D90',
        light: '#D4D1C6',
        dark: '#6D6A60',
        contrastText: '#0F0F0E',
      },
      success: {
        main: '#67C28E',
        light: '#A3DEB8',
        dark: '#1F7A3F',
        contrastText: '#0F0F0E',
      },
      warning: {
        main: '#E8B065',
        light: '#F5D4A8',
        dark: '#B5701B',
        contrastText: '#0F0F0E',
      },
      error: {
        main: '#E88370',
        light: '#F5B8AF',
        dark: '#B23B2A',
        contrastText: '#0F0F0E',
      },
      info: {
        main: '#7DA8FF',
        light: '#B3CCFF',
        dark: '#2B6BFF',
        contrastText: '#0F0F0E',
      },
      background: {
        default: '#0F0F0E',
        paper: '#151514',
      },
      text: {
        primary: '#F4F1E8',
        secondary: '#A09D90',
        disabled: '#6D6A60',
      },
      divider: '#26251F',
      action: {
        hover: alpha('#F4F1E8', 0.06),
        selected: alpha('#F4F1E8', 0.1),
        disabled: alpha('#F4F1E8', 0.3),
        disabledBackground: alpha('#F4F1E8', 0.08),
      },
    },
  },
};

export const holdlineShape = {
  borderRadius: 4,
};

export const holdlineTypography = {
  fontFamily:
    "'Inter Variable', Inter, -apple-system, BlinkMacSystemFont, sans-serif",
  h1: {
    fontFamily: "'Source Serif 4 Variable', 'Source Serif 4', Georgia, serif",
    fontWeight: 400,
    fontSize: '4rem',
    lineHeight: 0.98,
    letterSpacing: '-0.025em',
  },
  h2: {
    fontFamily: "'Source Serif 4 Variable', 'Source Serif 4', Georgia, serif",
    fontWeight: 400,
    fontSize: '2.75rem',
    lineHeight: 1.05,
    letterSpacing: '-0.02em',
  },
  h3: {
    fontFamily: "'Source Serif 4 Variable', 'Source Serif 4', Georgia, serif",
    fontWeight: 400,
    fontSize: '1.375rem',
    lineHeight: 1.2,
    letterSpacing: '-0.01em',
  },
  h4: {
    fontFamily: "'Source Serif 4 Variable', 'Source Serif 4', Georgia, serif",
    fontWeight: 400,
    fontSize: '1.125rem',
    lineHeight: 1.3,
    letterSpacing: '-0.01em',
  },
  h5: {
    fontFamily: "'Inter Variable', Inter, sans-serif",
    fontWeight: 600,
    fontSize: '0.9375rem',
    lineHeight: 1.4,
    letterSpacing: '-0.005em',
  },
  h6: {
    fontFamily: "'Inter Variable', Inter, sans-serif",
    fontWeight: 600,
    fontSize: '0.875rem',
    lineHeight: 1.4,
    letterSpacing: '-0.005em',
  },
  subtitle1: {
    fontFamily: "'Inter Variable', Inter, sans-serif",
    fontWeight: 500,
    fontSize: '0.9375rem',
    lineHeight: 1.55,
    color: '#5C5A52',
  },
  subtitle2: {
    fontFamily: "'JetBrains Mono Variable', 'JetBrains Mono', monospace",
    fontWeight: 500,
    fontSize: '0.6875rem',
    lineHeight: 1.4,
    letterSpacing: '0.08em',
    textTransform: 'uppercase' as const,
  },
  body1: {
    fontFamily: "'Inter Variable', Inter, sans-serif",
    fontWeight: 400,
    fontSize: '0.9375rem',
    lineHeight: 1.55,
  },
  body2: {
    fontFamily: "'Inter Variable', Inter, sans-serif",
    fontWeight: 400,
    fontSize: '0.8125rem',
    lineHeight: 1.5,
  },
  caption: {
    fontFamily: "'JetBrains Mono Variable', 'JetBrains Mono', monospace",
    fontWeight: 400,
    fontSize: '0.6875rem',
    lineHeight: 1.4,
    letterSpacing: '0.04em',
  },
  overline: {
    fontFamily: "'JetBrains Mono Variable', 'JetBrains Mono', monospace",
    fontWeight: 500,
    fontSize: '0.625rem',
    lineHeight: 1.4,
    letterSpacing: '0.12em',
    textTransform: 'uppercase' as const,
  },
  button: {
    fontFamily: "'Inter Variable', Inter, sans-serif",
    fontWeight: 500,
    fontSize: '0.8125rem',
    letterSpacing: '0em',
    textTransform: 'none' as const,
  },
};

// Convenience: the display / serif font for use in sx props
export const serifFont =
  "'Source Serif 4 Variable', 'Source Serif 4', Georgia, serif";
export const monoFont =
  "'JetBrains Mono Variable', 'JetBrains Mono', monospace";
