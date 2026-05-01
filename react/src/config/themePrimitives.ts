import { alpha } from '@mui/material/styles';

// ─── legacy/initial primitives (kept for reference) ───────────────────────────

export const brand = {
  50: 'hsl(210, 100%, 95%)',
  100: 'hsl(210, 100%, 92%)',
  200: 'hsl(210, 100%, 80%)',
  300: 'hsl(210, 100%, 65%)',
  400: 'hsl(210, 98%, 48%)',
  500: 'hsl(210, 98%, 42%)',
  600: 'hsl(210, 98%, 55%)',
  700: 'hsl(210, 100%, 35%)',
  800: 'hsl(210, 100%, 16%)',
  900: 'hsl(210, 100%, 21%)',
};

export const gray = {
  50: 'hsl(220, 35%, 97%)',
  100: 'hsl(220, 30%, 94%)',
  200: 'hsl(220, 20%, 88%)',
  300: 'hsl(220, 20%, 80%)',
  400: 'hsl(220, 20%, 65%)',
  500: 'hsl(220, 20%, 42%)',
  600: 'hsl(220, 20%, 35%)',
  700: 'hsl(220, 20%, 25%)',
  800: 'hsl(220, 30%, 6%)',
  900: 'hsl(220, 35%, 3%)',
};

export const green = {
  50: 'hsl(120, 80%, 98%)',
  100: 'hsl(120, 75%, 94%)',
  200: 'hsl(120, 75%, 87%)',
  300: 'hsl(120, 61%, 77%)',
  400: 'hsl(120, 44%, 53%)',
  500: 'hsl(120, 59%, 30%)',
  600: 'hsl(120, 70%, 25%)',
  700: 'hsl(120, 75%, 16%)',
  800: 'hsl(120, 84%, 10%)',
  900: 'hsl(120, 87%, 6%)',
};

export const orange = {
  50: 'hsl(45, 100%, 97%)',
  100: 'hsl(45, 92%, 90%)',
  200: 'hsl(45, 94%, 80%)',
  300: 'hsl(45, 90%, 65%)',
  400: 'hsl(45, 90%, 40%)',
  500: 'hsl(45, 90%, 35%)',
  600: 'hsl(45, 91%, 25%)',
  700: 'hsl(45, 94%, 20%)',
  800: 'hsl(45, 95%, 16%)',
  900: 'hsl(45, 93%, 12%)',
};

export const red = {
  50: 'hsl(0, 100%, 97%)',
  100: 'hsl(0, 92%, 90%)',
  200: 'hsl(0, 94%, 80%)',
  300: 'hsl(0, 90%, 65%)',
  400: 'hsl(0, 90%, 40%)',
  500: 'hsl(0, 90%, 30%)',
  600: 'hsl(0, 91%, 25%)',
  700: 'hsl(0, 94%, 18%)',
  800: 'hsl(0, 95%, 12%)',
  900: 'hsl(0, 93%, 6%)',
};

export const colorSchemesInitial = {
  light: {
    palette: {
      primary: {
        light: brand[200],
        main: brand[400],
        dark: brand[700],
        contrastText: brand[50],
      },
      secondary: {
        light: gray[200],
        main: gray[400],
        dark: gray[700],
        contrastText: gray[50],
      },
      info: {
        light: brand[100],
        main: brand[300],
        dark: brand[600],
        contrastText: gray[50],
      },
      warning: {
        light: orange[300],
        main: orange[400],
        dark: orange[800],
      },
      error: {
        light: red[300],
        main: red[400],
        dark: red[800],
      },
      success: {
        light: green[300],
        main: green[400],
        dark: green[800],
      },
      grey: {
        ...gray,
      },
      divider: alpha(gray[300], 0.4),
      background: {
        default: 'hsl(0, 0%, 99%)',
        paper: '#F3F4F6',
      },
      text: {
        primary: gray[800],
        secondary: gray[600],
        warning: orange[400],
      },
      action: {
        hover: alpha(gray[200], 0.2),
        selected: `${alpha(gray[200], 0.3)}`,
      },
      baseShadow:
        'hsla(220, 30%, 5%, 0.07) 0px 4px 16px 0px, hsla(220, 25%, 10%, 0.07) 0px 8px 16px -5px',
    },
  },
  dark: {
    palette: {
      primary: {
        contrastText: brand[50],
        light: brand[300],
        main: brand[400],
        dark: brand[700],
      },
      secondary: {
        contrastText: gray[50],
        light: gray[300],
        main: gray[400],
        dark: gray[700],
      },
      info: {
        contrastText: brand[300],
        light: brand[500],
        main: brand[700],
        dark: brand[900],
      },
      warning: {
        light: orange[400],
        main: orange[500],
        dark: orange[700],
      },
      error: {
        light: red[400],
        main: red[500],
        dark: red[700],
      },
      success: {
        light: green[400],
        main: green[500],
        dark: green[700],
      },
      grey: {
        ...gray,
      },
      divider: alpha(gray[700], 0.6),
      background: {
        default: gray[900],
        paper: 'hsl(220, 30%, 7%)',
      },
      text: {
        primary: 'hsl(0, 0%, 100%)',
        secondary: gray[400],
      },
      action: {
        hover: alpha(gray[600], 0.2),
        selected: alpha(gray[600], 0.3),
      },
      baseShadow:
        'hsla(220, 30%, 5%, 0.7) 0px 4px 16px 0px, hsla(220, 25%, 10%, 0.8) 0px 8px 16px -5px',
    },
  },
};

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
  fontFamily: "'Inter Variable', Inter, -apple-system, BlinkMacSystemFont, sans-serif",
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
export const serifFont = "'Source Serif 4 Variable', 'Source Serif 4', Georgia, serif";
export const monoFont = "'JetBrains Mono Variable', 'JetBrains Mono', monospace";
