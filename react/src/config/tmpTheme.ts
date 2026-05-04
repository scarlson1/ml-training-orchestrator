// ─── Design tokens ────────────────────────────────────────────────────────────

export interface Tokens {
  bg: string;
  panel: string;
  panelAlt: string;
  line: string;
  lineSoft: string;
  ink: string;
  inkSoft: string;
  inkMuted: string;
  accent: string;
  good: string;
  warn: string;
  bad: string;
  chipBg: string;
}
// TODO: integrate into theme
export const TOKENS: { light: Tokens; dark: Tokens } = {
  light: {
    bg: '#FBFAF7',
    panel: '#FFFFFF',
    panelAlt: '#F4F2EC',
    line: '#E7E3D9',
    lineSoft: '#EFEBE0',
    ink: '#1A1A18',
    inkSoft: '#5C5A52',
    inkMuted: '#8F8C82',
    accent: '#2B6BFF',
    good: '#1F7A3F',
    warn: '#B5701B',
    bad: '#B23B2A',
    chipBg: '#F1EEE5',
  },
  dark: {
    bg: '#0F0F0E',
    panel: '#151514',
    panelAlt: '#1A1A18',
    line: '#26251F',
    lineSoft: '#1F1E18',
    ink: '#F4F1E8',
    inkSoft: '#A09D90',
    inkMuted: '#6D6A60',
    accent: '#7DA8FF',
    good: '#67C28E',
    warn: '#E8B065',
    bad: '#E88370',
    chipBg: '#1C1B15',
  },
};
