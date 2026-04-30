import { createTheme } from '@mui/material/styles';
import { colorSchemes, shape } from '~/config/themePrimitives';

export const theme = createTheme({
  colorSchemes: colorSchemes,
  cssVariables: { colorSchemeSelector: 'data', cssVarPrefix: '' },
  shape: shape,
  typography: {
    fontFamily: "'Roboto Variable', sans-serif",
  },
});
