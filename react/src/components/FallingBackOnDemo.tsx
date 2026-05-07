import { WarningAmberRounded } from '@mui/icons-material';
import { Stack, Typography } from '@mui/material';

export function FallingBackOnDemo({ show }: { show: boolean }) {
  if (!show) return null; // || !import.meta.env.DEV
  return (
    <Stack
      spacing={1}
      direction='row'
      sx={{ display: 'flex', justifyContent: 'center', mt: 2 }}
    >
      <WarningAmberRounded fontSize='small' color='warning' />
      <Typography color='warning' variant='body2'>
        Falling back on demo data
      </Typography>
    </Stack>
  );
}
