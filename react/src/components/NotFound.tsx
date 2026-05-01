import { Box, Button, Typography } from '@mui/material';
import { Link } from '@tanstack/react-router';

export function NotFound() {
  return (
    <Box
      sx={{
        display: 'flex',
        flexDirection: 'column',
        alignItems: 'center',
        justifyContent: 'center',
        height: '100%',
        gap: 2,
        textAlign: 'center',
      }}
    >
      <Typography variant='h1' sx={{ fontSize: '6rem', fontWeight: 700, lineHeight: 1 }}>
        404
      </Typography>
      <Typography variant='h5' color='text.secondary'>
        Page not found
      </Typography>
      <Button component={Link} to='/' variant='contained' sx={{ mt: 1 }}>
        Go home
      </Button>
    </Box>
  );
}
