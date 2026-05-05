import Button, { type ButtonProps } from '@mui/material/Button';
import { createLink } from '@tanstack/react-router';
import { forwardRef } from 'react';

// Create a router-compatible MUI Button
export const RouterButton = createLink(
  forwardRef<HTMLButtonElement, ButtonProps>((props, ref) => {
    return <Button ref={ref} component='button' {...props} />;
  }),
);
