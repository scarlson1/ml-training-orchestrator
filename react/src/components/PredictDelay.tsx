import {
  Box,
  Button,
  CircularProgress,
  MenuItem,
  Stack,
  TextField,
  Typography,
} from '@mui/material';
import { useTheme } from '@mui/material/styles';
import { useForm, useStore } from '@tanstack/react-form-start';
import { useMutation, useQuery } from '@tanstack/react-query';
import z from 'zod';
import { apiFetch } from '~/api/apiFetch';
import { VirtualAutocomplete } from '~/components/VirtualAutocomplete';
import { monoFont } from '~/config/themePrimitives';
import { getFlightInfo, getScheduledFlights } from '~/utils/flights.functions';
import type { FlightInfo } from '~/utils/flights.server';
import {
  AIRPORTS,
  CARRIERS,
  getCarrierLogo,
  getFlightCompositeId,
} from '~/utils/misc';

// ─── API types ────────────────────────────────────────────────────────────────

export interface PredictBody {
  flight_id: string; // composite: {carrier}{flight_number}_{date}_{dep_time}
  origin: string;
  dest: string;
  carrier: string;
  route_key: string;
  tail_number?: string;
}

export interface PredictResponse {
  flight_id: string;
  predicted_is_delayed: boolean;
  delay_probability: number;
  model_name: string;
  model_version: string;
  features_complete: boolean;
}

// ─── Schema (identical to PredictDelay) ───────────────────────────────────────

const predictSchema = z
  .object({
    origin: z.string().length(3),
    dest: z.string().length(3),
    carrier: z.string().length(2),
    flight: z.string(),
  })
  .refine(
    (data) => {
      if (!data.flight) return true;
      return data.flight.split('_')[0] === data.carrier;
    },
    { error: 'Carrier must match flight', path: ['carrier'] },
  );

type PredictSchema = z.infer<typeof predictSchema>;

const defaultPredict: PredictSchema = {
  origin: '',
  dest: '',
  carrier: '',
  flight: '',
};

// ─── Flat input style overrides ────────────────────────────────────────────────
// Applied to a wrapping Box so both VirtualAutocomplete and TextField select
// render without MUI borders/labels — the cell provides its own label.

const flatField = {
  width: '100%',
  '& .MuiInputLabel-root': { display: 'none' },
  '& .MuiOutlinedInput-notchedOutline': { border: 'none' },
  '& legend': { width: '0 !important' },
  '& .MuiOutlinedInput-root': {
    fontFamily: monoFont,
    fontSize: 14,
    fontWeight: 500,
    p: '0 !important',
  },
  '& .MuiInputBase-input, & .MuiSelect-select': {
    p: '0 !important',
    height: 'auto !important',
    minHeight: 'unset !important',
  },
  '& .MuiAutocomplete-popupIndicator, & .MuiAutocomplete-clearIndicator': {
    display: 'none',
  },
  '& .MuiSelect-icon': { display: 'none' },
} as const;

// ─── Cell ─────────────────────────────────────────────────────────────────────

const Cell = ({
  label,
  children,
  dividerColor,
}: {
  label: string;
  children: React.ReactNode;
  dividerColor?: string;
}) => (
  <Box
    sx={{
      flex: 1,
      px: 1.5,
      pt: 1,
      pb: 0.5,
      minWidth: 0,
      overflow: 'hidden',
      ...(dividerColor && {
        borderRight: `1px solid ${dividerColor}`,
      }),
    }}
  >
    <Typography
      sx={{
        fontSize: 10,
        color: 'text.disabled',
        mb: '2px',
        fontFamily: 'Inter, sans-serif',
      }}
    >
      {label}
    </Typography>
    <Box sx={flatField}>{children}</Box>
  </Box>
);

// ─── PredictDelay ───────────────────────────────────────────────────────

interface PredictDelayProps {
  onPredict: (data: PredictResponse) => void;
}

export const PredictDelay = ({ onPredict }: PredictDelayProps) => {
  const p = useTheme().vars.palette;
  const { mutate: predict, isPending } = useMutation({
    mutationFn: async (body: PredictBody) => {
      const data = await apiFetch<PredictResponse>('/predict', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      });
      return data;
    },
    onSuccess: (data) => {
      onPredict(data);
    },
  });

  const { mutateAsync: fetchFlightInfo } = useMutation({
    mutationFn: async (flight_iata: string) =>
      getFlightInfo({ data: { flight_iata } }),
  });

  const form = useForm({
    defaultValues: defaultPredict,
    validators: { onChange: predictSchema },
    onSubmit: async ({ value }) => {
      const [_, flight_num] = value.flight?.split('-');
      const flightInfo = flight_num
        ? await fetchFlightInfo(`${value.carrier}${flight_num}`)
        : ({} as FlightInfo);

      const body: PredictBody = {
        flight_id: getFlightCompositeId(value.origin, value.dest),
        origin: value.origin,
        dest: value.dest,
        carrier: value.carrier,
        route_key: `${value.origin}-${value.dest}`,
        tail_number: flightInfo?.reg_number ?? '',
      };
      predict(body);
    },
  });

  const origin = useStore(form.store, (state) => state.values.origin);
  const dest = useStore(form.store, (state) => state.values.dest);

  const { data: flights, isFetching } = useQuery({
    queryKey: ['flights', origin, dest],
    queryFn: () => getScheduledFlights({ data: { origin, dest } }),
    enabled: Boolean(origin && dest),
    staleTime: 1000 * 60 * 60 * 12,
    gcTime: 1000 * 60 * 60 * 12,
  });

  return (
    <form
      onSubmit={(e) => {
        e.preventDefault();
        e.stopPropagation();
        form.handleSubmit();
      }}
    >
      <Box
        sx={{
          display: 'flex',
          alignItems: 'stretch',
          border: '1px solid',
          borderColor: 'divider',
          bgcolor: 'background.paper',
          overflow: 'hidden',
        }}
      >
        {/* Origin */}
        <form.Field
          name='origin'
          listeners={{
            onChange: ({ fieldApi }) => {
              fieldApi.form.setFieldValue('flight', '');
            },
          }}
        >
          {({ state, handleChange, handleBlur }) => (
            <Cell label='Origin' dividerColor={p.custom.lineSoft}>
              <VirtualAutocomplete
                options={AIRPORTS}
                value={AIRPORTS.find((a) => a.code === state.value) ?? null}
                onChange={(airport) => handleChange(airport?.code ?? '')}
                onBlur={handleBlur}
                label='Origin'
                required
                autoComplete
                autoHighlight
                popperMinWidth={260}
                getOptionLabel={(o) => `${o.code} – ${o.name}`}
                isOptionEqualToValue={(o, v) => o.code === v.code}
                filterOptions={(options, { inputValue }) => {
                  const q = inputValue.toLowerCase();
                  return options.filter(
                    (o) =>
                      o.code.toLowerCase().includes(q) ||
                      o.name.toLowerCase().includes(q),
                  );
                }}
              />
            </Cell>
          )}
        </form.Field>

        {/* Destination */}
        <form.Field
          name='dest'
          listeners={{
            onChange: ({ fieldApi }) => {
              fieldApi.form.setFieldValue('flight', '');
            },
          }}
        >
          {({ state, handleChange, handleBlur }) => (
            <Cell label='Destination' dividerColor={p.custom.lineSoft}>
              <VirtualAutocomplete
                options={AIRPORTS}
                value={AIRPORTS.find((a) => a.code === state.value) ?? null}
                onChange={(airport) => handleChange(airport?.code ?? '')}
                onBlur={handleBlur}
                label='Destination'
                required
                autoComplete
                autoHighlight
                popperMinWidth={260}
                getOptionLabel={(o) => `${o.code} – ${o.name}`}
                isOptionEqualToValue={(o, v) => o.code === v.code}
                filterOptions={(options, { inputValue }) => {
                  const q = inputValue.toLowerCase();
                  return options.filter(
                    (o) =>
                      o.code.toLowerCase().includes(q) ||
                      o.name.toLowerCase().includes(q),
                  );
                }}
              />
            </Cell>
          )}
        </form.Field>

        {/* Carrier */}
        <form.Field
          name='carrier'
          validators={{
            onChangeListenTo: ['flight'],
            onChange: ({ value, fieldApi }) => {
              if (!fieldApi.getMeta().isDirty || !value) return;
              const flight = form.getFieldValue('flight');
              const flightDetails = flights?.find(
                (f) => f.flight_iata === flight,
              );
              if (!flight || !flightDetails) return;
              if (flightDetails.airline_iata !== value)
                return 'Carrier does not match flight';
            },
          }}
        >
          {({ state, handleChange, handleBlur }) => (
            <Cell label='Carrier' dividerColor={p.custom.lineSoft}>
              <TextField
                id='carrier'
                select
                value={state.value}
                onChange={(e) => handleChange(e.target.value)}
                onBlur={handleBlur}
                fullWidth
                required
                slotProps={{
                  select: {
                    MenuProps: { sx: { maxHeight: 360 } },
                    renderValue: (value) => (
                      <Stack
                        direction='row'
                        spacing={1}
                        sx={{ alignItems: 'center' }}
                      >
                        <Box>
                          {getCarrierLogo((value as string) ?? '', {
                            sx: { fontSize: 'inherit' },
                          })}
                        </Box>
                        <Typography
                          sx={{
                            fontFamily: monoFont,
                            fontSize: 14,
                            fontWeight: 500,
                          }}
                        >
                          {value ? `${value}` : '--'}
                        </Typography>
                      </Stack>
                    ),
                  },
                }}
              >
                <MenuItem value=''>{'--'}</MenuItem>
                {CARRIERS.map((option) => (
                  <MenuItem
                    key={option.code}
                    value={option.code}
                    sx={{
                      display: 'flex',
                      alignItems: 'center',
                      gap: 2,
                      flexWrap: 'nowrap',
                    }}
                  >
                    <Box
                      sx={{
                        display: 'flex',
                        alignItems: 'center',
                        flexShrink: 0,
                      }}
                    >
                      {getCarrierLogo(option.code, {
                        sx: { fontSize: 'inherit' },
                      })}
                    </Box>
                    <Typography
                      sx={{
                        whiteSpace: 'nowrap',
                        overflow: 'hidden',
                        textOverflow: 'ellipsis',
                        fontFamily: monoFont,
                      }}
                    >
                      {option.name}
                    </Typography>
                  </MenuItem>
                ))}
              </TextField>
            </Cell>
          )}
        </form.Field>

        {/* Flight # */}
        <form.Field
          name='flight'
          listeners={{
            onChange: ({ value }) => {
              if (!value) return;
              const newCarrierVal = (value as string).split('_')[0] ?? '';
              form.setFieldValue('carrier', newCarrierVal);
            },
          }}
        >
          {({ state, handleChange, handleBlur }) => (
            <form.Subscribe
              selector={(s) => [
                s.values.origin,
                s.values.dest,
                s.values.carrier,
              ]}
            >
              {([origin, dest, carrier]) => {
                const flightOptions = carrier
                  ? flights?.filter((f) => f.airline_iata === carrier)
                  : flights;

                return (
                  <Cell label='Flight #'>
                    <TextField
                      value={state.value}
                      onChange={(e) => handleChange(e.target.value)}
                      disabled={!(origin && dest)}
                      select
                      onBlur={handleBlur}
                      fullWidth
                      slotProps={{
                        input: {
                          endAdornment: isFetching ? (
                            <CircularProgress color='inherit' size={16} />
                          ) : null,
                        },
                        select: {
                          IconComponent: isFetching ? () => null : undefined,
                          renderValue: (value) => (
                            <Typography
                              sx={{
                                fontFamily: monoFont,
                                fontSize: 14,
                                fontWeight: 500,
                              }}
                            >
                              {value
                                ? `${(value as string).split('_')[1]}`
                                : '--'}
                            </Typography>
                          ),
                        },
                      }}
                    >
                      <MenuItem value=''>{'--'}</MenuItem>
                      {flightOptions?.map((option) => (
                        <MenuItem
                          key={option?.flight_number}
                          value={`${option.airline_iata}_${option.flight_number}`}
                          sx={{
                            display: 'flex',
                            alignItems: 'center',
                            gap: 2,
                            flexWrap: 'nowrap',
                          }}
                        >
                          <Box
                            sx={{
                              display: 'flex',
                              alignItems: 'center',
                              flexShrink: 0,
                            }}
                          >
                            {getCarrierLogo(option.airline_iata)}
                          </Box>
                          <Box
                            sx={{
                              display: 'flex',
                              flexDirection: 'column',
                              minWidth: 0,
                              overflow: 'hidden',
                            }}
                          >
                            <Typography
                              variant='body2'
                              noWrap
                              sx={{ lineHeight: 1.2, fontFamily: monoFont }}
                            >
                              {option.flight_iata}
                            </Typography>
                            <Typography
                              variant='caption'
                              color='text.secondary'
                              noWrap
                              sx={{
                                lineHeight: 1.2,
                                fontFamily: monoFont,
                                fontSize: '0.625rem',
                              }}
                            >
                              {option.dep_time}
                            </Typography>
                          </Box>
                        </MenuItem>
                      ))}
                    </TextField>
                  </Cell>
                );
              }}
            </form.Subscribe>
          )}
        </form.Field>

        {/* Submit */}
        <form.Subscribe
          selector={(s) => [s.canSubmit, s.isSubmitting, s.isValidating]}
        >
          {([canSubmit, isSubmitting, isValidating]) => (
            <Button
              type='submit'
              disabled={!canSubmit}
              loading={isSubmitting || isValidating || isPending}
              loadingPosition='end'
              endIcon='→'
              variant='contained'
              disableElevation
              sx={{
                bgcolor: 'text.primary',
                color: 'background.default',
                borderRadius: 0,
                fontSize: 13,
                px: '22px',
                fontWeight: 500,
                textTransform: 'none',
                flexShrink: 0,
                alignSelf: 'stretch',
                '&:hover': { bgcolor: 'text.secondary' },
                '&.Mui-disabled': {
                  bgcolor: 'text.disabled',
                  color: 'background.default',
                },
              }}
            >
              Predict
            </Button>
          )}
        </form.Subscribe>
      </Box>
    </form>
  );
};
