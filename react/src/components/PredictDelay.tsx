import {
  Box,
  Button,
  CircularProgress,
  Grid,
  MenuItem,
  Stack,
  TextField,
  Typography,
} from '@mui/material';
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

// ─── Form types ───────────────────────────────────────────────────────────────

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
    {
      error: 'Carrier must match flight',
      path: ['carrier'],
    },
  );
type PredictSchema = z.infer<typeof predictSchema>;

const defaultPredict: PredictSchema = {
  origin: '',
  dest: '',
  carrier: '',
  flight: '',
};

// export const formOpts = formOptions({
//   defaultValues: defaultPredict
// })

// ─── PredictDelay Component ───────────────────────────────────────────────────

interface PredictDelayProps {
  onPredict: (data: PredictResponse) => void;
}

export const PredictDelay = ({ onPredict }: PredictDelayProps) => {
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
    validators: {
      onChange: predictSchema,
    },
    onSubmit: async ({ value }) => {
      console.log(value);
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

  if (form.state.errors.length) console.log(form.state.errors);

  const origin = useStore(form.store, (state) => state.values.origin);
  const dest = useStore(form.store, (state) => state.values.dest);

  // flight options: useQuery to fetch options depending on origin & dest
  const { data: flights, isFetching } = useQuery({
    queryKey: ['flights', origin, dest],
    queryFn: () => getScheduledFlights({ data: { origin, dest } }),
    // queryFn: () => getFlights({ data: { origin, dest } }),
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
      <Grid container spacing={{ xs: 1, sm: 1.5, md: 2 }}>
        <Grid size={{ xs: 4, sm: 'grow' }}>
          <form.Field
            name='origin'
            listeners={{
              onChange: ({ fieldApi }) => {
                fieldApi.form.setFieldValue('flight', '');
              },
            }}
            children={({ state, handleChange, handleBlur }) => (
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
            )}
          />
        </Grid>

        <Grid size={{ xs: 4, sm: 'grow' }}>
          <form.Field
            name='dest'
            listeners={{
              onChange: ({ fieldApi }) => {
                fieldApi.form.setFieldValue('flight', '');
              },
            }}
            children={({ state, handleChange, handleBlur }) => (
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
            )}
            // children={({ state, handleChange, handleBlur }) => (
            //   <Autocomplete<Airport>
            //     options={AIRPORTS}
            //     value={AIRPORTS.find((a) => a.code === state.value) ?? null}
            //     onChange={(_, airport) => handleChange(airport?.code ?? '')}
            //     onBlur={handleBlur}
            //     getOptionLabel={(option) => `${option.code} – ${option.name}`}
            //     filterOptions={(options, { inputValue }) => {
            //       const q = inputValue.toLowerCase();
            //       return options.filter(
            //         (o) =>
            //           o.code.toLowerCase().includes(q) ||
            //           o.name.toLowerCase().includes(q),
            //       );
            //     }}
            //     isOptionEqualToValue={(option, value) =>
            //       option.code === value.code
            //     }
            //     renderInput={(params) => (
            //       <TextField {...params} label='Destination' required />
            //     )}
            //   />
            // )}
          />
        </Grid>

        <Grid size={{ xs: 4, sm: 'grow' }}>
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
            children={({ state, handleChange, handleBlur }) => {
              return (
                <TextField
                  id='carrier'
                  label='Carrier'
                  select
                  value={state.value}
                  onChange={(e) => handleChange(e.target.value)}
                  onBlur={handleBlur}
                  placeholder='DL'
                  fullWidth
                  required
                  slotProps={{
                    select: {
                      MenuProps: {
                        sx: { maxHeight: 360 },
                      },
                      renderValue: (value) => {
                        return (
                          <Stack direction='row' spacing={1}>
                            <Box>
                              {getCarrierLogo((value as string) ?? '', {
                                sx: { fontSize: 'inherit' },
                              })}
                            </Box>
                            <Typography>{value ? `${value}` : '--'}</Typography>
                          </Stack>
                        );
                      },
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
              );
            }}
          />
        </Grid>

        <Grid size={{ xs: 4, sm: 'grow' }}>
          <form.Field
            name='flight'
            listeners={{
              onChange: ({ value }) => {
                if (!value) return;
                // const f = flights?.find((fl) => fl.flight_number === value);
                // console.log(
                //   `flight: ${value}`,
                //   `setting carrier: ${f?.airline_iata}`,
                // );
                let newCarrierVal = (value as string).split('_')[0] ?? '';
                form.setFieldValue('carrier', newCarrierVal);
              },
            }}
          >
            {({ state, handleChange, handleBlur }) => (
              <form.Subscribe
                selector={(state) => [
                  state.values.origin,
                  state.values.dest,
                  state.values.carrier,
                ]}
                children={([origin, dest, carrier]) => {
                  const flightOptions = carrier
                    ? flights?.filter((f) => f.airline_iata == carrier)
                    : flights;

                  return (
                    <TextField
                      value={state.value}
                      onChange={(e) => handleChange(e.target.value)}
                      disabled={!(origin && dest)}
                      label='Flight #'
                      select
                      onBlur={handleBlur}
                      placeholder='BNA'
                      fullWidth
                      slotProps={{
                        input: {
                          endAdornment: (
                            <>
                              {isFetching ? (
                                <CircularProgress color='inherit' size={16} />
                              ) : null}
                            </>
                          ),
                        },
                        select: {
                          // Removes the arrow while loading to prevent overlap
                          IconComponent: isFetching ? () => null : undefined,
                          renderValue: (value) => (
                            <Typography>
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
                              sx={{
                                lineHeight: 1.2,
                                fontFamily: monoFont,
                              }}
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
                  );
                }}
              />
            )}
          </form.Field>
        </Grid>

        <Grid size={{ xs: 12, sm: 'auto', md: 'auto' }}>
          <form.Subscribe
            selector={(state) => [
              state.canSubmit,
              state.isSubmitting,
              state.isValidating,
            ]}
            children={([canSubmit, isSubmitting, isValidating]) => {
              console.log(
                'canSubmit / isSubmitting, isValidating',
                canSubmit,
                isSubmitting,
                isValidating,
              );

              return (
                <Button
                  type='submit'
                  disabled={!canSubmit}
                  loading={isSubmitting || isValidating}
                  loadingPosition='end'
                  endIcon={'→'}
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
                    '&:hover': { bgcolor: 'text.secondary' },
                    '&.Mui-disabled': {
                      bgcolor: 'text.disabled',
                      color: 'background.default',
                    },
                    height: {
                      xs: 'auto',
                      sm: '100%',
                    },
                  }}
                >
                  Predict
                </Button>
              );
            }}
            // children={([canSubmit, isSubmitting, isValidating]) => (
            //   <Button
            //     type='submit'
            //     disabled={!canSubmit}
            //     loading={isSubmitting || isValidating}
            //     loadingPosition='end'
            //     endIcon={'→'}
            //     variant='contained'
            //     disableElevation
            //     sx={{
            //       bgcolor: 'text.primary',
            //       color: 'background.default',
            //       borderRadius: 0,
            //       fontSize: 13,
            //       px: '22px',
            //       fontWeight: 500,
            //       textTransform: 'none',
            //       '&:hover': { bgcolor: 'text.secondary' },
            //       '&.Mui-disabled': {
            //         bgcolor: 'text.disabled',
            //         color: 'background.default',
            //       },
            //       height: {
            //         xs: 'auto',
            //         sm: '100%',
            //       },
            //     }}
            //   >
            //     Predict
            //   </Button>
            // )}
          />
        </Grid>
      </Grid>
    </form>
  );
};
