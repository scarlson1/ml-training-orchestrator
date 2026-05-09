import type { FilterOptionsState } from '@mui/material';
import Autocomplete, { autocompleteClasses } from '@mui/material/Autocomplete';
import Popper from '@mui/material/Popper';
import { styled } from '@mui/material/styles';
import TextField from '@mui/material/TextField';
import type { Virtualizer } from '@tanstack/react-virtual';
import {
  useCallback,
  useRef,
  type ReactNode,
  type SyntheticEvent,
} from 'react';
import { VirtualListbox } from '~/components/VirtualListbox';

const ListboxPopper = styled(Popper)({
  [`& .${autocompleteClasses.listbox}`]: {
    boxSizing: 'border-box',
    '& ul': { padding: 0, margin: 0 },
  },
});

interface VirtualAutocompleteProps<T> {
  options: T[];
  value: T | null;
  onChange: (value: T | null) => void;
  onBlur?: () => void;
  label: string;
  required?: boolean;
  getOptionLabel: (option: T) => string;
  isOptionEqualToValue?: (option: T, value: T) => boolean;
  filterOptions?: (options: T[], state: FilterOptionsState<T>) => T[];
  groupBy?: (option: T) => string;
  popperMinWidth?: number | string;
  autoComplete?: boolean;
  autoHighlight?: boolean;
}

export const VirtualAutocomplete = <T,>({
  options,
  value,
  onChange,
  onBlur,
  label,
  required,
  getOptionLabel,
  isOptionEqualToValue,
  filterOptions,
  groupBy,
  popperMinWidth,
  autoComplete,
  autoHighlight,
}: VirtualAutocompleteProps<T>) => {
  const virtualizerRef = useRef<Virtualizer<HTMLDivElement, Element> | null>(
    null,
  );
  const optionIndexMapRef = useRef(new Map<unknown, number>());

  const handleItemsBuilt = useCallback((map: Map<unknown, number>) => {
    optionIndexMapRef.current = map;
  }, []);

  const handleHighlightChange = (_event: SyntheticEvent, option: T | null) => {
    if (!option || !virtualizerRef.current) return;
    const index = optionIndexMapRef.current.get(option);
    if (index !== undefined) {
      virtualizerRef.current.scrollToIndex(index, {
        align: 'auto',
      });
    }
  };

  return (
    <Autocomplete<T>
      autoComplete={autoComplete}
      autoHighlight={autoHighlight}
      disableListWrap
      options={options}
      value={value}
      onChange={(_, newValue) => onChange(newValue)}
      onBlur={onBlur}
      onHighlightChange={handleHighlightChange}
      getOptionLabel={getOptionLabel}
      isOptionEqualToValue={isOptionEqualToValue}
      filterOptions={filterOptions}
      groupBy={groupBy}
      renderInput={(params) => (
        <TextField {...params} label={label} required={required} />
      )}
      renderOption={(props, option, state) =>
        [props, option, state.index] as ReactNode
      }
      renderGroup={(params) => params as unknown as ReactNode}
      slots={{ popper: ListboxPopper }}
      slotProps={{
        listbox: {
          component: VirtualListbox,
          virtualizerRef,
          onItemsBuilt: handleItemsBuilt,
          getItemLabel: (option: unknown) => getOptionLabel(option as T),
        } as unknown as React.HTMLAttributes<HTMLElement>,
        popper: {
          sx: { minWidth: popperMinWidth },
        },
      }}
    />
  );
};
