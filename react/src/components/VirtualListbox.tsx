import Autocomplete, { autocompleteClasses } from '@mui/material/Autocomplete';
import ListSubheader from '@mui/material/ListSubheader';
import Popper from '@mui/material/Popper';
import { styled, useTheme } from '@mui/material/styles';
import TextField from '@mui/material/TextField';
import Typography from '@mui/material/Typography';
import useMediaQuery from '@mui/material/useMediaQuery';
import type { Virtualizer } from '@tanstack/react-virtual';
import { useVirtualizer } from '@tanstack/react-virtual';
import {
  forwardRef,
  useCallback,
  useEffect,
  useMemo,
  useRef,
  type HTMLAttributes,
  type Key,
  type ReactNode,
  type SyntheticEvent,
} from 'react';

const LISTBOX_PADDING = 8;

type OptionProps = HTMLAttributes<HTMLLIElement> & { key: Key };
type ItemData = Array<
  | { key: number; group: string; children: ReactNode }
  | [OptionProps, unknown, number]
>;

const StyledPopper = styled(Popper)({
  [`& .${autocompleteClasses.listbox}`]: {
    boxSizing: 'border-box',
    '& ul': { padding: 0, margin: 0 },
  },
});

interface VirtualListboxProps extends HTMLAttributes<HTMLElement> {
  virtualizerRef?: React.MutableRefObject<Virtualizer<
    HTMLDivElement,
    Element
  > | null>;
  onItemsBuilt?: (map: Map<unknown, number>) => void;
  getItemLabel?: (option: unknown) => ReactNode;
}

export const VirtualListbox = forwardRef<HTMLDivElement, VirtualListboxProps>(
  function ListboxComponent(
    { children, virtualizerRef, onItemsBuilt, getItemLabel, ...other },
    ref,
  ) {
    const theme = useTheme();
    const smUp = useMediaQuery(theme.breakpoints.up('sm'), { noSsr: true });
    const itemSize = smUp ? 36 : 48;

    // Flatten grouped children into a single array (same shape as the react-window example)
    const itemData = useMemo<ItemData>(() => {
      const data: ItemData = [];
      (children as ItemData).forEach((item) => {
        data.push(item);
        if ('children' in item && Array.isArray(item.children)) {
          data.push(...(item.children as ItemData));
        }
      });
      return data;
    }, [children]);

    // Map option string → flat index so onHighlightChange can call scrollToIndex
    const optionIndexMap = useMemo(() => {
      // const map = new Map<string, number>();
      const map = new Map<unknown, number>();
      itemData.forEach((item, i) => {
        if (Array.isArray(item)) map.set(item[1], i);
      });
      return map;
    }, [itemData]);

    useEffect(() => {
      onItemsBuilt?.(optionIndexMap);
    }, [onItemsBuilt, optionIndexMap]);

    const getItemSize = useCallback(
      (index: number) => ('group' in itemData[index] ? 48 : itemSize),
      [itemData, itemSize],
    );

    const scrollContainerRef = useRef<HTMLDivElement>(null);

    const virtualizer = useVirtualizer({
      count: itemData.length,
      getScrollElement: () => scrollContainerRef.current,
      estimateSize: getItemSize,
      overscan: 5,
    });

    // Expose the virtualizer instance so the parent can call scrollToIndex
    useEffect(() => {
      if (virtualizerRef) virtualizerRef.current = virtualizer;
    });

    const listHeight =
      itemData.length > 8
        ? 8 * itemSize + 2 * LISTBOX_PADDING
        : itemData.reduce((sum, _, i) => sum + getItemSize(i), 0) +
          2 * LISTBOX_PADDING;

    const { className, style: _style, ...otherProps } = other;

    return (
      // Outer div: MUI's ref + ARIA/event props (including onMouseDown: preventDefault).
      // Must stay separate from the scroll container — if onMouseDown: preventDefault
      // is on the scroll element itself the browser cancels scrollbar drag interactions.
      <div ref={ref} {...otherProps}>
        <div
          ref={scrollContainerRef}
          style={{ overflow: 'auto', height: listHeight }}
        >
          <ul
            className={className}
            style={{
              height: virtualizer.getTotalSize() + 2 * LISTBOX_PADDING,
              maxHeight: 'none',
              overflow: 'visible',
              width: '100%',
              position: 'relative',
              padding: 0,
              margin: 0,
            }}
          >
            {virtualizer.getVirtualItems().map((virtualRow) => {
              const item = itemData[virtualRow.index];
              const style: React.CSSProperties = {
                position: 'absolute',
                top: 0,
                left: 0,
                width: '100%',
                height: virtualRow.size,
                transform: `translateY(${virtualRow.start + LISTBOX_PADDING}px)`,
                // MUI's option class sets display:flex; text-overflow only works on block
                display: 'block',
                overflow: 'hidden',
                textOverflow: 'ellipsis',
                whiteSpace: 'nowrap',
              };

              if ('group' in item) {
                return (
                  <ListSubheader
                    key={virtualRow.key}
                    component='div'
                    style={style}
                  >
                    {item.group}
                  </ListSubheader>
                );
              }

              const [{ key, ...optionProps }, option, index] = item;

              return (
                <Typography
                  key={key}
                  component='li'
                  {...optionProps}
                  noWrap
                  style={style}
                  sx={{
                    textOverflow: 'ellipsis',
                    overflow: 'hidden',
                    whiteSpace: 'nowrap',
                    width: '100%',
                  }}
                >
                  {getItemLabel ? getItemLabel(option) : String(option)}
                </Typography>
              );
            })}
          </ul>
        </div>
      </div>
    );
  },
);

// ─── Demo ────────────────────────────────────────────────────────────────────

function random(length: number) {
  const chars =
    'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
  let result = '';
  for (let i = 0; i < length; i++) {
    result += chars.charAt(Math.floor(Math.random() * chars.length));
  }
  return result;
}

const OPTIONS = Array.from({ length: 10000 })
  .map(() => random(10 + Math.ceil(Math.random() * 20)))
  .sort((a, b) => a.toUpperCase().localeCompare(b.toUpperCase()));

function Virtualize() {
  const virtualizerRef = useRef<Virtualizer<HTMLDivElement, Element> | null>(
    null,
  );
  const optionIndexMapRef = useRef(new Map<string, number>());

  const handleItemsBuilt = useCallback((map: Map<string, number>) => {
    optionIndexMapRef.current = map;
  }, []);

  const handleHighlightChange = (
    _event: SyntheticEvent,
    option: string | null,
  ) => {
    if (!option || !virtualizerRef.current) return;
    const index = optionIndexMapRef.current.get(option);
    if (index !== undefined) {
      virtualizerRef.current.scrollToIndex(index, { align: 'auto' });
    }
  };

  return (
    <Autocomplete
      sx={{ width: 300 }}
      disableListWrap
      options={OPTIONS}
      groupBy={(option) => option[0].toUpperCase()}
      renderInput={(params) => <TextField {...params} label='10,000 options' />}
      renderOption={(props, option, state) =>
        [props, option, state.index] as ReactNode
      }
      renderGroup={(params) => params as unknown as ReactNode}
      onHighlightChange={handleHighlightChange}
      slots={{ popper: StyledPopper }}
      slotProps={{
        listbox: {
          component: VirtualListbox,
          virtualizerRef,
          onItemsBuilt: handleItemsBuilt,
        } as unknown as React.HTMLAttributes<HTMLElement>,
      }}
    />
  );
}
