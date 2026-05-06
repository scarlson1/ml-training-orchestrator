const BASE_URL = import.meta.env.VITE_API_URL ?? '';

export function apiUrl(path: string): string {
  return `${BASE_URL}${path}`;
}

export async function apiFetch<T>(
  path: string,
  options?: RequestInit,
  params?: string[][] | Record<string, string> | string | URLSearchParams,
): Promise<T> {
  const url = new URL(apiUrl(path));
  if (params) url.search = new URLSearchParams(params).toString();
  const res = await fetch(url, options);
  if (!res.ok) throw new Error(`Response status: ${res.status}`);

  return (await res.json()) as T;
  // return fetch(apiUrl(path), init);
}
