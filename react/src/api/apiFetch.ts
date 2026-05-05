const BASE_URL = import.meta.env.VITE_API_URL ?? '';

export function apiUrl(path: string): string {
  return `${BASE_URL}${path}`;
}

export async function apiFetch<T>(
  path: string,
  options?: RequestInit,
): Promise<T> {
  const res = await fetch(apiUrl(path), options);
  if (!res.ok) throw new Error(`Response status: ${res.status}`);

  return (await res.json()) as T;
  // return fetch(apiUrl(path), init);
}
