const BASE_URL = import.meta.env.VITE_API_URL ?? '';

export function apiUrl(path: string): string {
  return `${BASE_URL}${path}`;
}

export function apiFetch(path: string, init?: RequestInit): Promise<Response> {
  return fetch(apiUrl(path), init);
}
