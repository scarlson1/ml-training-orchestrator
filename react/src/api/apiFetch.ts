const BASE_URL = import.meta.env.VITE_API_URL ?? '';

export function apiUrl(path: string): string {
  return `${BASE_URL}${path}`;
}

export async function apiFetch(
  path: string,
  init?: RequestInit,
): Promise<Response> {
  // const response = await fetch(apiUrl(path), init);

  // if (!response.ok) {
  //   const body = await response.json().catch(() => null);
  //   const detail = body?.detail ?? response.statusText;
  //   throw new Error(`${response.status} ${detail}`);
  // }

  // return response;
  return fetch(apiUrl(path), init);
}
