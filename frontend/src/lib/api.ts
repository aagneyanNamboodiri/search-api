import type { SearchResponse } from "@/types/search";

export type SearchParams = {
  q: string;
  page?: number;
  size?: number;
};

export class ApiError extends Error {
  status: number;

  constructor(message: string, status: number) {
    super(message);
    this.name = "ApiError";
    this.status = status;
  }
}

export const search = async ({
  q,
  page = 1,
  size = 10,
}: SearchParams): Promise<SearchResponse> => {
  const url = new URL("/api/search", window.location.origin);
  url.searchParams.set("q", q);
  url.searchParams.set("page", String(page));
  url.searchParams.set("size", String(size));

  const response = await fetch(url);

  if (!response.ok) {
    let detail = response.statusText;
    try {
      const body = (await response.json()) as { detail?: unknown };
      if (typeof body.detail === "string") {
        detail = body.detail;
      }
    } catch {
      // ignore JSON parse errors
    }
    throw new ApiError(detail, response.status);
  }

  return response.json() as Promise<SearchResponse>;
};
