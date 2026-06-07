import { resolveSearchField } from "@/lib/search-query/fields";
import type { ParsedSearchQuery } from "@/lib/search-query/types";
import type {
  Batch,
  BatchedSearchResponse,
} from "@/types/search";

export type SortOption = "relevance" | "recent";

export type StoreClausePayload = { field: string; value: string };

export type SearchRequestPayload = {
  store?: StoreClausePayload;
  level?: string;
  value?: string;
  term?: string;
  sort: SortOption;
  dedup: boolean;
  size?: number;
};

export type MorePayload = {
  store?: StoreClausePayload;
  level: string;
  query?: string;
  sort: SortOption;
  offset: number;
  size?: number;
};

export type SearchOptions = {
  sort: SortOption;
  dedup: boolean;
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

/**
 * Translate a parsed query into the backend request contract:
 * - store clause -> hard filter (first store clause wins)
 * - entity clause -> a single level + value (case 1/4)
 * - otherwise the residual free-text term (case 2/3)
 */
export const buildSearchRequest = (
  parsed: ParsedSearchQuery,
  options: SearchOptions,
): SearchRequestPayload => {
  let store: StoreClausePayload | undefined;
  let level: string | undefined;
  let value: string | undefined;

  for (const clause of parsed.clauses) {
    const group = clause.group ?? resolveSearchField(clause.field)?.group;
    if (group === "store") {
      store ??= { field: clause.field, value: clause.value };
    } else if (group === "entity") {
      if (!level) {
        level = clause.field.split(".")[0];
        value = clause.value;
      }
    }
  }

  const payload: SearchRequestPayload = {
    sort: options.sort,
    dedup: options.dedup,
  };
  if (options.size) payload.size = options.size;
  if (store) payload.store = store;
  if (level && value) {
    payload.level = level;
    payload.value = value;
  } else if (parsed.term) {
    payload.term = parsed.term;
  }

  return payload;
};

/** The query text a given batch was searched with, for "show more" requests. */
export const queryForBatch = (payload: SearchRequestPayload): string | undefined =>
  payload.term ?? payload.value;

const withIndex = (path: string, index: string): string =>
  `${path}?index=${encodeURIComponent(index)}`;

const postJson = async <T>(path: string, body: unknown): Promise<T> => {
  const response = await fetch(path, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });

  if (!response.ok) {
    let detail = response.statusText;
    try {
      const parsed = (await response.json()) as { detail?: unknown };
      if (typeof parsed.detail === "string") {
        detail = parsed.detail;
      }
    } catch {
      // ignore JSON parse errors
    }
    throw new ApiError(detail, response.status);
  }

  return response.json() as Promise<T>;
};

export const runSearch = (
  index: string,
  payload: SearchRequestPayload,
): Promise<BatchedSearchResponse> =>
  postJson<BatchedSearchResponse>(
    withIndex("/api/v1/multi-search", index),
    payload,
  );

export const fetchMore = (
  index: string,
  payload: MorePayload,
): Promise<Batch> =>
  postJson<Batch>(withIndex("/api/v1/search/more", index), payload);
