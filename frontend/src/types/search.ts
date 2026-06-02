export type SearchHit = {
  id: string;
  score: number | null;
  source: Record<string, unknown>;
};

export type SearchResponse = {
  total: number;
  page: number;
  size: number;
  took_ms: number;
  hits: SearchHit[];
};

export type StoreSource = {
  id?: number;
  title?: string;
  city?: string;
  state?: string;
  region?: string;
  country?: string;
  [key: string]: unknown;
};

export type BatchHit = {
  session_uuid: string;
  score: number | null;
  matched: string[];
  category_title: string | null;
  store: StoreSource | null;
};

export type Batch = {
  level: string;
  label: string;
  offset: number;
  has_more: boolean;
  hits: BatchHit[];
};

export type SearchStrategy = "multi" | "single";

export type BatchedSearchResponse = {
  took_ms: number;
  strategy: SearchStrategy;
  deduped: boolean;
  batches: Batch[];
};
