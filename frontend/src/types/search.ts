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
