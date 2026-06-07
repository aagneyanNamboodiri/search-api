import { useState } from "react";

import { BatchedResults } from "@/components/BatchedResults";
import { SearchBar } from "@/components/SearchBar";
import { Button } from "@/components/ui/button";
import {
  ApiError,
  buildSearchRequest,
  fetchMore,
  queryForBatch,
  runSearch,
  type SearchRequestPayload,
  type SortOption,
} from "@/lib/api";
import { parseSearchQuery } from "@/lib/search-query/parser";
import type { BatchedSearchResponse } from "@/types/search";

const INDICES = ["scm_constellation_brands_poc", "scm_demo_infilect_2025"];

type Toggle<T extends string> = { value: T; label: string };

const SORTS: Toggle<SortOption>[] = [
  { value: "relevance", label: "Relevance" },
  { value: "recent", label: "Recent" },
];

const ToggleGroup = <T extends string>({
  options,
  value,
  onChange,
}: {
  options: Toggle<T>[];
  value: T;
  onChange: (value: T) => void;
}) => (
  <div className="flex gap-1">
    {options.map((option) => (
      <Button
        key={option.value}
        onClick={() => onChange(option.value)}
        size="sm"
        type="button"
        variant={option.value === value ? "default" : "outline"}
      >
        {option.label}
      </Button>
    ))}
  </div>
);

const App = () => {
  const [query, setQuery] = useState("");
  const [index, setIndex] = useState(INDICES[0]);
  const [sort, setSort] = useState<SortOption>("relevance");
  const [dedup, setDedup] = useState(false);

  const [response, setResponse] = useState<BatchedSearchResponse | null>(null);
  const [activePayload, setActivePayload] = useState<SearchRequestPayload | null>(
    null,
  );
  const [activeIndex, setActiveIndex] = useState(index);
  const [error, setError] = useState<string | null>(null);
  const [isLoading, setIsLoading] = useState(false);
  const [loadingMore, setLoadingMore] = useState<Record<string, boolean>>({});

  const handleSearch = async (raw: string) => {
    const parsed = parseSearchQuery(raw);
    const payload = buildSearchRequest(parsed, { sort, dedup });

    if (!payload.store && !payload.level && !payload.term) {
      setError("Enter a search query.");
      setResponse(null);
      return;
    }

    setIsLoading(true);
    setError(null);

    try {
      const data = await runSearch(index, payload);
      setResponse(data);
      setActivePayload(payload);
      setActiveIndex(index);
      setLoadingMore({});
    } catch (err) {
      setResponse(null);
      setError(
        err instanceof ApiError || err instanceof Error
          ? err.message
          : "Something went wrong. Is the API running?",
      );
    } finally {
      setIsLoading(false);
    }
  };

  const handleShowMore = async (level: string) => {
    if (!response || !activePayload) return;
    const batch = response.batches.find((item) => item.level === level);
    if (!batch) return;

    setLoadingMore((prev) => ({ ...prev, [level]: true }));

    try {
      const next = await fetchMore(activeIndex, {
        store: activePayload.store,
        level,
        query: queryForBatch(activePayload),
        sort,
        offset: batch.hits.length,
      });

      setResponse((prev) =>
        prev
          ? {
              ...prev,
              batches: prev.batches.map((item) =>
                item.level === level
                  ? {
                      ...item,
                      hits: [...item.hits, ...next.hits],
                      has_more: next.has_more,
                    }
                  : item,
              ),
            }
          : prev,
      );
    } catch (err) {
      setError(
        err instanceof ApiError || err instanceof Error
          ? err.message
          : "Failed to load more results.",
      );
    } finally {
      setLoadingMore((prev) => ({ ...prev, [level]: false }));
    }
  };

  return (
    <div className="mx-auto flex min-h-svh w-full max-w-2xl flex-col gap-6 px-4 py-12">
      <SearchBar
        isLoading={isLoading}
        onChange={setQuery}
        onSubmit={(value) => void handleSearch(value)}
        value={query}
      />

      <div className="flex flex-wrap items-center gap-x-4 gap-y-2 text-sm">
        <label className="flex items-center gap-2">
          <span className="text-muted-foreground">Index</span>
          <select
            className="rounded-md border border-input bg-background px-2 py-1 text-sm"
            onChange={(event) => setIndex(event.target.value)}
            value={index}
          >
            {INDICES.map((name) => (
              <option key={name} value={name}>
                {name}
              </option>
            ))}
          </select>
        </label>

        <ToggleGroup options={SORTS} value={sort} onChange={setSort} />

        <Button
          onClick={() => setDedup((prev) => !prev)}
          size="sm"
          type="button"
          variant={dedup ? "default" : "outline"}
        >
          Dedup: {dedup ? "on" : "off"}
        </Button>
      </div>

      {error && (
        <p className="rounded-lg border border-destructive/30 bg-destructive/10 px-3 py-2 text-sm text-destructive">
          {error}
        </p>
      )}

      {response && (
        <BatchedResults
          data={response}
          loadingMore={loadingMore}
          onShowMore={(level) => void handleShowMore(level)}
        />
      )}
    </div>
  );
};

export default App;
