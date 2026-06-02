import { SearchIcon } from "lucide-react";
import { type FormEvent, useState } from "react";

import { SearchResults } from "@/components/SearchResults";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { ApiError, search } from "@/lib/api";
import type { SearchResponse } from "@/types/search";

const App = () => {
  const [query, setQuery] = useState("");
  const [results, setResults] = useState<SearchResponse | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [isLoading, setIsLoading] = useState(false);

  const handleSubmit = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    const trimmed = query.trim();

    if (!trimmed) {
      setError("Enter a search query.");
      setResults(null);
      return;
    }

    setIsLoading(true);
    setError(null);

    try {
      const data = await search({ q: trimmed });
      setResults(data);
    } catch (err) {
      setResults(null);
      if (err instanceof ApiError) {
        setError(err.message);
      } else if (err instanceof Error) {
        setError(err.message);
      } else {
        setError("Something went wrong. Is the API running?");
      }
    } finally {
      setIsLoading(false);
    }
  };

  return (
    <div className="mx-auto flex min-h-svh w-full max-w-2xl flex-col gap-8 px-4 py-12">
      <header className="space-y-2 text-center">
        <h1 className="font-heading text-3xl font-semibold tracking-tight">
          Search
        </h1>
        <p className="text-sm text-muted-foreground">
          Queries the Search API via the dev proxy at{" "}
          <code className="rounded bg-muted px-1.5 py-0.5 text-xs">/api</code>
        </p>
      </header>

      <form className="flex gap-2" onSubmit={handleSubmit}>
        <div className="relative flex-1">
          <SearchIcon
            aria-hidden
            className="pointer-events-none absolute top-1/2 left-2.5 size-4 -translate-y-1/2 text-muted-foreground"
          />
          <Input
            aria-label="Search query"
            className="pl-8"
            placeholder="Search documents…"
            value={query}
            onChange={(event) => setQuery(event.target.value)}
          />
        </div>
        <Button disabled={isLoading} type="submit">
          {isLoading ? "Searching…" : "Search"}
        </Button>
      </form>

      {error && (
        <p className="rounded-lg border border-destructive/30 bg-destructive/10 px-3 py-2 text-sm text-destructive">
          {error}
        </p>
      )}

      {results && <SearchResults data={results} />}
    </div>
  );
};

export default App;
