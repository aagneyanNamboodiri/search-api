import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import type { SearchResponse } from "@/types/search";

type SearchResultsProps = {
  data: SearchResponse;
};

const formatSource = (source: Record<string, unknown>) => {
  const title =
    typeof source.title === "string"
      ? source.title
      : typeof source.name === "string"
        ? source.name
        : null;

  if (title) {
    return title;
  }

  return JSON.stringify(source, null, 2);
};

export const SearchResults = ({ data }: SearchResultsProps) => {
  if (data.hits.length === 0) {
    return (
      <p className="text-center text-sm text-muted-foreground">
        No results found.
      </p>
    );
  }

  return (
    <div className="space-y-3">
      <p className="text-sm text-muted-foreground">
        {data.total.toLocaleString()} result{data.total === 1 ? "" : "s"} ·{" "}
        {data.took_ms}ms
      </p>
      <ul className="space-y-3">
        {data.hits.map((hit) => (
          <li key={hit.id}>
            <Card size="sm">
              <CardHeader>
                <CardTitle className="font-mono text-xs text-muted-foreground">
                  {hit.id}
                </CardTitle>
                {hit.score != null && (
                  <CardDescription>Score {hit.score.toFixed(3)}</CardDescription>
                )}
              </CardHeader>
              <CardContent>
                <pre className="max-h-40 overflow-auto whitespace-pre-wrap font-sans text-sm">
                  {formatSource(hit.source)}
                </pre>
              </CardContent>
            </Card>
          </li>
        ))}
      </ul>
    </div>
  );
};
