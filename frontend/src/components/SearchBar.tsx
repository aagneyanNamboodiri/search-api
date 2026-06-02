import { SearchIcon } from "lucide-react";
import { type FormEvent, useMemo } from "react";

import { SearchHelpDialog } from "@/components/SearchHelpDialog";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { resolveSearchField } from "@/lib/search-query/fields";
import { parseSearchQuery } from "@/lib/search-query/parser";

type SearchBarProps = {
  value: string;
  onChange: (value: string) => void;
  onSubmit: (query: string) => void;
  isLoading?: boolean;
  disabled?: boolean;
};

export const SearchBar = ({
  value,
  onChange,
  onSubmit,
  isLoading = false,
  disabled = false,
}: SearchBarProps) => {
  const parsed = useMemo(() => parseSearchQuery(value), [value]);

  const handleSubmit = (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    onSubmit(value.trim());
  };

  const showSummary = parsed.clauses.length > 0 || Boolean(parsed.term);
  const isDisabled = disabled || isLoading;

  return (
    <form className="space-y-2" onSubmit={handleSubmit}>
      <div className="flex gap-2">
        <div className="relative flex-1">
          <SearchIcon
            aria-hidden
            className="pointer-events-none absolute top-1/2 left-2.5 size-4 -translate-y-1/2 text-muted-foreground"
          />
          <Input
            aria-label="Search query"
            autoCapitalize="off"
            autoComplete="off"
            autoCorrect="off"
            className="pl-8"
            disabled={isDisabled}
            placeholder="Search or use field: value…"
            spellCheck={false}
            value={value}
            onChange={(event) => onChange(event.target.value)}
          />
        </div>
        <SearchHelpDialog />
        <Button disabled={isDisabled} type="submit">
          {isLoading ? "Searching…" : "Search"}
        </Button>
      </div>

      {showSummary && (
        <div className="flex flex-wrap gap-1.5">
          {parsed.clauses.map((clause) => {
            const field = resolveSearchField(clause.field);
            return (
              <Badge key={`${clause.field}-${clause.value}`} variant="secondary">
                {field?.label ?? clause.field} · {clause.value}
              </Badge>
            );
          })}
          {parsed.term && (
            <Badge variant="outline">Search · {parsed.term}</Badge>
          )}
        </div>
      )}

      {parsed.errors.length > 0 && (
        <p className="text-xs text-destructive">{parsed.errors.join(" · ")}</p>
      )}
    </form>
  );
};
