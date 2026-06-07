import { useState } from "react";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import {
  Card,
  CardContent,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import type { Batch, BatchedSearchResponse, BatchHit } from "@/types/search";

type BatchedResultsProps = {
  data: BatchedSearchResponse;
  loadingMore: Record<string, boolean>;
  onShowMore: (level: string) => void;
};

const storeLabel = (hit: BatchHit): string | null => {
  if (!hit.store) return null;
  const { title, city } = hit.store;
  return [title, city].filter(Boolean).join(" · ") || null;
};

const hitUrl = (sessionUuid: string) =>
  `/app/store-explorer-v2?q=${encodeURIComponent(sessionUuid)}`;

const formatDate = (raw: string | null): string | null => {
  if (!raw) return null;
  try {
    const d = new Date(raw);
    return d.toLocaleDateString(undefined, {
      month: "short",
      day: "numeric",
      year: "numeric",
    });
  } catch {
    return raw;
  }
};

const HitRow = ({ hit }: { hit: BatchHit }) => (
  <li>
    <a
      href={hitUrl(hit.session_uuid)}
      className="flex flex-col gap-0.5 rounded-md border border-transparent px-2 py-1.5 -mx-2 transition-colors hover:border-border hover:bg-muted/50 active:bg-muted"
    >
      <div className="flex items-center justify-between gap-2">
        <span className="font-mono text-xs text-muted-foreground truncate">
          {hit.session_uuid}
        </span>
        {hit.visit_date && (
          <span className="text-[11px] text-muted-foreground whitespace-nowrap">
            {formatDate(hit.visit_date)}
          </span>
        )}
      </div>
      {hit.matched.length > 0 && (
        <p className="text-sm leading-snug">
          <span
            className="[&_em]:bg-primary/15 [&_em]:not-italic [&_em]:text-primary"
            dangerouslySetInnerHTML={{ __html: hit.matched.join(" … ") }}
          />
          {hit.matched_total > hit.matched.length && (
            <span className="ml-1 text-xs text-muted-foreground">
              +{hit.matched_total - hit.matched.length} more
            </span>
          )}
        </p>
      )}
      {(hit.category_title || storeLabel(hit)) && (
        <div className="flex flex-wrap items-center gap-1.5">
          {hit.category_title && (
            <Badge className="font-normal text-[11px] px-1.5 py-0" variant="outline">
              {hit.category_title}
            </Badge>
          )}
          {storeLabel(hit) && (
            <span className="text-[11px] text-muted-foreground">
              {storeLabel(hit)}
            </span>
          )}
        </div>
      )}
    </a>
  </li>
);

const ChevronIcon = ({ open }: { open: boolean }) => (
  <svg
    xmlns="http://www.w3.org/2000/svg"
    width="16"
    height="16"
    viewBox="0 0 24 24"
    fill="none"
    stroke="currentColor"
    strokeWidth="2"
    strokeLinecap="round"
    strokeLinejoin="round"
    className={`shrink-0 text-muted-foreground transition-transform duration-200 ${open ? "rotate-90" : ""}`}
  >
    <path d="m9 18 6-6-6-6" />
  </svg>
);

const BatchCard = ({
  batch,
  isLoadingMore,
  onShowMore,
}: {
  batch: Batch;
  isLoadingMore: boolean;
  onShowMore: (level: string) => void;
}) => {
  const [open, setOpen] = useState(true);

  return (
    <Card size="sm">
      <CardHeader
        className="cursor-pointer select-none"
        onClick={() => setOpen((o) => !o)}
      >
        <CardTitle className="flex items-center gap-2 text-sm">
          <ChevronIcon open={open} />
          {batch.label}
          <Badge className="font-normal" variant="secondary">
            {batch.hits.length}
            {batch.has_more ? "+" : ""}
          </Badge>
        </CardTitle>
      </CardHeader>
      {open && (
        <CardContent className="space-y-2 pt-0">
          <ul className="space-y-0.5">
            {batch.hits.map((hit) => (
              <HitRow key={`${batch.level}-${hit.session_uuid}`} hit={hit} />
            ))}
          </ul>
          {batch.has_more && (
            <Button
              disabled={isLoadingMore}
              onClick={(e) => {
                e.stopPropagation();
                onShowMore(batch.level);
              }}
              size="sm"
              type="button"
              variant="outline"
            >
              {isLoadingMore ? "Loading…" : "Show more"}
            </Button>
          )}
        </CardContent>
      )}
    </Card>
  );
};

export const BatchedResults = ({
  data,
  loadingMore,
  onShowMore,
}: BatchedResultsProps) => {
  const nonEmpty = data.batches.filter((batch) => batch.hits.length > 0);

  return (
    <div className="space-y-3">
      <p className="flex flex-wrap items-center gap-2 text-sm text-muted-foreground">
        <span>{data.strategy}-search</span>
        <span>·</span>
        <span>{data.took_ms}ms</span>
        {data.deduped && (
          <Badge className="font-normal" variant="outline">
            deduped
          </Badge>
        )}
      </p>

      {nonEmpty.length === 0 ? (
        <p className="text-center text-sm text-muted-foreground">
          No results found.
        </p>
      ) : (
        <div className="space-y-3">
          {nonEmpty.map((batch) => (
            <BatchCard
              key={batch.level}
              batch={batch}
              isLoadingMore={Boolean(loadingMore[batch.level])}
              onShowMore={onShowMore}
            />
          ))}
        </div>
      )}
    </div>
  );
};
