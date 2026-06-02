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

const HitRow = ({ hit }: { hit: BatchHit }) => (
  <li className="space-y-1 border-b border-border/40 pb-2 last:border-b-0 last:pb-0">
    <div className="flex items-center justify-between gap-2">
      <span className="font-mono text-xs text-muted-foreground">
        {hit.session_uuid}
      </span>
      {hit.score != null && (
        <span className="text-xs text-muted-foreground">
          {hit.score.toFixed(2)}
        </span>
      )}
    </div>
    {hit.matched.length > 0 && (
      <p
        className="text-sm [&_em]:bg-primary/15 [&_em]:not-italic [&_em]:text-primary"
        // ES highlight fragments are server-controlled markup (<em> only).
        dangerouslySetInnerHTML={{ __html: hit.matched.join(" … ") }}
      />
    )}
    <div className="flex flex-wrap items-center gap-1.5">
      {hit.category_title && (
        <Badge className="font-normal" variant="outline">
          {hit.category_title}
        </Badge>
      )}
      {storeLabel(hit) && (
        <span className="text-xs text-muted-foreground">{storeLabel(hit)}</span>
      )}
    </div>
  </li>
);

const BatchCard = ({
  batch,
  isLoadingMore,
  onShowMore,
}: {
  batch: Batch;
  isLoadingMore: boolean;
  onShowMore: (level: string) => void;
}) => (
  <Card size="sm">
    <CardHeader>
      <CardTitle className="flex items-center gap-2 text-sm">
        {batch.label}
        <Badge className="font-normal" variant="secondary">
          {batch.hits.length}
          {batch.has_more ? "+" : ""}
        </Badge>
      </CardTitle>
    </CardHeader>
    <CardContent className="space-y-3">
      <ul className="space-y-2">
        {batch.hits.map((hit) => (
          <HitRow key={`${batch.level}-${hit.session_uuid}`} hit={hit} />
        ))}
      </ul>
      {batch.has_more && (
        <Button
          disabled={isLoadingMore}
          onClick={() => onShowMore(batch.level)}
          size="sm"
          type="button"
          variant="outline"
        >
          {isLoadingMore ? "Loading…" : "Show more"}
        </Button>
      )}
    </CardContent>
  </Card>
);

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
