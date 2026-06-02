import { CircleHelpIcon } from "lucide-react";

import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from "@/components/ui/dialog";
import { ScrollArea } from "@/components/ui/scroll-area";
import { Separator } from "@/components/ui/separator";
import { getFieldsByGroup, getHelpFieldKeys } from "@/lib/search-query/fields";

const EXAMPLES = [
  {
    title: "Free-text search",
    description: "Search anything across all fields.",
    query: "neurobion",
  },
  {
    title: "Store search",
    description: "Filter by a specific store attribute.",
    query: "store.city: city 228",
  },
  {
    title: "Store + product search",
    description: "Combine filters with commas (AND).",
    query: "store.name: Modulo, brand: beer",
  },
  {
    title: "Product search",
    description: "Find a brand, SKU, or category across all stores.",
    query: "brand: vicks",
  },
] as const;

const FieldList = ({ fields }: { fields: string[] }) => (
  <div className="flex flex-wrap gap-1.5">
    {fields.map((field) => (
      <Badge key={field} className="font-normal" variant="outline">
        {field}
      </Badge>
    ))}
  </div>
);

export const SearchHelpDialog = () => (
  <Dialog>
    <DialogTrigger
      render={
        <Button
          aria-label="Search help"
          size="icon"
          type="button"
          variant="outline"
        />
      }
    >
      <CircleHelpIcon />
    </DialogTrigger>
    <DialogContent className="gap-0 overflow-hidden p-0 sm:max-w-lg">
      <DialogHeader className="px-4 pt-4">
        <DialogTitle>How to search</DialogTitle>
        <DialogDescription>
          Use free text or structured{" "}
          <code className="rounded bg-muted px-1 py-0.5 text-xs">field: value</code>{" "}
          clauses separated by commas.
        </DialogDescription>
      </DialogHeader>
      <ScrollArea className="max-h-[min(70vh,32rem)] px-4 pb-4">
        <div className="space-y-5 pr-3">
          <section className="space-y-3">
            <h3 className="text-sm font-medium">Quick start</h3>
            <div className="space-y-3">
              {EXAMPLES.map((example) => (
                <div key={example.title} className="space-y-1.5">
                  <p className="text-sm font-medium">{example.title}</p>
                  <p className="text-xs text-muted-foreground">
                    {example.description}
                  </p>
                  <Badge className="font-normal" variant="secondary">
                    {example.query}
                  </Badge>
                </div>
              ))}
            </div>
          </section>

          <Separator />

          <section className="space-y-2">
            <h3 className="text-sm font-medium">Syntax</h3>
            <ul className="list-inside list-disc space-y-1 text-sm text-muted-foreground">
              <li>
                Write{" "}
                <code className="rounded bg-muted px-1 py-0.5 text-xs">
                  field: value
                </code>{" "}
                for named searches.
              </li>
              <li>Separate multiple filters with commas — all must match.</li>
              <li>
                Without a recognized field prefix, your query is sent as free
                text.
              </li>
            </ul>
          </section>

          <Separator />

          <section className="space-y-2">
            <h3 className="text-sm font-medium">Store fields</h3>
            <FieldList fields={getHelpFieldKeys("store")} />
          </section>

          <section className="space-y-2">
            <h3 className="text-sm font-medium">Product fields</h3>
            <FieldList
              fields={getHelpFieldKeys("entity").filter(
                (field) =>
                  !field.includes(".id") &&
                  !field.includes(".name") &&
                  !field.includes(".title"),
              )}
            />
            <p className="text-xs text-muted-foreground">
              Also accepts{" "}
              {getFieldsByGroup("entity")
                .filter((field) => field.key.includes("."))
                .map((field) => field.key)
                .slice(0, 4)
                .join(", ")}
              , and similar variants.
            </p>
          </section>

          <Separator />

          <section className="space-y-2">
            <h3 className="text-sm font-medium">Tips</h3>
            <ul className="list-inside list-disc space-y-1 text-sm text-muted-foreground">
              <li>
                <code className="rounded bg-muted px-1 py-0.5 text-xs">
                  store.name
                </code>{" "}
                is an alias for{" "}
                <code className="rounded bg-muted px-1 py-0.5 text-xs">
                  store.title
                </code>
                .
              </li>
              <li>
                <code className="rounded bg-muted px-1 py-0.5 text-xs">
                  store.brand
                </code>{" "}
                is the store&apos;s brand attribute, not a product brand. Use{" "}
                <code className="rounded bg-muted px-1 py-0.5 text-xs">
                  brand
                </code>{" "}
                for product brands.
              </li>
            </ul>
          </section>
        </div>
      </ScrollArea>
    </DialogContent>
  </Dialog>
);
