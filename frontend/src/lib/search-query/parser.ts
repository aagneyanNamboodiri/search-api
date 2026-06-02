import { resolveSearchField } from "@/lib/search-query/fields";
import type {
  ParsedSearchQuery,
  SearchClause,
  SearchToken,
} from "@/lib/search-query/types";

const CLAUSE_PATTERN = /^([^:]+?)\s*:\s*(.+)$/;

type SegmentSpan = {
  start: number;
  end: number;
  text: string;
};

const getSegments = (raw: string): SegmentSpan[] => {
  const segments: SegmentSpan[] = [];
  let clauseStart = 0;

  for (let index = 0; index <= raw.length; index += 1) {
    if (index === raw.length || raw[index] === ",") {
      const text = raw.slice(clauseStart, index);
      if (text.trim()) {
        segments.push({ start: clauseStart, end: index, text });
      }
      clauseStart = index + 1;
    }
  }

  return segments;
};

const parseClause = (
  segmentText: string,
): { field: string; value: string } | null => {
  const match = segmentText.trim().match(CLAUSE_PATTERN);
  if (!match) {
    return null;
  }

  return {
    field: match[1].trim(),
    value: match[2].trim(),
  };
};

/**
 * Parse a query into recognized clauses plus a residual free-text term.
 *
 * Segments are comma-separated. A segment that matches `field: value` against a
 * known field becomes a clause; everything else is collected as the free-text
 * `term`. This supports mixed inputs like `store.name: ABC, corona` where a
 * store filter and a wildcard term coexist.
 */
export const parseSearchQuery = (raw: string): ParsedSearchQuery => {
  const trimmedRaw = raw.trim();
  if (!trimmedRaw) {
    return { raw, mode: "wildcard", clauses: [], term: undefined, tokens: [], errors: [] };
  }

  const segments = getSegments(raw);
  const clauses: SearchClause[] = [];
  const residual: string[] = [];
  const tokens: SearchToken[] = [];

  for (const segment of segments) {
    const parsed = parseClause(segment.text);
    const fieldDef = parsed ? resolveSearchField(parsed.field) : null;

    if (parsed && fieldDef) {
      clauses.push({
        field: fieldDef.key,
        value: parsed.value,
        group: fieldDef.group,
      });
      tokens.push({
        kind: "field",
        field: fieldDef.key,
        label: fieldDef.label,
        start: segment.start,
        end: segment.end,
        isValid: true,
      });
      continue;
    }

    residual.push(segment.text.trim());
    tokens.push({
      kind: "text",
      text: segment.text.trim(),
      start: segment.start,
      end: segment.end,
    });
  }

  const term = residual.join(" ").trim() || undefined;

  const mode: ParsedSearchQuery["mode"] =
    clauses.length > 0 ? (term ? "mixed" : "structured") : "wildcard";

  return { raw, mode, clauses, term, tokens, errors: [] };
};
