export type SearchFieldGroup = "store" | "entity" | "session";

export type SearchFieldDef = {
  key: string;
  label: string;
  esPath: string;
  group: SearchFieldGroup;
  aliases?: string[];
};

export type SearchClause = {
  field: string;
  value: string;
  group?: SearchFieldGroup;
};

export type SearchToken =
  | {
      kind: "field";
      field: string;
      label: string;
      start: number;
      end: number;
      isValid: boolean;
    }
  | {
      kind: "value";
      field: string;
      value: string;
      start: number;
      end: number;
    }
  | { kind: "separator"; text: string; start: number; end: number }
  | { kind: "text"; text: string; start: number; end: number }
  | { kind: "invalid"; text: string; start: number; end: number };

export type ParsedSearchQuery = {
  raw: string;
  mode: "structured" | "wildcard" | "mixed";
  clauses: SearchClause[];
  term?: string;
  tokens: SearchToken[];
  errors: string[];
};
