import type { SearchFieldDef, SearchFieldGroup } from "@/lib/search-query/types";

const storeField = (
  key: string,
  label: string,
  aliases?: string[],
): SearchFieldDef => ({
  key,
  label,
  esPath: key,
  group: "store",
  aliases,
});

const entityField = (
  base: string,
  label: string,
  esArray: string,
  aliases?: string[],
): SearchFieldDef => ({
  key: base,
  label,
  esPath: esArray,
  group: "entity",
  aliases,
});

const STORE_FIELDS: SearchFieldDef[] = [
  storeField("store.id", "Store ID"),
  storeField("store.title", "Store title", ["store.name"]),
  storeField("store.type", "Store type"),
  storeField("store.brand", "Store brand"),
  storeField("store.area", "Store area"),
  storeField("store.branch", "Store branch"),
  storeField("store.agency", "Store agency"),
  storeField("store.aw", "Store AW"),
  storeField("store.city", "Store city"),
  storeField("store.state", "Store state"),
  storeField("store.region", "Store region"),
  storeField("store.country", "Store country"),
];

const entityFields = (
  base: string,
  label: string,
  esArray: string,
): SearchFieldDef[] => [
  entityField(base, label, esArray),
  entityField(`${base}.id`, `${label} ID`, esArray),
  entityField(`${base}.name`, `${label} name`, esArray),
  entityField(`${base}.title`, `${label} title`, esArray),
];

const ENTITY_FIELDS: SearchFieldDef[] = [
  ...entityFields("brand", "Brand", "brands"),
  ...entityFields("category", "Category", "categories"),
  ...entityFields("sub_category", "Sub-category", "sub_categories"),
  ...entityFields("sku", "SKU", "skus"),
  ...entityFields("variant", "Variant", "variants"),
];

const SESSION_FIELDS: SearchFieldDef[] = [
  {
    key: "session_uuid",
    label: "Session UUID",
    esPath: "session_uuid",
    group: "session",
  },
  {
    key: "visit_date",
    label: "Visit date",
    esPath: "visit_date",
    group: "session",
  },
];

export const SEARCH_FIELDS: SearchFieldDef[] = [
  ...STORE_FIELDS,
  ...ENTITY_FIELDS,
  ...SESSION_FIELDS,
];

const fieldByKey = new Map<string, SearchFieldDef>();

const registerField = (lookupKey: string, field: SearchFieldDef) => {
  fieldByKey.set(lookupKey.toLowerCase(), field);
};

for (const field of SEARCH_FIELDS) {
  registerField(field.key, field);
  for (const alias of field.aliases ?? []) {
    registerField(alias, field);
  }
}

export const resolveSearchField = (key: string): SearchFieldDef | undefined =>
  fieldByKey.get(key.trim().toLowerCase());

export const getFieldsByGroup = (
  group: SearchFieldGroup,
): SearchFieldDef[] => {
  const seen = new Set<string>();
  return SEARCH_FIELDS.filter((field) => {
    if (field.group !== group || seen.has(field.key)) {
      return false;
    }
    seen.add(field.key);
    return true;
  });
};

export const getHelpFieldKeys = (group: SearchFieldGroup): string[] => {
  const keys = new Set<string>();

  for (const field of getFieldsByGroup(group)) {
    keys.add(field.key);
    for (const alias of field.aliases ?? []) {
      keys.add(alias);
    }
  }

  return [...keys].sort();
};
