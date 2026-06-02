"""Authoritative mapping between frontend field keys and Elasticsearch paths.

The frontend owns query parsing; this module owns how those parsed pieces map
onto the actual index. Kept declarative (data + small functions, no classes with
behavior) so the query builder can stay a set of pure functions.
"""

from dataclasses import dataclass
from typing import Any

# Subfield boosts applied within every schema level (and reused for store).
# name > id > title, per the agreed relevance ordering.
NAME_BOOST = 3
ID_BOOST = 2
TITLE_BOOST = 1


@dataclass(frozen=True)
class LevelDef:
    """A searchable "level" that can become a result batch."""

    key: str  # canonical key used on the wire / as batch id (ES array name)
    label: str  # human-friendly batch label
    boosted_fields: tuple[str, ...]  # multi_match fields with ^boost suffixes
    highlight_fields: tuple[str, ...]  # fields to request highlights for


def _entity_level(es_array: str, label: str) -> LevelDef:
    return LevelDef(
        key=es_array,
        label=label,
        boosted_fields=(
            f"{es_array}.name^{NAME_BOOST}",
            f"{es_array}.id^{ID_BOOST}",
            f"{es_array}.title^{TITLE_BOOST}",
        ),
        highlight_fields=(f"{es_array}.name", f"{es_array}.title"),
    )


# Schema levels searched by a wildcard term, keyed by ES array name.
SCHEMA_LEVELS: dict[str, LevelDef] = {
    level.key: level
    for level in (
        _entity_level("brands", "Brand"),
        _entity_level("categories", "Category"),
        _entity_level("sub_categories", "Sub-category"),
        _entity_level("skus", "SKU"),
        _entity_level("variants", "Variant"),
    )
}

# The store is its own level (only a batch when no store filter is present).
STORE_LEVEL = LevelDef(
    key="store",
    label="Store",
    boosted_fields=(
        f"store.title^{NAME_BOOST}",
        "store.city^2",
        "store.state",
        "store.region",
        "store.country",
        "store.type",
        "store.brand",
        "store.area",
        "store.branch",
        "store.agency",
    ),
    highlight_fields=(
        "store.title",
        "store.city",
        "store.state",
        "store.region",
        "store.country",
        "store.type",
        "store.brand",
    ),
)

ALL_LEVELS: dict[str, LevelDef] = {**SCHEMA_LEVELS, STORE_LEVEL.key: STORE_LEVEL}

# Accept the singular frontend keys and normalize them to the ES array name.
_LEVEL_ALIASES: dict[str, str] = {
    "brand": "brands",
    "category": "categories",
    "sub_category": "sub_categories",
    "subcategory": "sub_categories",
    "sku": "skus",
    "variant": "variants",
}

# Store clause field keys -> ES subfield path. "store.name" is an alias of title.
_STORE_FIELD_PATHS: dict[str, str] = {
    "store.id": "store.id",
    "store.title": "store.title",
    "store.name": "store.title",
    "store.type": "store.type",
    "store.brand": "store.brand",
    "store.area": "store.area",
    "store.branch": "store.branch",
    "store.agency": "store.agency",
    "store.aw": "store.aw",
    "store.city": "store.city",
    "store.state": "store.state",
    "store.region": "store.region",
    "store.country": "store.country",
}

# Fields returned in every hit's _source (minimal payload for speed).
HIT_SOURCE_FIELDS: tuple[str, ...] = (
    "session_uuid",
    "store",
    "categories.title",
)


def normalize_level_key(key: str) -> str | None:
    """Resolve a frontend level key (singular or plural) to its ES array name."""
    normalized = key.strip().lower()
    if normalized in SCHEMA_LEVELS:
        return normalized
    return _LEVEL_ALIASES.get(normalized)


def resolve_level(key: str) -> LevelDef | None:
    normalized = normalize_level_key(key)
    return SCHEMA_LEVELS.get(normalized) if normalized else None


def schema_level_keys() -> list[str]:
    """ES array names of all schema levels, in display order."""
    return list(SCHEMA_LEVELS.keys())


def build_store_filter(field: str, value: str) -> dict[str, Any] | None:
    """Translate a single store clause into a filter-context query.

    store.id matches exactly (term); all text fields use an analyzed match so
    the filter stays forgiving and case-insensitive.
    """
    path = _STORE_FIELD_PATHS.get(field.strip().lower())
    if not path:
        return None

    if path == "store.id":
        try:
            return {"term": {"store.id": int(value)}}
        except (TypeError, ValueError):
            return None

    return {"match": {path: value}}
