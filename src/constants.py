from dataclasses import dataclass, field


@dataclass
class SearchField:
    name: str
    boost: float = 1.0


@dataclass
class SearchableEntity:
    table_name: str
    search_fields: list[SearchField] = field(default_factory=list)
    size: int = 5
    fuzziness: str = "AUTO"


DEFAULT_SEARCH_FIELDS = [
    SearchField(name="internal_name", boost=1.0),
    SearchField(name="external_name", boost=0.2),
]

SEARCHABLE_ENTITIES: list[SearchableEntity] = [
    SearchableEntity(
        table_name="common_brand",
        search_fields=DEFAULT_SEARCH_FIELDS,
    ),
    SearchableEntity(
        table_name="common_category",
        search_fields=DEFAULT_SEARCH_FIELDS,
    ),
]
