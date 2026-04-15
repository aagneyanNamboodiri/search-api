from src.constants import (
    SearchField,
    SearchableEntity,
    DEFAULT_SEARCH_FIELDS,
    SEARCHABLE_ENTITIES,
    ENTITY_TO_DETECTED_SCHEMA_FK,
)


def test_search_field_defaults():
    f = SearchField(name="title")
    assert f.name == "title"
    assert f.boost == 1.0


def test_search_field_custom_boost():
    f = SearchField(name="title", boost=0.5)
    assert f.boost == 0.5


def test_searchable_entity_defaults():
    entity = SearchableEntity(table_name="common_brand")
    assert entity.table_name == "common_brand"
    assert entity.search_fields == []
    assert entity.fuzziness == "AUTO"


def test_default_search_fields_has_two_fields():
    assert len(DEFAULT_SEARCH_FIELDS) == 2
    names = [f.name for f in DEFAULT_SEARCH_FIELDS]
    assert "internal_name" in names
    assert "external_name" in names


def test_searchable_entities_has_brand_and_category():
    table_names = [e.table_name for e in SEARCHABLE_ENTITIES]
    assert "common_brand" in table_names
    assert "common_category" in table_names


def test_all_searchable_entities_have_fk_mapping():
    for entity in SEARCHABLE_ENTITIES:
        assert entity.table_name in ENTITY_TO_DETECTED_SCHEMA_FK, (
            f"{entity.table_name} missing from ENTITY_TO_DETECTED_SCHEMA_FK"
        )


def test_fk_mapping_values():
    assert ENTITY_TO_DETECTED_SCHEMA_FK["common_brand"] == "brand_id"
    assert ENTITY_TO_DETECTED_SCHEMA_FK["common_category"] == "category_id"
