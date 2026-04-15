import psycopg2
from pypika import Query, Schema, Field, Criterion

from src.config.settings import settings

_pg_conn = None


def get_pg_connection():
    global _pg_conn
    if _pg_conn is None or _pg_conn.closed:
        _pg_conn = psycopg2.connect(
            host=settings.pg_host,
            port=settings.pg_port,
            dbname=settings.pg_database,
            user=settings.pg_user,
            password=settings.pg_password,
        )
    return _pg_conn


def _execute_query(sql: str) -> list[tuple]:
    conn = get_pg_connection()
    with conn.cursor() as cur:
        cur.execute(sql)
        return cur.fetchall()


def fetch_detected_schema_ids(
    tenant: str,
    entity_id_map: dict[str, list[int]],
) -> list[int]:
    """
    Find detected_schema rows where any of the given entity FK columns
    match the provided IDs.

    entity_id_map example: {"brand_id": [12, 45], "category_id": [5]}
    """
    if not entity_id_map:
        return []

    schema = Schema(tenant)
    table = schema.analytics_detected_schema

    conditions = [
        Field(fk_col).isin(ids)
        for fk_col, ids in entity_id_map.items()
        if ids
    ]
    if not conditions:
        return []

    query = (
        Query.from_(table)
        .select(Field("id"))
        .where(Criterion.any(conditions))
    )

    rows = _execute_query(query.get_sql())
    return [row[0] for row in rows]


def fetch_session_metric_ids(
    tenant: str,
    detected_schema_ids: list[int],
) -> list[int]:
    """
    Get session_metric_ids from both shelf and display facings tables
    for the given detected_schema_ids.
    """
    if not detected_schema_ids:
        return []

    schema = Schema(tenant)
    shelf = schema.analytics_shelf_facings_detail
    display = schema.analytics_display_facings_detail

    shelf_query = (
        Query.from_(shelf)
        .select(Field("session_metric_id"))
        .where(Field("detected_schema_id").isin(detected_schema_ids))
    )

    display_query = (
        Query.from_(display)
        .select(Field("session_metric_id"))
        .where(Field("detected_schema_id").isin(detected_schema_ids))
    )

    union_sql = f"{shelf_query.get_sql()} UNION {display_query.get_sql()}"

    rows = _execute_query(union_sql)
    return [row[0] for row in rows]


def fetch_session_uuids(
    tenant: str,
    session_metric_ids: list[int],
) -> list[str]:
    """
    Get distinct session_uuids from session_facings_detail
    for the given session_metric_ids, excluding soft-deleted rows.
    """
    if not session_metric_ids:
        return []

    schema = Schema(tenant)
    table = schema.analytics_session_facings_detail

    query = (
        Query.from_(table)
        .select(Field("session_uuid"))
        .distinct()
        .where(Field("id").isin(session_metric_ids))
        .where(Field("is_deleted").eq(False))
    )

    rows = _execute_query(query.get_sql())
    return [str(row[0]) for row in rows]
