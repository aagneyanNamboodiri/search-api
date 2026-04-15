import psycopg2
from pypika import Query, Schema, Field, Criterion
from pypika.terms import ValueWrapper

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
    entity_queries: list[tuple[str, str, list[int]]],
) -> list[tuple[int, str]]:
    """
    Find detected_schema rows matching entity IDs, tagged with entity name.

    entity_queries: list of (entity_name, fk_col, ids)
    Returns: list of (detected_schema_id, entity_name)
    """
    if not entity_queries:
        return []

    schema = Schema(tenant)
    table = schema.analytics_detected_schema

    sub_queries = []
    for entity_name, fk_col, ids in entity_queries:
        if not ids:
            continue
        q = (
            Query.from_(table)
            .select(Field("id"), ValueWrapper(entity_name).as_("entity"))
            .where(Field(fk_col).isin(ids))
        )
        sub_queries.append(q.get_sql())

    if not sub_queries:
        return []

    sql = " UNION ALL ".join(sub_queries)
    return _execute_query(sql)


def fetch_session_metric_ids(
    tenant: str,
    detected_schema_ids: list[int],
) -> list[tuple[int, int]]:
    """
    Get (session_metric_id, detected_schema_id) pairs from the
    shelf facings table.
    """
    if not detected_schema_ids:
        return []

    schema = Schema(tenant)
    shelf = schema.analytics_shelf_facings_detail

    query = (
        Query.from_(shelf)
        .select(Field("session_metric_id"), Field("detected_schema_id"))
        .where(Field("detected_schema_id").isin(detected_schema_ids))
    )

    return _execute_query(query.get_sql())


def fetch_session_uuids(
    tenant: str,
    session_metric_ids: list[int],
) -> list[tuple[int, str]]:
    """
    Get (id, session_uuid) from session_facings_detail,
    excluding soft-deleted rows. The id here is the session_metric_id
    from the facings tables.
    """
    if not session_metric_ids:
        return []

    schema = Schema(tenant)
    table = schema.analytics_session_facings_detail

    query = (
        Query.from_(table)
        .select(Field("id"), Field("session_uuid"))
        .where(Field("id").isin(session_metric_ids))
        .where(Field("is_deleted").eq(False))
    )

    return _execute_query(query.get_sql())
