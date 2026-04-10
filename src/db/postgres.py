import psycopg2
from pypika import Query, Table, Field

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


def fetch_records_by_ids(tenant: str, ids: list[str]) -> list[dict]:
    """
    Fetch full records from PostgreSQL for the given IDs using PyPika.
    Tenant maps to the appropriate schema/table in the DB.
    """
    if not ids:
        return []

    conn = get_pg_connection()

    # TODO: Replace with your actual table/column names.
    # The tenant can be used as a schema prefix or to pick the right table.
    table = Table("items")  # TODO: e.g. Table("items").as_(tenant) or Schema(tenant).items

    query = (
        Query.from_(table)
        .select("*")
        .where(Field("id").isin(ids))
    )

    sql = query.get_sql()

    with conn.cursor() as cur:
        cur.execute(sql)
        columns = [desc[0] for desc in cur.description]
        rows = cur.fetchall()

    return [dict(zip(columns, row)) for row in rows]
