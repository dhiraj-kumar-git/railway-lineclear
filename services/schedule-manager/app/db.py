"""Database access for Schedule Manager: one small connection pool per pod."""

import os
from pathlib import Path

from psycopg.conninfo import make_conninfo
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

SQL_DIR = Path(__file__).resolve().parent.parent / "sql"
SCHEMA_LOCK_ID = 7106  # any fixed number; replicas take turns applying the schema


def create_pool() -> ConnectionPool:
    conninfo = make_conninfo(
        host=os.environ["DB_HOST"],
        port=os.environ.get("DB_PORT", "5432"),
        dbname=os.environ["DB_NAME"],
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
    )
    return ConnectionPool(
        conninfo,
        min_size=1,
        max_size=4,
        kwargs={"row_factory": dict_row},
        open=False,
    )


def apply_schema(pool: ConnectionPool) -> None:
    """Create the tables and seed rows if they are missing. Safe to run on every start."""
    with pool.connection() as conn:
        conn.execute("SELECT pg_advisory_xact_lock(%s)", (SCHEMA_LOCK_ID,))
        conn.execute((SQL_DIR / "schema.sql").read_text())
        conn.execute((SQL_DIR / "seed.sql").read_text())