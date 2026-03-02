import os

import psycopg2


def get_postgres_uri() -> str:
    return os.environ["POSTGRES_URI"]


def get_connection():
    return psycopg2.connect(get_postgres_uri())


def ensure_schema(conn) -> None:
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS dashboards (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                table_name TEXT NOT NULL,
                viewer_config JSONB NOT NULL,
                created_at TIMESTAMPTZ DEFAULT NOW()
            )
        """)
    conn.commit()
