from __future__ import annotations

import json
import logging
import uuid

logger = logging.getLogger(__name__)


class DashboardStore:
    def __init__(self, get_connection):
        self._get_connection = get_connection
        self._connection = get_connection()

    def _reconnect(self):
        logger.info("Reconnecting to PostgreSQL")
        try:
            self._connection.close()
        except Exception:
            pass
        self._connection = self._get_connection()

    def _execute(self, operation):
        if self._connection.closed:
            self._reconnect()
        return operation(self._connection)

    def ensure_table(self) -> None:
        def _op(connection):
            with connection.cursor() as cursor:
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS dashboards (
                        id TEXT PRIMARY KEY,
                        name TEXT NOT NULL,
                        table_name TEXT NOT NULL,
                        viewer_config JSONB NOT NULL,
                        created_at TIMESTAMPTZ DEFAULT NOW()
                    )
                """)
            connection.commit()

        self._execute(_op)

    def list(self) -> list[dict]:
        def _op(connection):
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT id, name, table_name, created_at FROM dashboards ORDER BY created_at DESC"
                )
                columns = [desc[0] for desc in cursor.description]
                return [dict(zip(columns, row)) for row in cursor.fetchall()]

        return self._execute(_op)

    def save(self, name: str, table_name: str, viewer_config: dict) -> dict:
        dashboard_id = uuid.uuid4().hex[:12]

        def _op(connection):
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO dashboards (id, name, table_name, viewer_config)
                    VALUES (%s, %s, %s, %s)
                    RETURNING id, name, table_name, viewer_config, created_at
                    """,
                    (dashboard_id, name, table_name, json.dumps(viewer_config)),
                )
                columns = [desc[0] for desc in cursor.description]
                row = cursor.fetchone()
            connection.commit()
            return dict(zip(columns, row))

        return self._execute(_op)

    def get(self, dashboard_id: str) -> dict | None:
        def _op(connection):
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT id, name, table_name, viewer_config, created_at FROM dashboards WHERE id = %s",
                    (dashboard_id,),
                )
                row = cursor.fetchone()
                if row is None:
                    return None
                columns = [desc[0] for desc in cursor.description]
                return dict(zip(columns, row))

        return self._execute(_op)

    def delete(self, dashboard_id: str) -> bool:
        def _op(connection):
            with connection.cursor() as cursor:
                cursor.execute("DELETE FROM dashboards WHERE id = %s", (dashboard_id,))
                deleted = cursor.rowcount > 0
            connection.commit()
            return deleted

        return self._execute(_op)

    # --- Queries ---

    def ensure_queries_table(self) -> None:
        def _op(connection):
            with connection.cursor() as cursor:
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS queries (
                        id TEXT PRIMARY KEY,
                        name TEXT NOT NULL,
                        sql_text TEXT NOT NULL,
                        created_at TIMESTAMPTZ DEFAULT NOW()
                    )
                """)
            connection.commit()

        self._execute(_op)

    def list_queries(self) -> list[dict]:
        def _op(connection):
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT id, name, sql_text, created_at FROM queries ORDER BY created_at DESC"
                )
                columns = [desc[0] for desc in cursor.description]
                return [dict(zip(columns, row)) for row in cursor.fetchall()]

        return self._execute(_op)

    def save_query(self, name: str, sql_text: str) -> dict:
        query_id = uuid.uuid4().hex[:12]

        def _op(connection):
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO queries (id, name, sql_text)
                    VALUES (%s, %s, %s)
                    RETURNING id, name, sql_text, created_at
                    """,
                    (query_id, name, sql_text),
                )
                columns = [desc[0] for desc in cursor.description]
                row = cursor.fetchone()
            connection.commit()
            return dict(zip(columns, row))

        return self._execute(_op)

    def get_query(self, query_id: str) -> dict | None:
        def _op(connection):
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT id, name, sql_text, created_at FROM queries WHERE id = %s",
                    (query_id,),
                )
                row = cursor.fetchone()
                if row is None:
                    return None
                columns = [desc[0] for desc in cursor.description]
                return dict(zip(columns, row))

        return self._execute(_op)

    def update_query(self, query_id: str, name: str, sql_text: str) -> dict | None:
        def _op(connection):
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE queries SET name = %s, sql_text = %s
                    WHERE id = %s
                    RETURNING id, name, sql_text, created_at
                    """,
                    (name, sql_text, query_id),
                )
                row = cursor.fetchone()
                if row is None:
                    return None
                columns = [desc[0] for desc in cursor.description]
            connection.commit()
            return dict(zip(columns, row))

        return self._execute(_op)

    def delete_query(self, query_id: str) -> bool:
        def _op(connection):
            with connection.cursor() as cursor:
                cursor.execute("DELETE FROM queries WHERE id = %s", (query_id,))
                deleted = cursor.rowcount > 0
            connection.commit()
            return deleted

        return self._execute(_op)
