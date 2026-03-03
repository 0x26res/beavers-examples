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
