import json
import uuid


class DashboardStore:
    def __init__(self, conn):
        self._conn = conn

    def ensure_table(self) -> None:
        with self._conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS dashboards (
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    table_name TEXT NOT NULL,
                    viewer_config JSONB NOT NULL,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                )
            """)
        self._conn.commit()

    def list(self) -> list[dict]:
        with self._conn.cursor() as cur:
            cur.execute(
                "SELECT id, name, table_name, created_at FROM dashboards ORDER BY created_at DESC"
            )
            columns = [desc[0] for desc in cur.description]
            return [dict(zip(columns, row)) for row in cur.fetchall()]

    def save(self, name: str, table_name: str, viewer_config: dict) -> dict:
        dashboard_id = uuid.uuid4().hex[:12]
        with self._conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO dashboards (id, name, table_name, viewer_config)
                VALUES (%s, %s, %s, %s)
                RETURNING id, name, table_name, viewer_config, created_at
                """,
                (dashboard_id, name, table_name, json.dumps(viewer_config)),
            )
            columns = [desc[0] for desc in cur.description]
            row = cur.fetchone()
        self._conn.commit()
        return dict(zip(columns, row))

    def get(self, dashboard_id: str) -> dict | None:
        with self._conn.cursor() as cur:
            cur.execute(
                "SELECT id, name, table_name, viewer_config, created_at FROM dashboards WHERE id = %s",
                (dashboard_id,),
            )
            row = cur.fetchone()
            if row is None:
                return None
            columns = [desc[0] for desc in cur.description]
            return dict(zip(columns, row))

    def delete(self, dashboard_id: str) -> bool:
        with self._conn.cursor() as cur:
            cur.execute("DELETE FROM dashboards WHERE id = %s", (dashboard_id,))
            deleted = cur.rowcount > 0
        self._conn.commit()
        return deleted
