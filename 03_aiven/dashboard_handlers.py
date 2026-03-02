import json
import pathlib

import perspective
import tornado.web

from dashboard_store import DashboardStore

_TEMPLATES_DIR = str(pathlib.Path(__file__).parent / "templates")


class DashboardApiHandler(tornado.web.RequestHandler):
    _store: DashboardStore

    def initialize(self, store: DashboardStore) -> None:
        self._store = store

    def get(self) -> None:
        dashboards = self._store.list()
        for d in dashboards:
            if d.get("created_at"):
                d["created_at"] = d["created_at"].isoformat()
        self.set_header("Content-Type", "application/json")
        self.write(json.dumps(dashboards))

    def post(self) -> None:
        body = json.loads(self.request.body)
        name = body["name"]
        table_name = body["table_name"]
        viewer_config = body["viewer_config"]
        record = self._store.save(name, table_name, viewer_config)
        if record.get("created_at"):
            record["created_at"] = record["created_at"].isoformat()
        self.set_header("Content-Type", "application/json")
        self.set_status(201)
        self.write(json.dumps(record))


class DashboardDetailHandler(tornado.web.RequestHandler):
    _store: DashboardStore

    def initialize(self, store: DashboardStore) -> None:
        self._store = store

    def delete(self, dashboard_id: str) -> None:
        deleted = self._store.delete(dashboard_id)
        if not deleted:
            self.set_status(404)
            self.write(json.dumps({"error": "not found"}))
            return
        self.write(json.dumps({"ok": True}))


class DashboardListHandler(tornado.web.RequestHandler):
    _store: DashboardStore
    _table_names: list[str]

    def initialize(self, store: DashboardStore, table_names: list[str]) -> None:
        self._store = store
        self._table_names = table_names

    async def get(self) -> None:
        dashboards = self._store.list()
        await self.render(
            _TEMPLATES_DIR + "/dashboard_list.html",
            dashboards=dashboards,
            table_names=self._table_names,
        )


class DashboardViewHandler(tornado.web.RequestHandler):
    _store: DashboardStore

    def initialize(self, store: DashboardStore) -> None:
        self._store = store

    async def get(self, dashboard_id: str) -> None:
        dashboard = self._store.get(dashboard_id)
        if dashboard is None:
            self.set_status(404)
            self.write("Dashboard not found")
            return
        viewer_config = dashboard["viewer_config"]
        if isinstance(viewer_config, str):
            viewer_config = json.loads(viewer_config)
        await self.render(
            _TEMPLATES_DIR + "/dashboard_view.html",
            dashboard=dashboard,
            viewer_config_json=json.dumps(viewer_config),
            perspective_version=perspective.__version__,
        )
