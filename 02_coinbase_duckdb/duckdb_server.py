import duckdb
import dataclasses
import logging
import pathlib
import random
import string
from typing import Callable

import perspective
import pyarrow as pa
from beavers import Dag, Node
from beavers.perspective_wrapper import ASSETS_DIRECTORY, _table_to_bytes
from beavers.kafka import KafkaDriver, SourceTopic
import pandas as pd
import tornado

from util.json_util import JsonArrowParser

logger = logging.getLogger(__name__)

TABLE = str(pathlib.Path(__file__).parent / "table.html")
BASE64_CHARS = string.ascii_letters + string.digits
DEFAULT_QUERY = "SELECT * FROM ticker JOIN status ON ticker.product_id = status.id"




def generate_id() -> str:
    return "".join(random.choices(BASE64_CHARS, k=30))  # nosec B311


@dataclasses.dataclass(frozen=True)
class TableStore:
    tables: dict[str, bytes] = dataclasses.field(default_factory=dict)

    def put(self, table_name: str, table: bytes):
        while len(self.tables) >= 10:
            self.tables.pop(next(iter(self.tables)))
        self.tables[table_name] = table

    def get(self, table_name: str) -> bytes:
        return self.tables[table_name]


class TableRequestHandler(tornado.web.RequestHandler):
    table_store: TableStore = None

    def initialize(self, table_store: TableStore):
        self.table_store = table_store

    async def get(self, path: str):
        self.write(self.table_store.get(path))


class QueryRequestHandler(tornado.web.RequestHandler):
    tables_getter: Callable[[], dict[str, pa.Table]] = None
    table_store: TableStore

    def initialize(
        self,
        tables_getter: Callable[[], dict[str, pa.Table]],
        table_store: TableStore,
    ) -> None:
        self.tables_getter = tables_getter
        self.table_store = table_store

    async def get(self, path: str) -> None:
        await self.render(
            TABLE,
            table_name=None,
            query=DEFAULT_QUERY,
            perspective_version=perspective.__version__,
            error_message=None,
        )

    async def post(self, path: str) -> None:
        query = self.get_argument("query")
        logger.info("POST query: %s", query)
        table_name = None
        error_message = None
        if query:
            try:
                table = self.run_query(query)
                table_name = generate_id()
                self.table_store.put(table_name, _table_to_bytes(table))
            except Exception as e:
                logger.error(f"Error executing query: {query}", exc_info=True)
                error_message = str(e)

        await self.render(
            TABLE,
            table_name=table_name,
            perspective_version=perspective.__version__,
            query=query,
            error_message=error_message,
        )

    def run_query(self, query: str) -> pa.Table:
        duckdb_connection = duckdb.connect(":memory:")
        for name, table in self.tables_getter().items():
            duckdb_connection.register(name, table)
        return duckdb_connection.sql(query).to_arrow_table()


def add_average_price(ticker: pa.Table, average_price: pa.Table) -> pa.Table:
    return ticker.join(average_price, keys="product_id")


def register_driver(
    kafka_driver: KafkaDriver,
):
    callback = tornado.ioloop.PeriodicCallback(
        callback=lambda: kafka_driver.run_cycle(0.0), callback_time=1_000
    )
    callback.start()


def duckdb_server(port: int = 8082):
    dag = Dag()
    ticker_source: Node[pa.Table] = dag.pa.source_table(
        schema=TICKER_SCHEMA, name="ticker"
    )
    ticker_state: Node[pa.Table] = dag.pa.last_by_keys(
        ticker_source, keys=["product_id"]
    )

    status_source: Node[pa.Table] = dag.pa.source_table(
        schema=STATUS_SCHEMA, name="status"
    )
    status_state: Node[pa.Table] = dag.pa.last_by_keys(status_source, keys=["id"])

    tables: Node[dict[str, pa.Table]] = dag.state(dict).map(
        ticker=ticker_state, status=status_state
    )
    sink = dag.sink("tables", tables)

    kafka_driver = KafkaDriver.create(
        dag,
        producer_config={"bootstrap.servers": "localhost:9092"},
        consumer_config={"group.id": "beavers", "bootstrap.servers": "localhost:9092"},
        source_topics={
            "ticker": SourceTopic.from_relative_time(
                "ticker",
                JsonArrowParser.create(TICKER_SCHEMA),
                relative_time=pd.to_timedelta("1h"),
            ),
            "status": SourceTopic.from_relative_time(
                "status",
                JsonArrowParser.create(STATUS_SCHEMA),
                relative_time=pd.to_timedelta("24h"),
            ),
        },
        sink_topics={},
        batch_size=20_000,
    )
    dag.execute(pd.Timestamp.utcnow())

    loop = tornado.ioloop.IOLoop.current()
    loop.call_later(0, register_driver, kafka_driver)
    logger.info("Ready to serve on http://localhost:%s", port)

    table_store = TableStore()

    web_app = tornado.web.Application(
        [
            (
                r"/assets/(.*)",
                tornado.web.StaticFileHandler,
                {"path": ASSETS_DIRECTORY, "default_filename": None},
            ),
            (r"/table/(.*)", TableRequestHandler, {"table_store": table_store}),
            (
                r"/(.*)",
                QueryRequestHandler,
                {"table_store": table_store, "tables_getter": sink.get_sink_value},
            ),
        ],
        serve_traceback=True,
    )
    web_app.listen(port)

    print("Running in http://localhost:8082/")
    loop.start()


if __name__ == "__main__":
    duckdb_server()
