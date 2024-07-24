import dataclasses
import threading
from typing import Literal, Any, Optional

import perspective
import pyarrow as pa
import pyarrow.json
import tornado
from perspective import PerspectiveManager, PerspectiveTornadoHandler

from beavers import Node
from beavers.kafka import KafkaDriver



COMPARATORS = (
    "==",
    "!=",
    ">",
    ">=",
    "<",
    "<=",
    "begins with",
    "contains",
    "ends with",
    "in",
    "not in",
    "is not null",
    "is null",
)


@dataclasses.dataclass(frozen=True)
class TableConfig:
    """
    Perspective table config, which is passed to the UI
    """

    name: str
    index: str
    columns: list[str]
    sort: list[tuple[str, Literal["asc", "desc"]]]
    filters: list[tuple[str, str, Any]]


@dataclasses.dataclass(frozen=True)
class PerspectiveTableDefinition:
    name: str
    schema: pa.Schema
    index_column: str
    remove_column: str
    sort: list[tuple[str, Literal["asc", "desc"]]] = dataclasses.field(
        default_factory=list
    )
    filters: list[tuple[str, str, Any]] = dataclasses.field(default_factory=list)

    def __post_init__(self):
        assert self.index_column in self.schema.names, self.index_column
        assert self.remove_column in self.schema.names, self.remove_column
        assert isinstance(self.sort, list)
        for column, order in self.sort:
            assert isinstance(column, str)
            assert column in self.schema.names
            assert order in ("asc", "desc")
        for each_filter in self.filters:
            assert len(each_filter) in (2, 3)
            assert isinstance(each_filter[0], str), each_filter
            assert each_filter[1] in COMPARATORS

    def to_table_config(self) -> TableConfig:
        return TableConfig(
            name=self.name,
            index=self.index_column,
            columns=[f for f in self.schema.names if not f.startswith("_")],
            sort=[] if self.sort is None else self.sort,
            filters=self.filters,
        )



class TableHandler(tornado.web.RequestHandler):
    _tables: Optional[dict[str, TableConfig]] = None
    _default_table: Optional[str] = None

    def initialize(self, tables: list[PerspectiveTableDefinition]) -> None:
        self._tables = {table.name: table.to_table_config() for table in tables}
        self._default_table = tables[0].name

    async def get(self, path: str) -> None:
        table_name = path or self._default_table
        table_config = self._tables[table_name]

        await self.render(
            "./table.html",
            table_config=table_config,
            perspective_version=perspective.__version__,
        )


def table_to_bytes(table: pa.Table) -> bytes:
    with pa.BufferOutputStream() as sink:
        with pa.ipc.new_stream(sink, table.schema) as writer:
            for batch in table.to_batches():
                writer.write_batch(batch)
        return sink.getvalue().to_pybytes()



@dataclasses.dataclass(frozen=True)
class UpdateRunner:
    kafka_driver: KafkaDriver
    nodes_and_table: list[tuple[Node, perspective.Table]]

    def __call__(self):
        if self.kafka_driver.run_cycle(0.0):
            for node, table in self.nodes_and_table:
                if node.get_cycle_id() >= self.kafka_driver._dag.get_cycle_id():
                    table.update(table_to_bytes(node.get_value()))



def perspective_thread(
    manager: perspective.PerspectiveManager,
    kafka_driver: KafkaDriver,
    nodes: list[tuple[Node, PerspectiveTableDefinition]],
):
    psp_loop = tornado.ioloop.IOLoop()

    manager.set_loop_callback(psp_loop.add_callback)
    nodes_and_table = []
    for node, table_definition in nodes:
        perspective_table = perspective.Table(
            table_to_bytes(table_definition.schema.empty_table()),
            index=table_definition.index_column,
        )
        nodes_and_table.append((node, perspective_table))
        manager.host_table(table_definition.name, perspective_table)

    callback = tornado.ioloop.PeriodicCallback(
        callback=UpdateRunner(kafka_driver, nodes_and_table), callback_time=1_000
    )
    callback.start()
    psp_loop.start()




def create_web_application(
    node_to_table_definition: list[tuple[Node, PerspectiveTableDefinition]],
    kafka_driver: KafkaDriver,
    assets_directory: str,
) -> tornado.web.Application:
    manager = PerspectiveManager()

    thread = threading.Thread(
        target=perspective_thread,
        args=(manager, kafka_driver, node_to_table_definition),
    )
    thread.daemon = True
    thread.start()

    return tornado.web.Application(
        [
            (
                r"/websocket",
                PerspectiveTornadoHandler,
                {"manager": manager, "check_origin": True},
            ),
            (
                r"/assets/(.*)",
                tornado.web.StaticFileHandler,
                {"path": assets_directory, "default_filename": None},
            ),
            (
                r"/([a-z0-9_]*)",
                TableHandler,
                {"tables": [nt[1] for nt in node_to_table_definition]},
            ),
        ],
        serve_traceback=True,
    )



def run_web_app(web_app: tornado.web.Application, port: int):
    web_app.listen(port)
    loop = tornado.ioloop.IOLoop.current()
    loop.start()