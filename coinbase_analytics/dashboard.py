import dataclasses
import io
import threading
from typing import Sequence

import confluent_kafka
import pandas as pd
import perspective
import pyarrow as pa
import pyarrow.json
import tornado
from perspective import PerspectiveManager, PerspectiveTornadoHandler

from beavers import Dag, Node
from beavers.kafka import KafkaDriver, SourceTopic

TICKER_SCHEMA = pa.schema(
    [
        pa.field("sequence", pa.int64()),
        pa.field("product_id", pa.string()),
        pa.field("price", pa.float64()),
        pa.field("open_24h", pa.float64()),
        pa.field("volume_24h", pa.float64()),
        pa.field("low_24h", pa.float64()),
        pa.field("high_24h", pa.float64()),
        pa.field("volume_30d", pa.float64()),
        pa.field("best_bid", pa.float64()),
        pa.field("best_bid_size", pa.float64()),
        pa.field("best_ask", pa.float64()),
        pa.field("best_ask_size", pa.float64()),
        pa.field("side", pa.string()),
        pa.field("time", pa.timestamp("ns", "UTC")),
        pa.field("trade_id", pa.int64()),
        pa.field("last_size", pa.float64()),
    ]
)


@dataclasses.dataclass(frozen=True)
class JsonArrowParser:
    schema: pa.Schema

    def __call__(self, messages: Sequence[confluent_kafka.Message]) -> pa.Table:
        if messages:
            with io.BytesIO() as buffer:
                for message in messages:
                    if message.value():
                        buffer.write(message.value())
                        buffer.write(b"\n")
                buffer.seek(0)
                return pyarrow.json.read_json(buffer).cast(self.schema)
        else:
            return self.schema.empty_table()


class MainHandler(tornado.web.RequestHandler):
    async def get(self, path: str) -> None:
        await self.render(
            "./index.html",
            perspective_version=perspective.__version__,
        )


def table_to_bytes(table: pa.Table) -> bytes:
    with pa.BufferOutputStream() as sink:
        with pa.ipc.new_stream(sink, table.schema) as writer:
            for batch in table.to_batches():
                writer.write_batch(batch)
        return sink.getvalue().to_pybytes()


def perspective_thread(
    manager: perspective.PerspectiveManager,
    table: perspective.Table,
    kafka_driver: KafkaDriver,
    node: Node,
):
    psp_loop = tornado.ioloop.IOLoop()

    manager.set_loop_callback(psp_loop.add_callback)
    manager.host_table("table", table)

    def run_update():
        if (
            kafka_driver.run_cycle(0.0)
            and node.get_cycle_id() >= kafka_driver._dag.get_cycle_id()
        ):
            table.update(table_to_bytes(node.get_value()))

    callback = tornado.ioloop.PeriodicCallback(callback=run_update, callback_time=1000)
    callback.start()
    psp_loop.start()


def create_web_application(
    node: Node,
    schema: pa.Schema,
    index: str,
    kafka_driver: KafkaDriver,
) -> tornado.web.Application:
    manager = PerspectiveManager()

    table = perspective.Table(table_to_bytes(schema.empty_table()), index=index)
    thread = threading.Thread(
        target=perspective_thread, args=(manager, table, kafka_driver, node)
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
            (r"/([a-z0-9_]*)", MainHandler),
        ],
        serve_traceback=True,
    )


def main():
    dag = Dag()
    source = dag.source_stream(empty=TICKER_SCHEMA.empty_table(), name="ticker")
    latest = dag.pa.latest_by_keys(stream=source, keys=["product_id"])

    kafka_driver = KafkaDriver.create(
        dag,
        producer_config={"bootstrap.servers": "localhost:9092"},
        consumer_config={"group.id": "beavers", "bootstrap.servers": "localhost:9092"},
        source_topics={
            "ticker": SourceTopic.from_relative_time(
                "ticker",
                JsonArrowParser(TICKER_SCHEMA),
                relative_time=pd.to_timedelta("5min"),
            )
        },
        sink_topics={},
    )
    web_app = create_web_application(latest, TICKER_SCHEMA, "product_id", kafka_driver)
    web_app.listen(8082)
    loop = tornado.ioloop.IOLoop.current()
    loop.start()


if __name__ == "__main__":
    main()
