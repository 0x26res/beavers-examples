import pathlib

import pandas as pd
import pyarrow as pa
from beavers import Dag
from beavers.kafka import KafkaDriver, SourceTopic

from coinbase_analytics.json_util import JsonArrowParser
from coinbase_analytics.perpective_util import (
    create_web_application,
    PerspectiveTableDefinition,
    run_web_app,
)

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

ASSETS = str(pathlib.Path(__file__).parent / "assets")


def dashboard():
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
                JsonArrowParser.create(TICKER_SCHEMA),
                relative_time=pd.to_timedelta("1h"),
            )
        },
        sink_topics={},
    )
    web_app = create_web_application(
        [
            (
                latest,
                PerspectiveTableDefinition(
                    name="latest",
                    schema=TICKER_SCHEMA,
                    index_column="product_id",
                    remove_column="product_id",
                ),
            )
        ],
        kafka_driver,
        ASSETS,
    )
    print("Running in http://localhost:8082")
    run_web_app(web_app, 8082)


if __name__ == "__main__":
    dashboard()
