import dataclasses
import datetime
import pathlib
import sys

import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
from beavers import Dag
from beavers.kafka import KafkaDriver, SourceTopic

from util.json_util import JsonArrowParser
from util.perpective_util import (
    create_web_application,
    PerspectiveTableDefinition,
    run_web_app,
)

TICKER_SCHEMA = pa.schema(
    [
        pa.field("sequence", pa.int64()),
        pa.field("product_id", pa.string()),
        # Historic info:
        pa.field("open_24h", pa.float64()),
        pa.field("low_24h", pa.float64()),
        pa.field("high_24h", pa.float64()),
        pa.field("volume_24h", pa.float64()),
        pa.field("volume_30d", pa.float64()),
        # Bid/Off info:
        pa.field("best_bid", pa.float64()),
        pa.field("best_bid_size", pa.float64()),
        pa.field("best_ask", pa.float64()),
        pa.field("best_ask_size", pa.float64()),
        # Last trade info:
        pa.field("side", pa.string()),
        pa.field("price", pa.float64()),
        pa.field("time", pa.timestamp("ns", "UTC")),
        pa.field("trade_id", pa.int64()),
        pa.field("last_size", pa.float64()),
    ]
)

ASSETS = str(pathlib.Path(__file__).parent / "assets")


def add_spread(table: pa.Table) -> pa.Table:
    return table.append_column(
        "spread", pc.subtract(table["best_ask"], table["best_bid"])
    )


@dataclasses.dataclass()
class TickerHistory:
    state: pa.Table = dataclasses.field(default_factory=TICKER_SCHEMA.empty_table)
    window: datetime.timedelta = datetime.timedelta(minutes=10)

    def __call__(self, ticker: pa.Table, now: pd.Timestamp) -> pa.Table:
        self.state = (
            pa.concat_tables([self.state, ticker])
            .filter(pc.field("time") > (now - self.window))
            .sort_by("time")
        )
        return self.state


def add_5min_change(ticker: pa.Table, history: pa.Table) -> pa.Table:
    ticker = (
        ticker.append_column(
            "time_minus_5min", pc.subtract(ticker["time"], pd.to_timedelta("5min"))
        )
        .sort_by("time_minus_5min")
        .join_asof(
            history.select(["time", "product_id", "price"]).rename_columns(
                ["time", "product_id", "price_5min_before"]
            ),
            on="time_minus_5min",
            right_on="time",
            tolerance=sys.maxsize,
            by=["product_id"],
        )
    )
    return ticker.append_column(
        "5min_change",
        pc.multiply(
            pc.divide(
                pc.subtract(ticker["price_5min_before"], ticker["price"]),
                ticker["price_5min_before"],
            ),
            100.0,
        ),
    ).select(TICKER_SCHEMA.names + ["5min_change"])


def dashboard():
    dag = Dag()
    ticker = dag.pa.source_table(schema=TICKER_SCHEMA, name="ticker")

    # Simple, stateless transformation:
    latest_with_spread = dag.stream(add_spread).map(ticker)

    # Keep track of last 10 minutes
    ticker_history = dag.state(TickerHistory()).map(ticker, dag.now())
    with_change = dag.pa.table_stream(
        add_5min_change, TICKER_SCHEMA.append(pa.field("5min_change", pa.float64()))
    ).map(ticker, ticker_history)

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
                ticker,
                PerspectiveTableDefinition(
                    name="ticker",
                    schema=TICKER_SCHEMA,
                    index_column="product_id",
                    remove_column="product_id",
                    hidden_columns=("sequence", "trade_id"),
                ),
            ),
            (
                latest_with_spread,
                PerspectiveTableDefinition(
                    name="latest_with_spread",
                    schema=TICKER_SCHEMA.append(pa.field("spread", pa.float64())),
                    index_column="product_id",
                    remove_column="product_id",
                    hidden_columns=("sequence", "trade_id"),
                ),
            ),
            (
                with_change,
                PerspectiveTableDefinition(
                    name="with_change",
                    schema=TICKER_SCHEMA.append(pa.field("5min_change", pa.float64())),
                    index_column="product_id",
                    remove_column="product_id",
                    hidden_columns=("sequence", "trade_id"),
                ),
            ),
        ],
        kafka_driver,
        ASSETS,
    )
    print("Running in http://localhost:8082/ticker")
    run_web_app(web_app, 8082)


if __name__ == "__main__":
    dashboard()
