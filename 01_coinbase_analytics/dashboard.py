import dataclasses
import datetime
import pathlib

from beavers.perspective_wrapper import (
    PerspectiveTableDefinition,
    create_web_application,
    run_web_application,
)
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
from beavers import Dag
from beavers.kafka import KafkaDriver, SourceTopic

from util.json_util import JsonArrowParser

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
TICKER_WITH_SPREAD_SCHEMA = TICKER_SCHEMA.append(pa.field("spread", pa.float64()))
TICKER_WITH_AVERAGE_SCHEMA = TICKER_SCHEMA.append(
    pa.field("average_price", pa.float64())
)


ASSETS = str(pathlib.Path(__file__).parent / "assets")


def add_spread(table: pa.Table) -> pa.Table:
    return table.append_column(
        "spread", pc.subtract(table["best_ask"], table["best_bid"])
    )


@dataclasses.dataclass()
class TickerHistory:
    window: datetime.timedelta = datetime.timedelta(minutes=10)
    state: pa.Table = dataclasses.field(default_factory=TICKER_SCHEMA.empty_table)

    def __call__(self, ticker: pa.Table, now: pd.Timestamp) -> pa.Table:
        self.state = (
            pa.concat_tables([self.state, ticker])
            .filter(pc.field("time") > (now - self.window))
            .sort_by("time")
        )
        return self.state


@dataclasses.dataclass()
class WithAverageCalculator:
    window: datetime.timedelta = datetime.timedelta(minutes=5)
    state: pa.Table = dataclasses.field(default_factory=TICKER_SCHEMA.empty_table)

    def __call__(self, ticker: pa.Table, now: pd.Timestamp) -> pa.Table:
        self.state = (
            pa.concat_tables([self.state, ticker])
            .filter(
                pc.field("time")
                > pa.scalar((now - self.window), pa.timestamp("us", "UTC"))
            )
            .sort_by("time")
        )
        average = (
            self.state.filter(
                pc.is_in(self.state["product_id"], ticker["product_id"].unique())
            )
            .group_by("product_id")
            .aggregate([("price", "mean")])
            .rename_columns(["product_id", "average_price"])
        )
        return ticker.join(average, keys="product_id")


def add_average_price(ticker: pa.Table, average_price: pa.Table) -> pa.Table:
    return ticker.join(average_price, keys="product_id")


def dashboard():
    dag = Dag()
    ticker = dag.pa.source_table(schema=TICKER_SCHEMA, name="ticker")
    dag.psp.to_perspective(
        ticker,
        PerspectiveTableDefinition(
            name="ticker",
            index_column="product_id",
            hidden_columns=("sequence", "trade_id"),
        ),
    )

    # Simple, stateless transformation:
    ticker_with_spread = dag.pa.table_stream(
        add_spread, schema=TICKER_WITH_SPREAD_SCHEMA
    ).map(ticker)
    dag.psp.to_perspective(
        ticker_with_spread,
        PerspectiveTableDefinition(
            name="ticker_with_spread",
            index_column="product_id",
            hidden_columns=("sequence", "trade_id"),
        ),
    )

    # Keep track of last 10 minutes
    ticker_with_average = dag.pa.table_stream(
        WithAverageCalculator(), TICKER_WITH_AVERAGE_SCHEMA
    ).map(ticker, dag.now())
    dag.psp.to_perspective(
        ticker_with_average,
        PerspectiveTableDefinition(
            name="ticker_with_average",
            index_column="product_id",
            hidden_columns=("sequence", "trade_id"),
        ),
    )

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
    print("Running in http://localhost:8082/ticker")
    run_web_application(create_web_application(kafka_driver), 8082)


if __name__ == "__main__":
    dashboard()
