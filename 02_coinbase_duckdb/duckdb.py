import dataclasses
import datetime
import pathlib

import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
from beavers import Dag


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
STATUS_SCHEMA = pa.schema(
    [
        pa.field("id", pa.string()),
        pa.field("base_currency", pa.string()),
        pa.field("quote_currency", pa.string()),
        pa.field("base_increment", pa.float64()),
        pa.field("quote_increment", pa.float64()),
        pa.field("display_name", pa.string()),
        pa.field("status", pa.string()),
        pa.field("status_message", pa.string()),
        pa.field("min_market_funds", pa.float64()),
        pa.field("post_only", pa.boolean()),
        pa.field("post_only", pa.boolean()),
        pa.field("limit_only", pa.boolean()),
        pa.field("cancel_only", pa.boolean()),
        pa.field("fx_stablecoin", pa.boolean()),
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
    ticker_source = dag.pa.source_table(schema=TICKER_SCHEMA, name="ticker")
    ticker_state = dag.pa.latest_by_keys(ticker_source, keys=["product_id"])

    status_source = dag.pa.source_table(schema=STATUS_SCHEMA, name="status")
    status_state = dag.pa.latest_by_keys(status_source, keys=["id"])

    tables = dag.state(dict).map(ticker_state, status_state)

    print("Running in http://localhost:8082/ticker")


if __name__ == "__main__":
    dashboard()
