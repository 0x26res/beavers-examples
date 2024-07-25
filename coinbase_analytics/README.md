# Coinbase Market Data API Analytics


## TLDR:

This tutorial uses a mix of [beavers][1], [kafka][2], [arrow][3] and [perspective][4] to display market data coming from [Coinbase API][5]


## Set Up

### Python Virtual Environment

```shell
python3.11 -m venv --clear .venv
source ./.venv/bin/activate
pip install -r requirements.txt
```

### Kafka

You'll need a kafka cluster. 
The simplest way is to use [kafka-kraft][6] docker image

```shell
docker run --name=simple_kafka -p 9092:9092 -d bashj79/kafka-kraft
```

Once started you can create the topic: 
```shell
docker exec simple_kafka /opt/kafka/bin/kafka-topics.sh \
  --create \
  --topic=ticker \
  --partitions=1 \
  --bootstrap-server=localhost:9092 \
  --replication-factor=1
```

### Publish Coinbase data on kafka

Run the `websocket_feed.py` to publish data to kafka:
```shell
python ./websocket_feed.py
```

You should now be able to see the data in kafka:
```shell
docker exec simple_kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --topic=ticker \
  --bootstrap-server=localhost:9092
```


### Run the Beavers application:

```shell
python ./dashboard.py
```

And go to http://localhost:8082 to see it

## Introducing Beavers

In order to build our dashboard, we'll use Beavers.
Beavers is a streaming python library optimized for analytics.

At its core, Beavers uses a ~~dam~~ DAG to process incoming data.
Each node in the DAG is a Python function.

```python
dag = Dag()
```

The first node in the dashboard DAG is a source node,  called `ticker`.
Its output is a `pyarrow.Table` for which we need to specify the schema.
```python
ticker = dag.pa.source_table(schema=TICKER_SCHEMA, name="ticker")
```

This is what is displayed in the dashboard.

## Simple Transformation in Beavers

Next, we want to add a derived column to `ticker`. 
The new columns, `spread`, is the difference between the `best_ask` and `best_bid`.

For this we just introduce a function:
```python
def add_spread(table: pa.Table) -> pa.Table:
    return table.append_column(
        "spread", pc.subtract(table["best_ask"], table["best_bid"])
    )
```

And add the function to the DAG:
```python
latest_with_spread = dag.state(add_spread).map(ticker)
```

You can see it in: http://localhost:8082/latest_with_spread

## Advanced Transformation with Beavers

Now let's introduce a more advanced computation. 
For each incoming `ticker` record, we would like to calculate the change in the last 5 minutes

For this, first we need to keep track of the history over the last 10 minutes (we do need a bit more than 5 minutes).
```python
ticker_history = dag.state(TickerHistory()).map(ticker, dag.now())
```
And then do an as of join, to find the price 5 minutes ago and calculate the change
```python
with_change = dag.pa.table_stream(
    add_5min_change, TICKER_SCHEMA.append(pa.field("5min_change", pa.float64()))
).map(ticker, ticker_history)
```

You can see it in: http://localhost:8082/latest_with_spread

[1]: https://github.com/tradewelltech/beavers
[2]: https://kafka.apache.org/
[3]: https://arrow.apache.org/
[4]: https://github.com/finos/perspective
[5]: https://docs.cdp.coinbase.com/exchange/docs/websocket-overview/
[6]: https://github.com/bashj79/kafka-kraft-docker
