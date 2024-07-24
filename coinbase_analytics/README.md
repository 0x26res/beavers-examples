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
docker run -p 9092:9092 -d bashj79/kafka-kraft
```

Once started you can create the topic: 
```shell
kafka-topics --create --topic ticker --partitions=1 --bootstrap-server=localhost:9092
```

### Publish Coinbase data on kafka

Run the `websocket_feed.py` to publish data to kafka:
```shell
python ./websocket_feed.py
```

### Run the Beavers application:

```shell
python ./dashboard.py
```

And go to http://localhost:8082 to see it

### Debugging the kafka data

To listen to the data coming from kafka:
```shell
kafka-console-consumer --topic ticker --bootstrap-server=localhost:9092 | jq
```


[1]: https://github.com/tradewelltech/beavers
[2]: https://kafka.apache.org/
[3]: https://arrow.apache.org/
[4]: https://github.com/finos/perspective
[5]: https://docs.cdp.coinbase.com/exchange/docs/websocket-overview/
[6]: https://github.com/bashj79/kafka-kraft-docker
