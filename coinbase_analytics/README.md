# Coinbase Market Data API Analytics


## TLDR:

This tutorial uses a mix of beavers, kafka, polars and perspective to display market data coming from Coinbase


## Set Up

### Python Virtual Environment

```shell
python3.11 -m venv --clear .venv
source ./.venv/bin/activate
pip install -r requirements.txt
```

### Kafka

You'll need a kafka cluster. 
The simplest way is to use [this docker image](https://github.com/bashj79/kafka-kraft-docker)

```shell
docker run -p 9092:9092 -d bashj79/kafka-kraft
```

Once started you can create the topic: 
```shell
kafka-topics --create --topic ticker --partitions=1 --bootstrap-server=localhost:9092
```

And listen to it:
```shell
kafka-console-consumer --topic ticker --bootstrap-server=localhost:9092
```
### Publish Coinbase data on kafka

Run the `websocket_feed.py` to publish data to kafka:
```angular2html
python ./websocket_feed.py
```