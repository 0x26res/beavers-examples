"""
Listen to market data from coinbase websocket API and publish it to kafka
"""

import asyncio
import json
import logging
import os
import sys

import confluent_kafka
import websockets

logger = logging.getLogger(__name__)


def set_logger():
    logging.basicConfig(
        stream=sys.stdout,
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )


def on_delivery(err: confluent_kafka.KafkaError, msg: confluent_kafka.Message):
    if err is not None:
        logger.error(f"Delivery failed for {msg.topic()}[{msg.key()}]: {err}")
    else:
        logger.debug(f"Delivered to {msg.topic()}[{msg.partition()}] @ {msg.offset()}")


def on_error(err: confluent_kafka.KafkaError):
    logger.error(f"Producer error: {err}")


async def run_web_socket(producer: confluent_kafka.Producer):
    async with websockets.connect(
        "wss://ws-feed.exchange.coinbase.com", ping_interval=None
    ) as ws:
        await ws.send(
            json.dumps({"type": "subscribe", "channels": [{"name": "status"}]})
        )
        subscribed = []

        while True:
            payload = await ws.recv()
            data = json.loads(payload)
            data_type = data.pop("type")

            if data_type == "ticker":
                producer.produce(
                    topic="ticker", value=json.dumps(data), key=data["product_id"]
                )
            elif data_type == "status":
                product_ids = sorted([p["id"] for p in data["products"] if p["id"]])
                if subscribed != product_ids:
                    logger.info(f"Subscribing to {product_ids}")
                    subscribed = product_ids
                    await ws.send(
                        json.dumps(
                            {
                                "type": "subscribe",
                                "product_ids": product_ids,
                                "channels": ["ticker", "heartbeat"],
                            }
                        )
                    )
                else:
                    logger.info("Status unchanged")
                for product in data["products"]:
                    producer.produce(
                        topic="status",
                        value=json.dumps(product),
                        key=product["id"],
                    )
            elif data_type == "subscriptions":
                logger.info(f"Subscriptions: {data}")
            elif data_type == "error":
                logger.error(f"Error {data}")
            elif data_type == "heartbeat":
                logger.debug(f"Heartbeat {data}")
            else:
                logger.error("Unknown data type: {}".format(data_type))
            producer.poll(0.0)


def main():
    producer = confluent_kafka.Producer(
        {
            "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
            "security.protocol": "SSL",
            "ssl.ca.location": os.environ.get("KAFKA_SSL_CA", ".secrets/ca.pem"),
            "ssl.certificate.location": os.environ.get("KAFKA_SSL_CERT", ".secrets/service.cert"),
            "ssl.key.location": os.environ.get("KAFKA_SSL_KEY", ".secrets/service.key"),
        },
        on_delivery=on_delivery,
        error_cb=on_error,
    )
    while True:
        try:
            asyncio.run(run_web_socket(producer))
        except KeyError:
            logger.exception("Stopped by user")
            break
        except websockets.WebSocketException:
            logger.exception("Websocket error, restarting")


if __name__ == "__main__":
    set_logger()
    main()
