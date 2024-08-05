"""
Listen to market data from coinbase websocket API and publish it to kafka
"""

import asyncio
import json
import logging
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


async def run_web_socket(producer: confluent_kafka.Producer):
    async with websockets.connect("wss://ws-feed.exchange.coinbase.com") as ws:
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
            elif data_type == "subscriptions":
                logger.info(f"Subscriptions: {data}")
            elif data_type == "error":
                logger.error(f"Error {data}")
            elif data_type != "heartbeat":
                logger.error("Unknown data type: {}".format(data_type))
            producer.poll(0.0)


def main():
    producer = confluent_kafka.Producer({"bootstrap.servers": "localhost:9092"})
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
