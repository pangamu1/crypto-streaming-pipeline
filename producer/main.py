import argparse
import asyncio
import json
import logging
import signal

import websockets
from confluent_kafka import Producer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("producer")

WS_URL = "wss://advanced-trade-ws.coinbase.com"
PRODUCT_IDS = ["BTC-USD", "ETH-USD"]
KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC = "trades.raw"


def parse_trades(message: dict) -> list[dict]:
    """Flatten events[*].trades[*] out of one market_trades message."""
    return [t for event in message.get("events", []) for t in event.get("trades", [])]


def on_delivery(err, msg) -> None:
    if err is not None:
        logger.error("delivery failed for key=%s: %s", msg.key(), err)


async def run(duration: float | None) -> None:
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})
    subscribe = {"type": "subscribe", "product_ids": PRODUCT_IDS, "channel": "market_trades"}

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    loop.add_signal_handler(signal.SIGINT, stop.set)   # clean Ctrl-C, no traceback
    loop.add_signal_handler(signal.SIGTERM, stop.set)
    if duration:
        loop.call_later(duration, stop.set)            # bounded-window timer
        logger.info("bounded window: capturing for %.0fs", duration)

    backoff = 1
    try:
        while not stop.is_set():
            try:
                async with websockets.connect(WS_URL) as ws:
                    await ws.send(json.dumps(subscribe))
                    logger.info("subscribed to market_trades for %s", PRODUCT_IDS)
                    backoff = 1
                    async for raw in ws:
                        if stop.is_set():
                            break
                        message = json.loads(raw)
                        if message.get("channel") != "market_trades":
                            continue
                        for t in parse_trades(message):
                            producer.produce(TOPIC, key=t["product_id"],
                                             value=json.dumps(t), on_delivery=on_delivery)
                        producer.poll(0)
            except (websockets.ConnectionClosed, OSError) as e:
                if stop.is_set():
                    break
                logger.warning("connection lost: %s — reconnecting in %ds", e, backoff)
                try:
                    await asyncio.wait_for(stop.wait(), timeout=backoff)
                    break  # stop fired during backoff
                except asyncio.TimeoutError:
                    backoff = min(backoff * 2, 30)
    finally:
        remaining = producer.flush(10)
        logger.info("shutdown complete — flushed producer, %d message(s) still queued", remaining)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Coinbase trades -> Kafka producer")
    parser.add_argument("--duration", type=float, default=0,
                        help="seconds to capture then stop cleanly (0 = until Ctrl-C)")
    args = parser.parse_args()
    asyncio.run(run(args.duration or None))