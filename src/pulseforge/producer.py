import argparse
import asyncio
import logging
import signal

from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaConnectionError

from pulseforge.config import Settings
from pulseforge.generator import SCENARIOS, EventGenerator
from pulseforge.logging import configure_logging

logger = logging.getLogger(__name__)


async def run(count: int, scenario: str, stop: asyncio.Event | None = None) -> int:
    settings = Settings()
    stop = stop or asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:  # Windows asyncio.run handles Ctrl+C cancellation.
            pass
    producer = AIOKafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        enable_idempotence=True,
        acks="all",
        client_id="pulseforge-generator",
        request_timeout_ms=10000,
    )
    generator = EventGenerator(settings.generator_seed, scenario, settings.anomaly_rate)
    sent = 0
    try:
        for attempt in range(5):
            try:
                await asyncio.wait_for(producer.start(), timeout=15)
                break
            except (TimeoutError, OSError, KafkaConnectionError):
                if attempt == 4:
                    raise
                logger.warning("kafka_start_retry")
                await asyncio.sleep(min(2**attempt, 8))
        while not stop.is_set() and (count == 0 or sent < count):
            started = loop.time()
            key, value = generator.next_record()
            # aiokafka handles retriable broker errors and idempotent sequencing. A timeout is
            # ambiguous: fail rather than retrying with a new producer and silently duplicating.
            await asyncio.wait_for(
                producer.send_and_wait(settings.kafka_topic, value=value, key=key), timeout=30
            )
            sent += 1
            if sent == 1 or sent % 100 == 0:
                logger.info("events_delivered count=%s", sent)
            remaining = max(0, 1 / settings.events_per_second - (loop.time() - started))
            try:
                await asyncio.wait_for(stop.wait(), timeout=remaining)
            except TimeoutError:
                pass
    finally:
        await asyncio.wait_for(producer.stop(), timeout=15)
        logger.info("producer_stopped delivered=%s", sent)
    return sent


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate synthetic commerce events into Kafka")
    parser.add_argument("--count", type=int, default=0, help="0 streams until interrupted")
    parser.add_argument("--scenario", choices=SCENARIOS, default="normal")
    args = parser.parse_args()
    if args.count < 0:
        parser.error("count must be nonnegative")
    configure_logging()
    try:
        asyncio.run(run(args.count, args.scenario))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
