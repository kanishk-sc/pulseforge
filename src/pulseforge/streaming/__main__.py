import json
import logging
import signal
import threading
from pathlib import Path

from pulseforge.logging import configure_logging
from pulseforge.streaming.config import StreamSettings
from pulseforge.streaming.progress import ProgressLogger
from pulseforge.streaming.queries import start_queries
from pulseforge.streaming.runtime import spark_session
from pulseforge.streaming.telemetry import start_metrics_server
from pulseforge.streaming.warehouse import connect, initialize_schema

logger = logging.getLogger(__name__)
HEALTH_FILE = Path("/tmp/pulseforge-streaming-health.json")


def main() -> None:
    configure_logging()
    settings = StreamSettings()
    stop = threading.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop.set())
    HEALTH_FILE.unlink(missing_ok=True)
    with connect(settings) as connection:
        initialize_schema(connection)
    try:
        start_metrics_server()
    except Exception as exc:
        logger.warning("stream_metrics_unavailable", extra={"error_type": type(exc).__name__})
    spark = spark_session(settings)
    queries = []
    spark.streams.addListener(ProgressLogger())
    try:
        queries = start_queries(spark, settings)
        while not stop.wait(1):
            for query in queries:
                if not query.isActive:
                    raise RuntimeError(f"Critical streaming query terminated: {query.name}")
            HEALTH_FILE.write_text(json.dumps({"queries": [q.name for q in queries]}))
    finally:
        HEALTH_FILE.unlink(missing_ok=True)
        for query in queries:
            query.stop()
        spark.stop()


if __name__ == "__main__":
    main()
