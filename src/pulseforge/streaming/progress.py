"""Listen to every committed progress event; polling lastProgress can miss fast batches."""

import json
import logging
import time

from pyspark.sql.streaming import StreamingQueryListener

from pulseforge.streaming.telemetry import (
    BATCH_DURATION,
    INPUT_ROWS,
    LAST_PROGRESS,
    PROCESSED_RATE,
    QUERY_FAILURES,
    QUERY_NAMES,
    SOURCE_OFFSET,
    WATERMARK_DROPS,
)

logger = logging.getLogger(__name__)


class ProgressLogger(StreamingQueryListener):
    def __init__(self) -> None:
        super().__init__()
        self._names: dict[str, str] = {}

    def onQueryStarted(self, event) -> None:
        self._names[str(event.id)] = event.name
        logger.info("query_started query=%s id=%s", event.name, event.id)

    def onQueryProgress(self, event) -> None:
        try:
            self._observe_progress(event)
        except Exception as exc:
            logger.warning(
                "stream_progress_telemetry_failed", extra={"error_type": type(exc).__name__}
            )

    def _observe_progress(self, event) -> None:
        progress = json.loads(event.progress.json)
        query = progress.get("name")
        if query not in QUERY_NAMES:
            return
        dropped = sum(
            op.get("numRowsDroppedByWatermark", 0) for op in progress.get("stateOperators", [])
        )
        LAST_PROGRESS.labels(query).set(time.time())
        INPUT_ROWS.labels(query).inc(max(0, progress.get("numInputRows", 0)))
        PROCESSED_RATE.labels(query).set(max(0, progress.get("processedRowsPerSecond") or 0))
        duration_ms = progress.get("durationMs", {}).get("triggerExecution")
        if duration_ms is not None:
            BATCH_DURATION.labels(query).observe(max(0, duration_ms / 1000))
        WATERMARK_DROPS.labels(query).inc(max(0, dropped))
        if query == "raw":
            for source in progress.get("sources", []):
                try:
                    offsets = json.loads(source.get("endOffset") or "{}")
                except (TypeError, ValueError):
                    continue
                for partition, offset in offsets.get("commerce.events.v1", {}).items():
                    if str(partition) in {"0", "1", "2"}:
                        SOURCE_OFFSET.labels(str(partition)).set(offset)
        logger.log(
            logging.WARNING if dropped else logging.INFO,
            "query_progress query=%s batch=%s input=%s watermark_dropped=%s",
            query,
            progress["batchId"],
            progress["numInputRows"],
            dropped,
        )

    def onQueryTerminated(self, event) -> None:
        query = self._names.pop(str(event.id), "other")
        if event.exception:
            # Query IDs are deliberately excluded from metric labels.
            try:
                QUERY_FAILURES.labels(query).inc()
            except Exception:
                logger.warning("stream_termination_telemetry_failed")
        logger.log(
            logging.ERROR if event.exception else logging.INFO,
            "query_terminated id=%s failed=%s",
            event.id,
            bool(event.exception),
        )

    def onQueryIdle(self, event) -> None:
        pass
