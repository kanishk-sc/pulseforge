"""Listen to every committed progress event; polling lastProgress can miss fast batches."""

import json
import logging

from pyspark.sql.streaming import StreamingQueryListener

logger = logging.getLogger(__name__)


class ProgressLogger(StreamingQueryListener):
    def onQueryStarted(self, event) -> None:
        logger.info("query_started query=%s id=%s", event.name, event.id)

    def onQueryProgress(self, event) -> None:
        progress = json.loads(event.progress.json)
        dropped = sum(
            op.get("numRowsDroppedByWatermark", 0) for op in progress.get("stateOperators", [])
        )
        logger.log(
            logging.WARNING if dropped else logging.INFO,
            "query_progress query=%s batch=%s input=%s watermark_dropped=%s",
            progress["name"],
            progress["batchId"],
            progress["numInputRows"],
            dropped,
        )

    def onQueryTerminated(self, event) -> None:
        logger.log(
            logging.ERROR if event.exception else logging.INFO,
            "query_terminated id=%s failed=%s",
            event.id,
            bool(event.exception),
        )

    def onQueryIdle(self, event) -> None:
        pass
