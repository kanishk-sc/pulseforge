"""Read durable PostgreSQL operational state for short-lived analytics jobs."""

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import psycopg
from prometheus_client import CollectorRegistry, generate_latest
from prometheus_client.core import GaugeMetricFamily

from pulseforge.config import Settings
from pulseforge.logging import configure_logging


class OperationalCollector:
    def __init__(self, settings: Settings):
        self.settings = settings

    def collect(self):
        settings = self.settings
        with psycopg.connect(
            host=settings.postgres_host,
            port=settings.postgres_port,
            dbname=settings.postgres_db,
            user=settings.postgres_user,
            password=settings.postgres_password.get_secret_value(),
            connect_timeout=2,
            options="-c statement_timeout=2000 -c default_transaction_read_only=on",
        ) as connection:
            builds = GaugeMetricFamily(
                "pulseforge_analytics_builds", "Durable analytics build records", labels=["status"]
            )
            for status, count in connection.execute(
                "SELECT status,count(*) FROM product.analytics_builds GROUP BY status"
            ):
                builds.add_metric([status], count)
            yield builds

            latest = connection.execute(
                """SELECT status, extract(epoch from started_at),
                          extract(epoch from completed_at),
                          extract(epoch from published_at),
                          extract(epoch from source_max_ingested_at), source_event_count,
                          build_id
                   FROM product.analytics_builds ORDER BY started_at DESC, build_id DESC LIMIT 1"""
            ).fetchone()
            if latest:
                status, started, completed, _, _, _, _ = latest
                yield GaugeMetricFamily(
                    "pulseforge_analytics_latest_attempt_failed",
                    "Whether the newest build attempt failed",
                    value=int(status == "failed"),
                )
                if completed is not None:
                    yield GaugeMetricFamily(
                        "pulseforge_analytics_last_duration_seconds",
                        "Last finished build attempt duration",
                        value=max(0, float(completed - started)),
                    )
                statuses = [
                    row[0]
                    for row in connection.execute(
                        """SELECT status FROM product.analytics_builds
                           ORDER BY started_at DESC, build_id DESC LIMIT 100"""
                    )
                ]
                consecutive = next(
                    (index for index, item in enumerate(statuses) if item != "failed"),
                    len(statuses),
                )
                yield GaugeMetricFamily(
                    "pulseforge_analytics_consecutive_failures",
                    "Consecutive latest failed build attempts, capped at 100",
                    value=consecutive,
                )

            publication = connection.execute(
                """SELECT extract(epoch from b.published_at),
                          extract(epoch from b.source_max_ingested_at), b.source_event_count,
                          q.pass_count, q.success_count
                   FROM product.analytics_builds b
                   LEFT JOIN product.dbt_quality_runs q USING (build_id)
                   WHERE b.status='succeeded'
                   ORDER BY b.published_at DESC,b.build_id DESC LIMIT 1"""
            ).fetchone()
            yield GaugeMetricFamily(
                "pulseforge_analytics_publication_present",
                "Whether any successful publication exists",
                value=int(publication is not None),
            )
            if publication:
                published, source_ingested, source_count, passed, succeeded = publication
                yield GaugeMetricFamily(
                    "pulseforge_analytics_last_success_timestamp_seconds",
                    "Latest successful product publication time",
                    value=float(published),
                )
                if source_ingested is not None:
                    yield GaugeMetricFamily(
                        "pulseforge_analytics_source_max_ingested_timestamp_seconds",
                        "Frozen staging source ingestion watermark of latest publication",
                        value=float(source_ingested),
                    )
                yield GaugeMetricFamily(
                    "pulseforge_analytics_source_event_count",
                    "Source event count in the latest successful publication",
                    value=int(source_count),
                )
                if passed is not None and succeeded is not None:
                    quality = GaugeMetricFamily(
                        "pulseforge_analytics_quality_results",
                        "dbt result statuses in the latest published artifact",
                        labels=["status"],
                    )
                    quality.add_metric(["pass"], passed)
                    quality.add_metric(["success"], succeeded)
                    yield quality

            source_count, source_ingested = connection.execute(
                "SELECT count(*),extract(epoch from max(ingested_at)) FROM stream_events"
            ).fetchone()
            yield GaugeMetricFamily(
                "pulseforge_warehouse_committed_rows",
                "Unique committed warehouse event rows",
                value=source_count,
            )
            if source_ingested is not None:
                yield GaugeMetricFamily(
                    "pulseforge_warehouse_source_max_ingested_timestamp_seconds",
                    "Latest warehouse source ingestion timestamp",
                    value=float(source_ingested),
                )
            batch_count, inserted_count = connection.execute(
                "SELECT count(*),coalesce(sum(row_count),0) FROM streaming_batches"
            ).fetchone()
            yield GaugeMetricFamily(
                "pulseforge_warehouse_committed_batches",
                "Unique committed sink batch ledger entries",
                value=batch_count,
            )
            yield GaugeMetricFamily(
                "pulseforge_warehouse_inserted_rows",
                "Ledger sum of committed inserted rows",
                value=inserted_count,
            )

            evaluations = GaugeMetricFamily(
                "pulseforge_detector_evaluations",
                "Durable detector evaluation attempts",
                labels=["status", "skip_reason"],
            )
            for status, reason, count in connection.execute(
                """SELECT status,coalesce(skip_reason,'none'),count(*)
                   FROM product.detector_runs GROUP BY status,skip_reason"""
            ):
                evaluations.add_metric([status, reason], count)
            yield evaluations
            detector = connection.execute(
                """SELECT extract(epoch from max(completed_at)),
                          coalesce(sum(created_incidents),0)
                   FROM product.detector_runs"""
            ).fetchone()
            if detector[0] is not None:
                yield GaugeMetricFamily(
                    "pulseforge_detector_last_evaluation_timestamp_seconds",
                    "Last completed detector evaluation time",
                    value=float(detector[0]),
                )
            yield GaugeMetricFamily(
                "pulseforge_detector_incidents_created",
                "Durable new incidents reported by detector attempts",
                value=detector[1],
            )
            duration = connection.execute(
                """SELECT extract(epoch from completed_at-started_at)
                   FROM product.detector_runs WHERE completed_at IS NOT NULL
                   ORDER BY completed_at DESC LIMIT 1"""
            ).fetchone()
            if duration:
                yield GaugeMetricFamily(
                    "pulseforge_detector_last_duration_seconds",
                    "Last finished detector duration",
                    value=max(0, float(duration[0])),
                )


def create_registry(settings: Settings) -> CollectorRegistry:
    registry = CollectorRegistry(auto_describe=False)
    registry.register(OperationalCollector(settings))
    return registry


def main() -> None:
    configure_logging()
    registry = create_registry(Settings())

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path == "/health":
                body, status = b"ok\n", 200
            elif self.path == "/metrics":
                try:
                    body, status = generate_latest(registry), 200
                except (psycopg.Error, TimeoutError):
                    body, status = b"operational database unavailable\n", 503
            else:
                body, status = b"not found\n", 404
            self.send_response(status)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, _format, *_args):
            pass

    ThreadingHTTPServer(("0.0.0.0", 9108), Handler).serve_forever()


if __name__ == "__main__":
    main()
