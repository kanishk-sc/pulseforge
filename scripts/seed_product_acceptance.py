"""Seed a deterministic payment anomaly through the replay-safe ingestion boundary."""

import argparse
import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import NAMESPACE_URL, uuid5

from psycopg import sql

from pulseforge.events import STATUS_BY_TYPE, CommerceEvent, EventType
from pulseforge.streaming.config import StreamSettings
from pulseforge.streaming.validation import validate_payload
from pulseforge.streaming.warehouse import (
    EVENT_COLUMNS,
    commit_stage,
    connect,
    initialize_schema,
    prepare_stage,
    stage_name,
)


def payment_event(timestamp: datetime, sequence: int, failed: bool) -> CommerceEvent:
    kind = EventType.PAYMENT_FAILED if failed else EventType.PAYMENT_PROCESSED
    identity = f"phase-4-product:{timestamp.isoformat()}:{sequence}:{kind.value}"
    journey = uuid5(NAMESPACE_URL, identity)
    return CommerceEvent.model_validate(
        {
            "event_id": journey,
            "event_type": kind,
            "timestamp": timestamp,
            "customer_id": f"acceptance-customer-{journey.hex[:12]}",
            "order_id": f"acceptance-order-{journey.hex}",
            "product_id": "acceptance-product-payment",
            "amount": Decimal("25.00"),
            "payment_provider": "stripe",
            "region": "ap-south",
            "status": STATUS_BY_TYPE[kind],
            "metadata": {"synthetic": True, "fixture": "phase-4-product-acceptance"},
        }
    )


def scenario(evaluation_start: datetime) -> list[CommerceEvent]:
    events: list[CommerceEvent] = []
    # Six complete, healthy baseline windows followed by an all-failure evaluation window.
    for hours_before in range(6, 0, -1):
        timestamp = evaluation_start - timedelta(hours=hours_before) + timedelta(minutes=5)
        events.extend(payment_event(timestamp, sequence, False) for sequence in range(20))
    anomaly_timestamp = evaluation_start + timedelta(minutes=5)
    events.extend(payment_event(anomaly_timestamp, sequence, True) for sequence in range(20))
    return events


def insert_stage(connection, name: str, events: list[CommerceEvent], topic: str) -> None:
    statement = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
        sql.Identifier(name),
        sql.SQL(",").join(map(sql.Identifier, EVENT_COLUMNS)),
        sql.SQL(",").join(sql.Placeholder() for _ in EVENT_COLUMNS),
    )
    for offset, source_event in enumerate(events):
        ingested_at = datetime.now(UTC)
        row = validate_payload(source_event.model_dump_json().encode(), ingested_at)["event"]
        row.update(
            source_topic=topic,
            source_partition=0,
            source_offset=offset,
            source_timestamp=source_event.timestamp,
            ingested_at=ingested_at,
        )
        connection.execute(statement, [row[column] for column in EVENT_COLUMNS])


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--evaluation-start",
        type=datetime.fromisoformat,
        help="UTC evaluation hour; defaults to the last complete hour",
    )
    args = parser.parse_args()
    evaluation_start = args.evaluation_start or (
        datetime.now(UTC).replace(minute=0, second=0, microsecond=0) - timedelta(hours=1)
    )
    if evaluation_start.tzinfo is None or evaluation_start.utcoffset() is None:
        parser.error("--evaluation-start must include a UTC offset")
    evaluation_start = evaluation_start.astimezone(UTC).replace(minute=0, second=0, microsecond=0)
    detector_now = evaluation_start + timedelta(hours=1, minutes=1)
    events = scenario(evaluation_start)
    suffix = evaluation_start.strftime("%Y%m%d%H")
    query_id = f"phase-4-product-acceptance-{suffix}"
    topic = f"phase4.acceptance.{suffix.lower()}"
    settings = StreamSettings()
    with connect(settings) as connection:
        initialize_schema(connection)
        name = stage_name(query_id, 0)
        prepare_stage(connection, name)
        insert_stage(connection, name, events, topic)
        inserted = commit_stage(connection, name, query_id, 0)
    print(
        json.dumps(
            {
                "detector_now": detector_now.isoformat(),
                "evaluation_start": evaluation_start.isoformat(),
                "events_requested": len(events),
                "events_inserted": inserted,
                "region": "ap-south",
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
