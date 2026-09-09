from datetime import UTC, datetime
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql

from pulseforge.generator import EventGenerator
from pulseforge.streaming.config import StreamSettings
from pulseforge.streaming.validation import validate_payload
from pulseforge.streaming.warehouse import (
    EVENT_COLUMNS,
    commit_stage,
    committed,
    connect,
    initialize_schema,
    prepare_stage,
    stage_name,
)

pytestmark = pytest.mark.integration


def test_schema_and_transactional_sink_replay():
    with connect(StreamSettings()) as connection:
        initialize_schema(connection)
        initialize_schema(connection)
        # Roll back the whole fixture including its test-only stage and metrics updates.
        with connection.transaction():
            query_id = str(uuid4())
            now = datetime.now(UTC)
            event = EventGenerator(seed=uuid4().int).next_event()
            row = validate_payload(event.model_dump_json().encode(), now)["event"]
            row.update(
                source_topic=f"test-{query_id}",
                source_partition=0,
                source_offset=1,
                source_timestamp=now,
                ingested_at=now,
            )
            name = stage_name(query_id, 0)
            prepare_stage(connection, name)
            insert = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
                sql.Identifier(name),
                sql.SQL(",").join(map(sql.Identifier, EVENT_COLUMNS)),
                sql.SQL(",").join(sql.Placeholder() for _ in EVENT_COLUMNS),
            )
            for _ in range(2):
                connection.execute(insert, [row[col] for col in EVENT_COLUMNS])
            assert commit_stage(connection, name, query_id, 0) == 1
            assert committed(connection, query_id, 0) == (1,)
            assert commit_stage(connection, name, query_id, 0) == 1
            prepare_stage(connection, name)
            connection.execute(insert, [row[col] for col in EVENT_COLUMNS])
            assert commit_stage(connection, name, query_id, 1) == 0
            assert (
                connection.execute(
                    "SELECT count(*) FROM stream_events WHERE event_id=%s", (event.event_id,)
                ).fetchone()[0]
                == 1
            )
            # Global totals must equal the unique event table even after both replay modes.
            assert (
                connection.execute("SELECT sum(event_count) FROM stream_metrics_minute").fetchone()[
                    0
                ]
                == connection.execute("SELECT count(*) FROM stream_events").fetchone()[0]
            )
            raise psycopg.Rollback
