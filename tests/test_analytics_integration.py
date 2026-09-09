import subprocess
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import UUID, uuid4

import pytest
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

pytestmark = [pytest.mark.integration, pytest.mark.analytics]


def event(
    number: int,
    kind: EventType,
    timestamp: datetime,
    journey: int,
    region: str,
    amount: str | None = None,
    **providers: str,
) -> CommerceEvent:
    return CommerceEvent.model_validate(
        {
            "event_id": UUID(int=number),
            "event_type": kind,
            "timestamp": timestamp,
            "customer_id": f"customer-{journey}",
            "order_id": f"order-{journey}",
            "product_id": f"product-{journey}",
            "region": region,
            "amount": amount,
            "status": STATUS_BY_TYPE[kind],
            "metadata": {"synthetic": True, "fixture": "phase-3-acceptance"},
            **providers,
        }
    )


def deterministic_events() -> list[CommerceEvent]:
    hour_10 = datetime(2026, 9, 8, 10, tzinfo=UTC)
    hour_11 = hour_10 + timedelta(hours=1)
    return [
        event(1, EventType.ORDER_CREATED, hour_10, 1, "us-east", "100.00"),
        event(
            2,
            EventType.PAYMENT_PROCESSED,
            hour_10 + timedelta(minutes=5),
            1,
            "us-east",
            "100.00",
            payment_provider="stripe",
        ),
        event(
            3,
            EventType.SHIPMENT_CREATED,
            hour_10 + timedelta(minutes=10),
            1,
            "us-east",
            shipment_provider="ups",
        ),
        event(
            4,
            EventType.SHIPMENT_DELAYED,
            hour_10 + timedelta(minutes=20),
            1,
            "us-east",
            shipment_provider="ups",
        ),
        event(
            5,
            EventType.REFUND_REQUESTED,
            hour_10 + timedelta(minutes=30),
            1,
            "us-east",
            "30.00",
        ),
        event(6, EventType.ORDER_CREATED, hour_10 + timedelta(minutes=2), 2, "us-east", "50.00"),
        event(
            7,
            EventType.PAYMENT_FAILED,
            hour_10 + timedelta(minutes=7),
            2,
            "us-east",
            "50.00",
            payment_provider="adyen",
        ),
        event(8, EventType.ORDER_CREATED, hour_10 + timedelta(minutes=3), 3, "us-west", "200.00"),
        event(
            9,
            EventType.PAYMENT_PROCESSED,
            hour_10 + timedelta(minutes=8),
            3,
            "us-west",
            "200.00",
            payment_provider="paypal",
        ),
        event(
            10,
            EventType.SHIPMENT_CREATED,
            hour_10 + timedelta(minutes=12),
            3,
            "us-west",
            shipment_provider="fedex",
        ),
        event(11, EventType.ORDER_CREATED, hour_11, 4, "us-east", "80.00"),
        event(
            12,
            EventType.PAYMENT_PROCESSED,
            hour_11 + timedelta(minutes=5),
            4,
            "us-east",
            "80.00",
            payment_provider="stripe",
        ),
    ]


def insert_stage_rows(connection, name: str, events: list[CommerceEvent]) -> None:
    statement = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
        sql.Identifier(name),
        sql.SQL(",").join(map(sql.Identifier, EVENT_COLUMNS)),
        sql.SQL(",").join(sql.Placeholder() for _ in EVENT_COLUMNS),
    )
    for offset, source_event in enumerate(events):
        ingested_at = source_event.timestamp + timedelta(minutes=1)
        row = validate_payload(source_event.model_dump_json().encode(), ingested_at)["event"]
        row.update(
            source_topic="analytics.acceptance.v1",
            source_partition=0,
            source_offset=offset,
            source_timestamp=source_event.timestamp,
            ingested_at=ingested_at,
        )
        connection.execute(statement, [row[column] for column in EVENT_COLUMNS])


def run_dbt(database: str, schema: str) -> str:
    result = subprocess.run(
        [
            "docker",
            "compose",
            "--profile",
            "analytics",
            "run",
            "--rm",
            "-e",
            f"POSTGRES_DB={database}",
            "-e",
            f"DBT_SCHEMA={schema}",
            "analytics-dbt",
            "build",
        ],
        capture_output=True,
        text=True,
        timeout=180,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    return result.stdout


@pytest.fixture(scope="module")
def analytics_database():
    database = f"pulseforge_analytics_test_{uuid4().hex[:8]}"
    schema = f"phase3_{uuid4().hex[:8]}"
    admin_settings = StreamSettings()
    with connect(admin_settings) as admin:
        admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(database)))
    settings = StreamSettings(postgres_db=database)
    try:
        events = deterministic_events()
        with connect(settings) as connection:
            initialize_schema(connection)
            name = stage_name("analytics-acceptance", 0)
            prepare_stage(connection, name)
            insert_stage_rows(connection, name, events)
            assert commit_stage(connection, name, "analytics-acceptance", 0) == len(events)

            replay_name = stage_name("analytics-acceptance", 1)
            prepare_stage(connection, replay_name)
            insert_stage_rows(connection, replay_name, events[:1])
            assert commit_stage(connection, replay_name, "analytics-acceptance", 1) == 0
        output = run_dbt(database, schema)
        assert "Completed successfully" in output
        yield settings, schema
    finally:
        with connect(admin_settings) as admin:
            admin.execute(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                "WHERE datname=%s AND pid<>pg_backend_pid()",
                (database,),
            )
            admin.execute(sql.SQL("DROP DATABASE IF EXISTS {}").format(sql.Identifier(database)))


def fetchall(settings: StreamSettings, query: str):
    with connect(settings) as connection:
        return connection.execute(query).fetchall()


def test_exact_hourly_business_metrics(analytics_database):
    settings, schema = analytics_database
    marts = f"{schema}_marts"
    revenue = fetchall(
        settings,
        f"SELECT revenue_hour_utc,region_code,successful_payment_count,revenue_amount "
        f'FROM "{marts}".mart_revenue_hourly ORDER BY 1,2',
    )
    assert revenue == [
        (datetime(2026, 9, 8, 10, tzinfo=UTC), "us-east", 1, Decimal("100.00")),
        (datetime(2026, 9, 8, 10, tzinfo=UTC), "us-west", 1, Decimal("200.00")),
        (datetime(2026, 9, 8, 11, tzinfo=UTC), "us-east", 1, Decimal("80.00")),
    ]
    payments = fetchall(
        settings,
        f"SELECT payment_attempt_count,successful_payment_count,failed_payment_count,"
        f'failure_rate,attempted_amount FROM "{marts}".mart_payment_health_hourly '
        "WHERE payment_hour_utc='2026-09-08 10:00:00+00' AND region_code='us-east'",
    )
    assert payments == [(2, 1, 1, Decimal("0.500000"), Decimal("150.00"))]
    refunds = fetchall(
        settings,
        f'SELECT refund_request_count,requested_amount FROM "{marts}".mart_refunds_hourly',
    )
    assert refunds == [(1, Decimal("30.00"))]


def test_fact_grains_dimensions_and_shipment_semantics(analytics_database):
    settings, schema = analytics_database
    core = f"{schema}_core"
    assert fetchall(
        settings,
        f'SELECT (SELECT count(*) FROM "{core}".fct_orders),'
        f'(SELECT count(*) FROM "{core}".fct_payment_attempts),'
        f'(SELECT count(*) FROM "{core}".fct_shipments),'
        f'(SELECT count(*) FROM "{core}".fct_refund_requests)',
    ) == [(4, 4, 2, 1)]
    assert fetchall(
        settings,
        f"SELECT order_id,delay_event_count,was_delayed,array_length(delay_source_event_ids,1) "
        f'FROM "{core}".fct_shipments ORDER BY order_id',
    ) == [("order-1", 1, True, 1), ("order-3", 0, False, None)]
    assert fetchall(
        settings,
        f'SELECT (SELECT count(*) FROM "{core}".dim_customers),'
        f'(SELECT count(*) FROM "{core}".dim_products),'
        f'(SELECT count(*) FROM "{core}".dim_regions)',
    ) == [(4, 4, 4)]


def test_replay_and_repeated_build_do_not_inflate_analytics(analytics_database):
    settings, schema = analytics_database
    core = f"{schema}_core"
    marts = f"{schema}_marts"
    before = fetchall(
        settings,
        f"SELECT (SELECT count(*) FROM public.stream_events),"
        f'(SELECT count(*) FROM "{core}".fct_orders),'
        f'(SELECT sum(revenue_amount) FROM "{marts}".mart_revenue_hourly)',
    )
    output = run_dbt(settings.postgres_db, schema)
    assert "Completed successfully" in output
    after = fetchall(
        settings,
        f"SELECT (SELECT count(*) FROM public.stream_events),"
        f'(SELECT count(*) FROM "{core}".fct_orders),'
        f'(SELECT sum(revenue_amount) FROM "{marts}".mart_revenue_hourly)',
    )
    assert before == after == [(12, 4, Decimal("380.00"))]
