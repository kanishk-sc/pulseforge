import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

import httpx
import psycopg
import pytest
from psycopg import sql

from pulseforge.assistant.corpus import (
    DIMENSIONS,
    IncompatibleIndex,
    LocalEmbedder,
    apply_migrations,
    chunk_markdown,
    digest,
    ingest,
    load_corpus,
    retrieve,
)
from pulseforge.assistant.models import Explanation, validate_citations
from pulseforge.assistant.service import _dsn, explain_offline
from pulseforge.config import Settings

pytestmark = [pytest.mark.integration, pytest.mark.assistant]


def test_populated_pgvector_retrieval_and_original_build_explanation():
    settings = Settings()
    embedder = LocalEmbedder(Path(settings.assistant_model_cache_dir))
    with psycopg.connect(_dsn(settings)) as connection:
        apply_migrations(connection)
        docs = load_corpus(Path(__file__).resolve().parents[1])
        first = ingest(connection, docs, embedder)
        second = ingest(connection, docs, embedder)
        assert second["updated_documents"] == 0
        assert first["corpus_sha256"] == second["corpus_sha256"]
        mode, sections = retrieve(
            connection, "payment failure rate incident triage", embedder=embedder, top_k=4
        )
        assert mode == "semantic"
        assert sections
        assert any(section["document_id"] == "product-design" for section in sections)
        assert all(section["chunk_id"] for section in sections)
        incident = connection.execute(
            "SELECT incident_id, analytics_build_id FROM product.incidents "
            "WHERE detector_name='payment_failure_rate_increase' "
            "ORDER BY detected_at DESC LIMIT 1"
        ).fetchone()
        assert incident is not None
    explanation = validate_citations(explain_offline(settings, incident[0]))
    assert explanation.analytics_build_id == incident[1]
    assert explanation.retrieval_mode == "semantic"
    assert explanation.mode == "offline"
    assert explanation.hypotheses == []
    assert any("Observed" in item.text and "baseline" in item.text for item in explanation.facts)
    assert any(citation.kind == "event" for citation in explanation.citations)
    assert explanation.current_build_id is not None
    response = httpx.post(
        f"http://127.0.0.1:8000/api/v1/incidents/{incident[0]}/explanation",
        json={"mode": "offline"},
        timeout=15,
    )
    assert response.status_code == 200
    remote = Explanation.model_validate(response.json())
    assert remote.analytics_build_id == explanation.analytics_build_id
    assert remote.label == "Offline evidence summary — no generative model used."


def test_changed_and_deleted_documents_are_reconciled_without_duplicates():
    settings = Settings()
    docs = load_corpus(Path(__file__).resolve().parents[1])
    embedder = LocalEmbedder(Path(settings.assistant_model_cache_dir))
    changed_text = "# Local operations runbooks\n\n## Bounded triage\n\nInspect the original build."
    changed = replace(
        docs[0],
        content_sha256=digest(changed_text),
        chunks=chunk_markdown(docs[0].document_id, changed_text),
    )
    with psycopg.connect(_dsn(settings)) as connection:
        try:
            ingest(connection, (changed, docs[1]), embedder)
            rows = connection.execute(
                "SELECT document_id, count(*) FROM assistant.chunks GROUP BY document_id"
            ).fetchall()
            assert dict(rows) == {
                changed.document_id: len(changed.chunks),
                docs[1].document_id: len(docs[1].chunks),
            }
            assert (
                connection.execute(
                    "SELECT count(*) FROM assistant.documents WHERE document_id=%s",
                    (docs[2].document_id,),
                ).fetchone()[0]
                == 0
            )
            second = ingest(connection, (changed, docs[1]), embedder)
            assert second["updated_documents"] == 0
        finally:
            connection.rollback()  # Preserve the user's populated corpus.


def test_model_change_requires_explicit_reindex():
    settings = Settings()
    docs = load_corpus(Path(__file__).resolve().parents[1])

    class DifferentModel:
        model_id = "test-different-model-384"

        def embed(self, texts: list[str]) -> list[list[float]]:
            return [[0.0] * DIMENSIONS for _ in texts]

    with psycopg.connect(_dsn(settings)) as connection:
        try:
            with pytest.raises(IncompatibleIndex, match="reindex_required"):
                ingest(connection, docs, DifferentModel())
            connection.rollback()
            result = ingest(connection, docs, DifferentModel(), reindex=True)
            assert result["embedded_chunks"] == sum(len(doc.chunks) for doc in docs)
            assert connection.execute("SELECT model_id FROM assistant.index_state").fetchone()[
                0
            ] == (DifferentModel.model_id)
        finally:
            connection.rollback()  # Test vectors are never committed.


def test_metadata_change_updates_citations_without_reembedding():
    settings = Settings()
    docs = load_corpus(Path(__file__).resolve().parents[1])
    embedder = LocalEmbedder(Path(settings.assistant_model_cache_dir))
    renamed = replace(docs[0], title="Renamed local operations runbooks", topic="incident-triage")
    with psycopg.connect(_dsn(settings)) as connection:
        try:
            baseline = ingest(connection, docs, embedder)
            changed = ingest(connection, (renamed, *docs[1:]), embedder)
            assert changed["updated_documents"] == 1
            assert changed["embedded_chunks"] == 0
            assert changed["corpus_sha256"] != baseline["corpus_sha256"]
            assert connection.execute(
                "SELECT title, topic FROM assistant.documents WHERE document_id=%s",
                (renamed.document_id,),
            ).fetchone() == (renamed.title, renamed.topic)
            mode, rows = retrieve(connection, "local operations runbooks", embedder=embedder)
            assert mode == "semantic"
            assert any(row["title"] == renamed.title for row in rows)
        finally:
            connection.rollback()


def test_offline_evidence_across_detector_types_in_disposable_database():
    """Exercise persisted, original-build evidence without touching the user's data."""
    admin = Settings()
    database = f"pulseforge_assistant_review_{uuid4().hex}"
    with psycopg.connect(_dsn(admin), autocommit=True) as connection:
        connection.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(database)))
    try:
        settings = Settings(postgres_db=database)
        now = datetime.now(UTC)
        original_build, current_build = uuid4(), uuid4()
        cases = (
            (
                "payment_failure_rate_increase",
                Decimal("0.625000"),
                40,
                Decimal("0.100000"),
                Decimal("0.200000"),
                "payment_attempt",
            ),
            (
                "shipment_delay_rate_increase",
                Decimal("0.375000"),
                24,
                Decimal("0.050000"),
                Decimal("0.150000"),
                "shipment_cohort",
            ),
            (
                "refund_request_spike",
                Decimal("0.250000"),
                32,
                Decimal("0.040000"),
                Decimal("0.100000"),
                "refund_request",
            ),
            (
                "order_volume_drop",
                Decimal("7.000000"),
                None,
                Decimal("30.000000"),
                Decimal("15.000000"),
                "order",
            ),
        )
        with psycopg.connect(_dsn(settings)) as connection:
            root = Path(__file__).resolve().parents[1]
            for name in ("0001_product_layer.sql", "0002_product_evidence.sql"):
                connection.execute(
                    (root / "src/pulseforge/product/migrations" / name).read_text(encoding="utf-8")
                )
            apply_migrations(connection)
            ingest(
                connection,
                load_corpus(root),
                LocalEmbedder(Path(settings.assistant_model_cache_dir)),
            )
            for build_id, published in (
                (original_build, now - timedelta(days=2)),
                (current_build, now),
            ):
                connection.execute(
                    "INSERT INTO product.analytics_builds "
                    "(build_id,build_key,status,started_at,completed_at,published_at,"
                    "source_event_count) "
                    "VALUES (%s,%s,'succeeded',%s,%s,%s,1)",
                    (
                        build_id,
                        str(build_id),
                        published - timedelta(minutes=2),
                        published - timedelta(minutes=1),
                        published,
                    ),
                )
            records = []
            for detector, observed, denominator, baseline, threshold, role in cases:
                incident_id, event_id = uuid4(), uuid4()
                connection.execute(
                    "INSERT INTO product.incidents "
                    "(incident_id,uniqueness_key,detector_name,detector_version,region_code,"
                    "evaluation_start,evaluation_end,observed_metric,observed_denominator,"
                    "baseline_metric,threshold,severity,summary,analytics_build_id) "
                    "VALUES (%s,%s,%s,'v1','us-east',%s,%s,%s,%s,%s,%s,'warning',%s,%s)",
                    (
                        incident_id,
                        str(incident_id),
                        detector,
                        now - timedelta(days=3),
                        now - timedelta(days=3, hours=-1),
                        observed,
                        denominator,
                        baseline,
                        threshold,
                        detector,
                        original_build,
                    ),
                )
                connection.execute(
                    "INSERT INTO product.incident_evidence VALUES (%s,%s,%s,%s)",
                    (incident_id, event_id, role, now - timedelta(days=3)),
                )
                connection.execute(
                    "INSERT INTO product.analytics_evidence VALUES (%s,%s,%s,%s,'us-east')",
                    (original_build, event_id, role, now - timedelta(days=3)),
                )
                records.append((incident_id, event_id, observed, denominator, baseline, threshold))
            connection.commit()
        for incident_id, event_id, observed, denominator, baseline, threshold in records:
            result = validate_citations(explain_offline(settings, incident_id))
            expected_numbers = (
                f"Observed {observed}"
                + (
                    f" with denominator {denominator}"
                    if denominator is not None
                    else " (count metric)"
                )
                + f"; recorded baseline {baseline} and threshold {threshold}."
            )
            assert any(
                item.text == expected_numbers and f"incident:{incident_id}" in item.citation_ids
                for item in result.facts
            )
            assert result.analytics_build_id == original_build
            assert result.current_build_id == current_build
            assert result.source_reference_count == 1
            assert f"event:{event_id}" in {item.citation_id for item in result.citations}
            assert not any(
                "absent from the original build" in item.text for item in result.limitations
            )
            assert result.hypotheses == []
            assert all(
                "Applicability to this incident is not established" in step.text
                for step in result.diagnostic_steps
            )
            assert any("not verified diagnoses" in item.text for item in result.limitations)
            assert any("older than" in item.text for item in result.limitations)
        with psycopg.connect(_dsn(settings)) as connection:
            connection.execute(
                "DELETE FROM product.analytics_evidence WHERE build_id=%s", (original_build,)
            )
            connection.commit()
        missing = explain_offline(settings, records[0][0])
        assert any("absent from the original build" in item.text for item in missing.limitations)
        assert missing.analytics_build_id == original_build
        failed_build, unavailable_incident = uuid4(), uuid4()
        with psycopg.connect(_dsn(settings)) as connection:
            connection.execute(
                "INSERT INTO product.analytics_builds "
                "(build_id,build_key,status,started_at,completed_at) "
                "VALUES (%s,%s,'failed',%s,%s)",
                (
                    failed_build,
                    str(failed_build),
                    now - timedelta(hours=2),
                    now - timedelta(hours=1),
                ),
            )
            connection.execute(
                "INSERT INTO product.incidents "
                "(incident_id,uniqueness_key,detector_name,detector_version,region_code,"
                "evaluation_start,evaluation_end,observed_metric,baseline_metric,threshold,"
                "severity,summary,analytics_build_id) "
                "VALUES (%s,%s,'payment_failure_rate_increase','v1','us-east',"
                "%s,%s,0.500000,0.100000,0.200000,'warning','missing build',%s)",
                (
                    unavailable_incident,
                    str(unavailable_incident),
                    now - timedelta(days=3),
                    now - timedelta(days=3, hours=-1),
                    failed_build,
                ),
            )
            connection.commit()
        unavailable = explain_offline(settings, unavailable_incident)
        assert unavailable.analytics_build_id == failed_build
        assert unavailable.original_build_published_at is None
        assert unavailable.current_build_id == current_build
        assert any(
            "original successful build is unavailable" in item.text
            for item in unavailable.limitations
        )
        # Retrieval-only evaluation queries are not a user-question API. Every
        # no-relevant-section case must be rejected if submitted as a question.
        cases_path = Path(__file__).resolve().parents[1] / "docs/assistant/eval-cases.jsonl"
        cases = [json.loads(line) for line in cases_path.read_text(encoding="utf-8").splitlines()]
        no_relevant = [case for case in cases if not case["expected_sections"]]
        assert len(no_relevant) == 7
        for case in no_relevant:
            response = httpx.post(
                f"http://127.0.0.1:8000/api/v1/incidents/{records[0][0]}/explanation",
                json={"mode": "offline", "question": case["query"]},
                timeout=15,
            )
            assert response.status_code == 422, case["id"]
    finally:
        with psycopg.connect(_dsn(admin), autocommit=True) as connection:
            connection.execute(
                sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(database))
            )
