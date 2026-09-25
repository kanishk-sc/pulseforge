from dataclasses import replace
from pathlib import Path

import httpx
import psycopg
import pytest

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
