from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import pytest

from pulseforge.assistant.corpus import (
    ALLOWED_PATHS,
    DIMENSIONS,
    IncompatibleIndex,
    LocalEmbedder,
    chunk_markdown,
    load_corpus,
)
from pulseforge.assistant.models import Citation, Explanation, Statement, validate_citations


def test_chunks_are_deterministic_bounded_and_section_aware():
    markdown = "# Runbook\n\n## Payments\n\nLook at failures.\n\nCheck the published build.\n"
    first = chunk_markdown("runbook", markdown)
    assert first == chunk_markdown("runbook", markdown)
    assert first[0].section_id == "payments-1"
    assert all(len(chunk.text) <= 1800 for chunk in first)
    assert first[0].chunk_id == "runbook:payments-1:0"


def test_corpus_only_contains_explicit_local_project_docs():
    project = Path(__file__).resolve().parents[1]
    docs = load_corpus(project)
    assert {document.source_path for document in docs} == ALLOWED_PATHS
    assert len({document.document_id for document in docs}) == len(docs)
    assert all(document.chunks for document in docs)


def test_embedding_revision_must_match_local_assets(tmp_path):
    reference = tmp_path / "models--qdrant--all-MiniLM-L6-v2-onnx" / "refs"
    reference.mkdir(parents=True)
    (reference / "main").write_text("wrong-revision", encoding="utf-8")
    with pytest.raises((IncompatibleIndex, RuntimeError)):
        LocalEmbedder(tmp_path)


def test_citation_ids_must_be_supplied_to_invocation():
    incident_id = uuid4()
    response = Explanation(
        label="Offline evidence summary — no generative model used.",
        mode="offline",
        incident_id=incident_id,
        analytics_build_id=uuid4(),
        original_build_published_at=None,
        current_build_id=None,
        retrieval_mode="unavailable",
        corpus_sha256=None,
        generated_at=datetime.now(UTC),
        facts=[Statement(text="Observed fact", citation_ids=["fabricated"])],
        interpretations=[],
        hypotheses=[],
        diagnostic_steps=[],
        limitations=[],
        citations=[
            Citation(citation_id=f"incident:{incident_id}", kind="incident", label="Incident")
        ],
    )
    with pytest.raises(ValueError, match="unprovided_citation"):
        validate_citations(response)
    assert DIMENSIONS == 384
