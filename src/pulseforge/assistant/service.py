"""Application-owned incident evidence selection and deterministic explanation."""

import time
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID

import psycopg
from psycopg.conninfo import make_conninfo
from psycopg.rows import dict_row

from pulseforge.assistant.corpus import (
    EmbeddingUnavailable,
    IncompatibleIndex,
    LocalEmbedder,
    retrieve,
)
from pulseforge.assistant.models import (
    Citation,
    EvidenceBundle,
    Explanation,
    RunbookReference,
    Statement,
    validate_citations,
)
from pulseforge.config import Settings
from pulseforge.telemetry import (
    ASSISTANT_RETRIEVAL,
    ASSISTANT_RETRIEVAL_DURATION,
    observe_histogram,
    record_counter,
)


class IncidentNotFound(Exception):
    pass


class AssistantUnavailable(Exception):
    pass


def _dsn(settings: Settings) -> str:
    return make_conninfo(
        "",
        host=settings.postgres_host,
        port=settings.postgres_port,
        dbname=settings.postgres_db,
        user=settings.postgres_user,
        password=settings.postgres_password.get_secret_value(),
        connect_timeout=3,
    )


def _row(cursor: psycopg.Cursor, query: str, params: tuple = ()) -> dict | None:
    cursor.execute(query, params)
    return cursor.fetchone()


def explain_offline(settings: Settings, incident_id: UUID) -> Explanation:
    """One repeatable-read read-only view; no model can issue database commands."""
    try:
        embedder = LocalEmbedder(Path(settings.assistant_model_cache_dir))
    except (EmbeddingUnavailable, IncompatibleIndex):
        embedder = None
    with psycopg.connect(_dsn(settings), row_factory=dict_row) as connection:
        connection.execute("BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        with connection.cursor() as cursor:
            incident = _row(
                cursor,
                "SELECT incident_id, detector_name, detector_version, region_code, "
                "evaluation_start, evaluation_end, observed_metric, observed_denominator, "
                "baseline_metric, threshold, severity, summary, analytics_build_id, "
                "detected_at FROM product.incidents WHERE incident_id=%s",
                (incident_id,),
            )
            if incident is None:
                raise IncidentNotFound
            original = _row(
                cursor,
                "SELECT build_id, status, published_at, source_max_event_ts, "
                "source_max_ingested_at, source_event_count FROM product.analytics_builds "
                "WHERE build_id=%s",
                (incident["analytics_build_id"],),
            )
            current = _row(
                cursor,
                "SELECT build_id, published_at FROM product.analytics_builds "
                "WHERE status='succeeded' ORDER BY published_at DESC, build_id DESC LIMIT 1",
            )
            attempts = _row(
                cursor,
                "SELECT max(started_at) FILTER (WHERE status='running') AS running_at, "
                "max(completed_at) FILTER (WHERE status='failed') AS failed_at "
                "FROM product.analytics_builds",
            )
            source_count = _row(
                cursor,
                "SELECT count(*) AS total FROM product.incident_evidence WHERE incident_id=%s",
                (incident_id,),
            )["total"]
            cursor.execute(
                "SELECT e.source_event_id, e.event_ts, e.evidence_role, "
                "a.source_event_id IS NOT NULL AS in_original_build "
                "FROM product.incident_evidence e LEFT JOIN product.analytics_evidence a "
                "ON a.build_id=%s AND a.source_event_id=e.source_event_id "
                "AND a.evidence_kind=e.evidence_role "
                "WHERE e.incident_id=%s ORDER BY e.event_ts, e.source_event_id LIMIT 12",
                (incident["analytics_build_id"], incident_id),
            )
            evidence = list(cursor.fetchall())
            query = (
                f"{incident['detector_name'].replace('_', ' ')} "
                f"{incident['region_code']} incident triage"
            )
            corpus_exists = _row(cursor, "SELECT to_regclass('assistant.index_state') AS relation")
            if corpus_exists and corpus_exists["relation"]:
                retrieval_started = time.perf_counter()
                try:
                    retrieval_mode, chunks = retrieve(connection, query, embedder=embedder, top_k=4)
                except IncompatibleIndex:
                    retrieval_mode, chunks = retrieve(connection, query, embedder=None, top_k=4)
                observe_histogram(
                    ASSISTANT_RETRIEVAL_DURATION,
                    time.perf_counter() - retrieval_started,
                    retrieval_mode,
                )
                state = _row(cursor, "SELECT corpus_sha256 FROM assistant.index_state")
            else:
                retrieval_mode, chunks = "unavailable", []
                state = None
    bundle = EvidenceBundle.model_validate(
        {
            "incident": incident,
            "original_build": original,
            "current_build": current,
            "latest_failed_at": attempts["failed_at"] if attempts else None,
            "running_at": attempts["running_at"] if attempts else None,
            "source_reference_count": source_count,
            "source_references": evidence,
            "runbooks": [
                {key: chunk[key] for key in RunbookReference.model_fields} for chunk in chunks
            ],
            "retrieval_mode": retrieval_mode,
            "corpus_sha256": state["corpus_sha256"] if state else None,
        }
    )
    # All presentation and optional-provider evidence is derived from the validated bundle.
    incident = bundle.incident.model_dump()
    original = bundle.original_build.model_dump() if bundle.original_build else None
    current = bundle.current_build.model_dump() if bundle.current_build else None
    attempts = {"failed_at": bundle.latest_failed_at, "running_at": bundle.running_at}
    evidence = [item.model_dump() for item in bundle.source_references]
    chunks = [item.model_dump() for item in bundle.runbooks]
    source_count = bundle.source_reference_count
    retrieval_mode = bundle.retrieval_mode
    record_counter(ASSISTANT_RETRIEVAL, retrieval_mode)
    incident_ref = f"incident:{incident_id}"
    citations = [
        Citation(citation_id=incident_ref, kind="incident", label="Persisted detector incident")
    ]
    facts = [
        Statement(
            text=(
                f"{incident['detector_name']} version {incident['detector_version']} was recorded "
                f"for {incident['region_code']} during "
                f"[{incident['evaluation_start'].isoformat()}, "
                f"{incident['evaluation_end'].isoformat()})."
            ),
            citation_ids=[incident_ref],
        ),
        Statement(
            text=(
                f"Observed {incident['observed_metric']}"
                + (
                    f" with denominator {incident['observed_denominator']}"
                    if incident["observed_denominator"] is not None
                    else " (count metric)"
                )
                + f"; recorded baseline {incident['baseline_metric']} and threshold "
                f"{incident['threshold']}."
            ),
            citation_ids=[incident_ref],
        ),
    ]
    interpretations = [
        Statement(
            text="The persisted detector comparison crossed its recorded threshold; "
            "this is an anomaly finding, not an established root cause.",
            citation_ids=[incident_ref],
        )
    ]
    limitations = []
    build_id = incident["analytics_build_id"]
    build_ref = f"build:{build_id}"
    if original and original["status"] == "succeeded":
        citations.append(
            Citation(
                citation_id=build_ref, kind="build", label="Original successful analytics build"
            )
        )
        facts.append(
            Statement(
                text=f"The finding uses original analytics build {build_id}, published "
                f"{original['published_at'].isoformat()}.",
                citation_ids=[incident_ref, build_ref],
            )
        )
        age = (datetime.now(UTC) - original["published_at"]).total_seconds()
        if age > settings.analytics_stale_after_seconds:
            limitations.append(
                Statement(
                    text="The incident's original publication is older than the configured "
                    "freshness window; "
                    "historical evidence remains labeled, not current pipeline health.",
                    citation_ids=[build_ref],
                )
            )
    else:
        limitations.append(
            Statement(
                text="The incident's original successful build is unavailable; "
                "no newer build is substituted.",
                citation_ids=[incident_ref],
            )
        )
    missing_projection = 0
    for item in evidence:
        event_ref = f"event:{item['source_event_id']}"
        citations.append(
            Citation(
                citation_id=event_ref,
                kind="event",
                label=f"{item['evidence_role']} source event",
            )
        )
        facts.append(
            Statement(
                text=f"The incident records source reference {item['source_event_id']} "
                f"({item['evidence_role']}) at {item['event_ts'].isoformat()}.",
                citation_ids=[incident_ref, event_ref],
            )
        )
        if not item["in_original_build"]:
            missing_projection += 1
    if missing_projection:
        limitations.append(
            Statement(
                text=f"{missing_projection} of {len(evidence)} bounded incident source references "
                "are absent from the original build's evidence projection; those links "
                "cannot be independently confirmed here.",
                citation_ids=[incident_ref],
            )
        )
    if not evidence:
        limitations.append(
            Statement(
                text="No source-event references were retained for this finding.",
                citation_ids=[incident_ref],
            )
        )
    if source_count > len(evidence):
        limitations.append(
            Statement(
                text=f"This bounded explanation includes {len(evidence)} of {source_count} "
                "persisted source references; inspect the incident detail for the rest.",
                citation_ids=[incident_ref],
            )
        )
    steps = []
    for chunk in chunks:
        reference = f"runbook:{chunk['chunk_id']}"
        citations.append(
            Citation(
                citation_id=reference,
                kind="runbook",
                label=f"{chunk['title']} — {chunk['section_title']}",
                source_path=chunk["source_path"],
                section_id=chunk["section_id"],
                document_version=chunk["content_sha256"],
                excerpt=chunk["chunk_text"][:700],
            )
        )
        steps.append(
            Statement(
                text=f"Candidate context: review {chunk['title']}, section "
                f"“{chunk['section_title']}”. Applicability to this incident "
                "is not established by retrieval rank.",
                citation_ids=[reference],
            )
        )
    if chunks:
        limitations.append(
            Statement(
                text="Retrieved runbook sections are candidates, not verified diagnoses "
                "or evidence that a suggested action will help.",
                citation_ids=[incident_ref],
            )
        )
    if not chunks:
        limitations.append(
            Statement(
                text="No indexed runbook section was returned.",
                citation_ids=[incident_ref],
            )
        )
    if retrieval_mode == "lexical_fallback":
        limitations.append(
            Statement(
                text="Runbooks were matched by labeled lexical fallback, not semantic retrieval; "
                "the local embedding model was unavailable or incompatible.",
                citation_ids=[incident_ref],
            )
        )
    elif retrieval_mode == "unavailable":
        limitations.append(
            Statement(
                text="Runbook retrieval is unavailable or not indexed.",
                citation_ids=[incident_ref],
            )
        )
    if current and current["build_id"] != build_id:
        status_ref = f"status:{current['build_id']}"
        citations.append(
            Citation(citation_id=status_ref, kind="status", label="Current publication identity")
        )
        limitations.append(
            Statement(
                text=f"The current successful publication is {current['build_id']}; "
                "it is separate from this incident's original build and was not used "
                "for its metrics.",
                citation_ids=[incident_ref, status_ref],
            )
        )
    status_ref = "status:build-ledger"
    if attempts and (attempts["running_at"] or attempts["failed_at"]):
        citations.append(
            Citation(citation_id=status_ref, kind="status", label="Current product build ledger")
        )
        if attempts["running_at"]:
            limitations.append(
                Statement(
                    text=f"A separate analytics build was running as of "
                    f"{attempts['running_at'].isoformat()}; it is not served by this explanation.",
                    citation_ids=[status_ref],
                )
            )
        if attempts["failed_at"]:
            limitations.append(
                Statement(
                    text=f"A failed analytics attempt is recorded at "
                    f"{attempts['failed_at'].isoformat()}; the failure did not replace "
                    "the incident's original build.",
                    citation_ids=[status_ref],
                )
            )
    limitations.append(
        Statement(
            text="Spark liveness is not observed by this assistant. Missing telemetry is unknown, "
            "not proof of health or failure.",
            citation_ids=[incident_ref],
        )
    )
    return validate_citations(
        Explanation(
            label="Offline evidence summary — no generative model used.",
            mode="offline",
            incident_id=incident_id,
            analytics_build_id=build_id,
            original_build_published_at=original["published_at"] if original else None,
            current_build_id=current["build_id"] if current else None,
            current_build_published_at=current["published_at"] if current else None,
            latest_failed_build_at=attempts["failed_at"] if attempts else None,
            running_build_started_at=attempts["running_at"] if attempts else None,
            source_reference_count=source_count,
            source_references_included=len(evidence),
            retrieval_mode=retrieval_mode,
            corpus_sha256=bundle.corpus_sha256,
            generated_at=datetime.now(UTC),
            facts=facts,
            interpretations=interpretations,
            hypotheses=[],
            diagnostic_steps=steps,
            limitations=limitations,
            citations=citations,
        )
    )
