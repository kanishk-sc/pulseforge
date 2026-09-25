from datetime import datetime
from decimal import Decimal
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, model_validator


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class ExplainRequest(StrictModel):
    mode: Literal["offline", "provider"] = "offline"


class IncidentSnapshot(StrictModel):
    incident_id: UUID
    detector_name: str
    detector_version: str
    region_code: str
    evaluation_start: datetime
    evaluation_end: datetime
    observed_metric: Decimal
    observed_denominator: int | None
    baseline_metric: Decimal
    threshold: Decimal
    severity: Literal["warning", "critical"]
    summary: str
    analytics_build_id: UUID
    detected_at: datetime


class BuildSnapshot(StrictModel):
    build_id: UUID
    status: Literal["running", "succeeded", "failed"]
    published_at: datetime | None
    source_max_event_ts: datetime | None
    source_max_ingested_at: datetime | None
    source_event_count: int | None


class CurrentBuildSnapshot(StrictModel):
    build_id: UUID
    published_at: datetime


class SourceReference(StrictModel):
    source_event_id: UUID
    event_ts: datetime
    evidence_role: str
    in_original_build: bool


class RunbookReference(StrictModel):
    chunk_id: str
    section_id: str
    section_title: str
    chunk_text: str = Field(max_length=2000)
    document_id: str
    title: str
    topic: str
    source_path: str
    content_sha256: str


class EvidenceBundle(StrictModel):
    incident: IncidentSnapshot
    original_build: BuildSnapshot | None
    current_build: CurrentBuildSnapshot | None
    latest_failed_at: datetime | None
    running_at: datetime | None
    source_reference_count: int = Field(ge=0)
    source_references: list[SourceReference] = Field(max_length=12)
    runbooks: list[RunbookReference] = Field(max_length=4)
    retrieval_mode: Literal["semantic", "lexical_fallback", "unavailable"]
    corpus_sha256: str | None

    @model_validator(mode="after")
    def validate_identity(self):
        if self.incident.evaluation_start >= self.incident.evaluation_end:
            raise ValueError("invalid_incident_window")
        if self.original_build and self.original_build.build_id != self.incident.analytics_build_id:
            raise ValueError("mixed_analytics_builds")
        if len(self.source_references) > self.source_reference_count:
            raise ValueError("source_reference_count_mismatch")
        return self


class Citation(StrictModel):
    citation_id: str
    kind: Literal["incident", "build", "event", "runbook", "status"]
    label: str
    source_path: str | None = None
    section_id: str | None = None
    document_version: str | None = None
    excerpt: str | None = Field(default=None, max_length=700)


class Statement(StrictModel):
    text: str = Field(min_length=1, max_length=700)
    citation_ids: list[str] = Field(min_length=1, max_length=8)


class Explanation(StrictModel):
    label: str
    mode: Literal["offline", "provider"]
    provider: str | None = None
    model: str | None = None
    input_tokens: int | None = Field(default=None, ge=0)
    output_tokens: int | None = Field(default=None, ge=0)
    incident_id: UUID
    analytics_build_id: UUID
    original_build_published_at: datetime | None
    current_build_id: UUID | None
    current_build_published_at: datetime | None = None
    latest_failed_build_at: datetime | None = None
    running_build_started_at: datetime | None = None
    source_reference_count: int = Field(default=0, ge=0)
    source_references_included: int = Field(default=0, ge=0, le=12)
    retrieval_mode: Literal["semantic", "lexical_fallback", "unavailable"]
    corpus_sha256: str | None
    generated_at: datetime
    facts: list[Statement] = Field(max_length=20)
    interpretations: list[Statement] = Field(max_length=8)
    hypotheses: list[Statement] = Field(max_length=8)
    diagnostic_steps: list[Statement] = Field(max_length=8)
    limitations: list[Statement] = Field(max_length=12)
    citations: list[Citation] = Field(max_length=40)


def validate_citations(explanation: Explanation) -> Explanation:
    ids = {citation.citation_id for citation in explanation.citations}
    if len(ids) != len(explanation.citations):
        raise ValueError("duplicate_citation_id")
    for statement in (
        explanation.facts
        + explanation.interpretations
        + explanation.hypotheses
        + explanation.diagnostic_steps
        + explanation.limitations
    ):
        if any(citation_id not in ids for citation_id in statement.citation_ids):
            raise ValueError("unprovided_citation")
    if not any(c.kind == "incident" for c in explanation.citations):
        raise ValueError("incident_citation_required")
    return explanation
