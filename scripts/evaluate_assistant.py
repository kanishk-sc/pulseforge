"""Measured local retrieval and offline-incident evaluation; no provider calls."""

import argparse
import json
import os
import shlex
import statistics
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

import psycopg

from pulseforge.assistant.corpus import LocalEmbedder, digest, retrieve
from pulseforge.assistant.models import validate_citations
from pulseforge.assistant.provider import PROMPT_VERSION
from pulseforge.assistant.service import _dsn, explain_offline
from pulseforge.config import Settings


def percentile(values: list[float], fraction: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    return round(ordered[min(len(ordered) - 1, int((len(ordered) - 1) * fraction))], 3)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--split", choices=["dev", "heldout", "all"], default="all")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    dataset = Path("docs/assistant/eval-cases.jsonl")
    cases = [json.loads(line) for line in dataset.read_text(encoding="utf-8").splitlines()]
    selected = [case for case in cases if args.split == "all" or case["split"] == args.split]
    settings = Settings()
    embedder = LocalEmbedder(Path(settings.assistant_model_cache_dir))
    start = time.perf_counter()
    results = []
    with psycopg.connect(_dsn(settings)) as connection:
        state = connection.execute(
            "SELECT corpus_sha256, model_id FROM assistant.index_state WHERE singleton=true"
        ).fetchone()
        if not state:
            raise RuntimeError("corpus_not_indexed")
        for case in selected:
            started = time.perf_counter()
            mode, rows = retrieve(connection, case["query"], embedder=embedder, top_k=4)
            elapsed_ms = round((time.perf_counter() - started) * 1000, 3)
            hits = sorted(set(case["expected_sections"]) & {row["chunk_id"] for row in rows})
            results.append(
                {
                    "id": case["id"],
                    "split": case["split"],
                    "scenario": case["scenario"],
                    "retrieval_mode": mode,
                    "expected_sections": case["expected_sections"],
                    "returned_sections": [row["chunk_id"] for row in rows],
                    "hits": hits,
                    "recall_at_4": (
                        len(hits) / len(case["expected_sections"])
                        if case["expected_sections"]
                        else None
                    ),
                    "latency_ms": elapsed_ms,
                    "behavioral_case_status": "not_executed_without_matching_incident_fixture",
                }
            )
        incident = connection.execute(
            "SELECT incident_id, analytics_build_id, observed_metric, baseline_metric, "
            "threshold, observed_denominator FROM product.incidents "
            "WHERE detector_name='payment_failure_rate_increase' "
            "ORDER BY detected_at DESC LIMIT 1"
        ).fetchone()
        if not incident:
            raise RuntimeError("payment_incident_fixture_unavailable")
        event_ids = {
            str(row[0])
            for row in connection.execute(
                "SELECT source_event_id FROM product.incident_evidence WHERE incident_id=%s",
                (incident[0],),
            ).fetchall()
        }
    explanation_started = time.perf_counter()
    explanation = validate_citations(explain_offline(settings, incident[0]))
    explanation_ms = round((time.perf_counter() - explanation_started) * 1000, 3)
    fact_text = " ".join(statement.text for statement in explanation.facts)
    numeric_fidelity = all(
        str(value) in fact_text for value in (incident[2], incident[3], incident[4], incident[5])
    )
    event_consistency = all(
        citation.citation_id.split(":", 1)[1] in event_ids
        for citation in explanation.citations
        if citation.kind == "event"
    )
    scored = [case["recall_at_4"] for case in results if case["recall_at_4"] is not None]
    split_recall = {
        split: (
            statistics.mean(
                case["recall_at_4"]
                for case in results
                if case["split"] == split and case["recall_at_4"] is not None
            )
            if any(case["split"] == split and case["recall_at_4"] is not None for case in results)
            else None
        )
        for split in ("dev", "heldout")
    }
    commit = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    dirty = bool(subprocess.check_output(["git", "status", "--porcelain"], text=True).strip())
    report = {
        "evaluated_at": datetime.now(UTC).isoformat(),
        "tested_commit": commit,
        "working_tree_dirty": dirty,
        "dataset_version": digest(dataset.read_text(encoding="utf-8")),
        "dataset_cases": len(cases),
        "selected_cases": len(selected),
        "corpus_version": state[0],
        "embedding_model": state[1],
        "prompt_version": PROMPT_VERSION,
        "execution_mode": "offline",
        "provider": None,
        "provider_model": None,
        "exact_command": os.environ.get("PULSEFORGE_EVAL_COMMAND")
        or shlex.join([sys.executable, *sys.argv]),
        "retrieval": {
            "scored_cases": len(scored),
            "unscored_no_relevant_section": len(results) - len(scored),
            "recall_at_4": statistics.mean(scored) if scored else None,
            "recall_at_4_by_split": split_recall,
            "latency_p50_ms": percentile([case["latency_ms"] for case in results], 0.5),
            "latency_p95_ms": percentile([case["latency_ms"] for case in results], 0.95),
            "cases": results,
        },
        "real_offline_incident": {
            "incident_id": str(incident[0]),
            "original_build_id": str(incident[1]),
            "response_build_id": str(explanation.analytics_build_id),
            "citation_validity": True,
            "numerical_fidelity": numeric_fidelity,
            "event_reference_consistency": event_consistency,
            "abstains_from_root_cause": not explanation.hypotheses,
            "retrieval_mode": explanation.retrieval_mode,
            "latency_ms": explanation_ms,
            "token_usage": None,
        },
        "semantic_grounding_rubric": {
            "status": "not_human_reviewed",
            "criteria": [
                "Every causal statement is supported by supplied original-build evidence",
                "Diagnostic steps match cited runbook text",
                "Missing/stale telemetry is interpreted as unknown",
                "Explanation is useful without overstating certainty",
            ],
            "score": None,
        },
        "live_provider_evaluation": "unverified_not_authorized",
        "elapsed_seconds": round(time.perf_counter() - start, 3),
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(
        json.dumps(
            {
                "cases": len(results),
                "recall_at_4": report["retrieval"]["recall_at_4"],
                "numeric_fidelity": numeric_fidelity,
                "event_reference_consistency": event_consistency,
                "output": str(args.output),
            }
        )
    )
    return 0 if numeric_fidelity and event_consistency else 1


if __name__ == "__main__":
    raise SystemExit(main())
