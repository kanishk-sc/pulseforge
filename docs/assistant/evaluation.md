# Phase 6 evaluation and limits

The version-controlled [case set](eval-cases.jsonl) contains 32 cases, originally split
before retrieval tuning into 16 development and 16 held-out cases. During PR #6 review,
the original held-out queries were inspected while considering an abstention threshold.
That split is now **review-exposed**, not an untouched holdout. A separate
[eight-case review holdout](eval-review-holdout.jsonl) was written before its first
retrieval run and was not tuned after scoring. Each case records its scenario,
query, relevant runbook section IDs (when one exists), required facts, forbidden
claims, and whether causal abstention is expected. It covers payment, shipment,
refund, order-volume and infrastructure interpretation; stale or absent evidence;
prompt injection; provider failures; citation fabrication; and corpus replacement.
The case labels are expectations, not proof that those behaviors were executed.

Run `uv run python scripts/evaluate_assistant.py --split all --output
docs/assistant/eval-results.json` against populated local PostgreSQL **and the local
API** after corpus ingestion. The runner measures retrieval recall@4 and latency.
Seven original cases label no relevant section and are excluded from recall. For each,
the runner submits its query as an unsupported `question` field to the live explanation
API and requires HTTP 422. This verifies the product's narrow input boundary, **not**
that semantic retrieval recognizes irrelevant text. The runner also executes one
persisted payment incident and checks citation IDs, an exact whole-field copy of
recorded numeric values, original build identity, source event membership, and no
root-cause hypothesis. The integration suite separately creates a disposable database
with persisted payment, shipment, refund and order-volume incidents, checks exact
numeric/source/build consistency and stale labeling, and removes one original-build
projection to verify explicit uncertainty. It does not claim a live immature-cohort
detector evaluation or human semantic grounding. Changed/deleted corpus and mocked
provider failure contracts have separate tests. No case is silently scored as an
executed scenario without a matching fixture.

Semantic search may still return a top-ranked section for an unrelated query; no
calibrated relevance-abstention threshold is claimed. Offline explanations label
runbook sections as candidate context and explicitly say retrieval rank does not
establish applicability. The no-relevant API assertions check that unsupported
free-form questions cannot turn those candidates into a user-facing answer.

The measured report is [eval-results.json](eval-results.json). Development cases
were used to choose bounded semantic/full-text rank fusion. The held-out cases were
not used for that choice. The recorded outcome is 14/14 development and 10/11
original held-out relevant sections retrieved in the top four (24/25 overall, 0.96
mean case recall@4). The original held-out result is historical and review-exposed.
The fresh review holdout [report](eval-review-holdout-results.json) retrieved 7/7
labeled relevant sections; one no-relevant case was excluded from recall and its
unsupported question was rejected with HTTP 422.
This is retrieval relevance on a small synthetic corpus, not business-incident
grounding, a general abstention metric, or a production-quality estimate.

## Claim-level rubric

For each human-reviewed explanation, inspect the exact original-build evidence
bundle and each cited runbook section, then score each criterion 0 (unsupported),
1 (partially supported), or 2 (fully supported):

1. Every causal claim is backed by original-build evidence; unsupported causes are
   stated as hypotheses or omitted.
2. Numeric facts, denominator, baseline, threshold, UTC window, and build identity
   match the persisted incident exactly.
3. Each diagnostic step follows its cited runbook section and does not imply
   remediation already occurred.
4. Missing or stale analytics, absent source projections, and unavailable telemetry
   remain explicitly uncertain.
5. The answer is useful for investigation without inventing service health,
   confidence percentages, source IDs, or completed actions.

The report marks human semantic grounding and usefulness **not reviewed**. A valid
citation ID says only that the source was supplied, not that it supports a claim.
No model judge or human score is asserted. Live provider evaluation is
**unverified**: the adapter was exercised with mocked transport tests, but no
billable inference or external incident/runbook transmission was authorized.
Token usage is therefore null. A live smoke test requires a separately authorized
provider and bounded budget; see [operations](operations.md).
