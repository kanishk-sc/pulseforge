# Phase 6 evaluation and limits

The version-controlled [case set](eval-cases.jsonl) contains 32 cases, split before
retrieval tuning into 16 development and 16 held-out cases. Each records its scenario,
query, relevant runbook section IDs (when one exists), required facts, forbidden
claims, and whether causal abstention is expected. It covers payment, shipment,
refund, order-volume and infrastructure interpretation; stale or absent evidence;
prompt injection; provider failures; citation fabrication; and corpus replacement.
The case labels are expectations, not proof that those behaviors were executed.

Run `uv run python scripts/evaluate_assistant.py --split all --output
docs/assistant/eval-results.json` against populated local PostgreSQL after corpus
ingestion. The runner measures retrieval recall@4 and latency for every case. Seven
cases intentionally label no relevant section and are excluded from recall rather
than counted as successes. It also executes one actual persisted payment incident
through offline explanation and checks citation ID validity, exact recorded numeric
values, original build identity, source event membership, and absence of a root-cause
hypothesis. Separate focused unit/integration tests exercise missing-build,
changed/deleted-document, provider timeout/malformed-output, and citation-rejection
behavior. No case is silently scored as an executed scenario without a matching
incident fixture.

The measured report is [eval-results.json](eval-results.json). Development cases
were used to choose bounded semantic/full-text rank fusion. The held-out cases were
not used for that choice. The recorded outcome is 14/14 development and 10/11
held-out relevant sections retrieved in the top four (24/25 overall, 0.96 mean
case recall@4). This is retrieval relevance on a small synthetic corpus, not a
business-incident grounding score or a production-quality estimate.

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
