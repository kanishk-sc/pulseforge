# Phase 6: evidence-bound incident assistance

## Boundary and data flow

The assistant explains an **existing** deterministic incident. It does not detect,
create, resolve or suppress incidents; it has no SQL-generation, shell, browser or
remediation tools. Airflow and Spark are unchanged. The product API still serves only
successful immutable publications. An explanation reads the incident's original build,
not the latest build's metrics. Current publication and attempt status are labeled
separately; Spark liveness remains unobserved.

```text
product.incidents + incident_evidence + original analytics_builds
              | read-only repeatable-read transaction
              v
typed bounded evidence bundle <- assistant.documents/chunks (pgvector)
              |                            ^
              |                     finite allowlisted indexer
              v
deterministic offline explanation -> POST /api/v1/incidents/{id}/explanation
              |                            |
              | optional explicit provider   v
              +----------------------> React incident detail
```

The application chooses fixed, parameterized queries. The incident's detector name
and region form a bounded retrieval query; API callers cannot submit SQL, URLs,
filesystem paths, arbitrary chat turns, or provider endpoints. The read-only
transaction selects one incident, its original build, at most 12 ordered source
references, the currently successful build and attempt timestamps, and at most four
indexed runbook chunks. The total source reference count is also returned so the bound
is visible. `EvidenceBundle` validates those bounds and build identity before output.
If an older incident's references are absent from its original build's immutable
evidence projection, the explanation explicitly reports the gap. Source references
remain references, not reconstructed event payloads or proof of root cause.

## Corpus and retrieval contract

`docs/assistant/corpus.json` is the versioned, project-owned allowlist. Only the
three checked-in paths listed there may be ingested. `load_corpus` rejects traversal,
unlisted paths, duplicate IDs and symlink escapes. Markdown is split at level 1–3
headings and paragraph boundaries, with a maximum of 1,800 characters per chunk.
Stable IDs are `document-id:section-slug-occurrence:ordinal`; a SHA-256 of document
and chunk content records revisions. `assistant.documents` stores source path, topic,
title, content hash and ingestion timestamp. `assistant.chunks` stores section, text,
hash, model identity and a 384-dimensional vector in a separate schema. No Phase 2
warehouse table or checkpoint changes.

The finite CLI takes an advisory transaction lock, reconciles the entire allowlist,
does nothing for unchanged documents, updates changed metadata without re-embedding,
replaces changed document chunks, and removes
documents no longer configured. Re-running it creates no duplicate chunks. Model ID
and dimensions are recorded in `assistant.index_state`. Mismatch requires explicit
`--reindex`; vectors from different models are never searched together. The indexer
commits the complete corpus reconciliation as one transaction. Do not use `--reindex`
casually: it deletes only assistant corpus rows in that transaction and regenerates
their embeddings, never source data or product incidents.

The local model is FastEmbed 0.7.4's ONNX export of
`sentence-transformers/all-MiniLM-L6-v2`, from
`qdrant/all-MiniLM-L6-v2-onnx` revision
`5f1b8cd78bc4fb444dd171e59b18f3a3af89a079`. Its advertised license is
Apache-2.0, dimensionality 384, and model size approximately 0.09 GB, per the
[official FastEmbed supported-model registry](https://qdrant.github.io/fastembed/examples/Supported_Models/).
The local downloaded cache occupied 86.9 MiB in this Windows checkout; this is a
measurement of files, not peak RAM. Allow several hundred MiB of spare memory for
ONNX inference and index operations; peak use has not been benchmarked. CPU inference
is bounded to two threads and batches of eight. Initial `download-model` is the only
intended model-asset network setup; normal indexing and explanation use the exact
pinned local snapshot with `local_files_only=True`. If that asset is absent or
incompatible, the API labels PostgreSQL full-text matching as `lexical_fallback`.
If no corpus is indexed, retrieval is `unavailable`. Neither is claimed as semantic.

Semantic search uses pgvector cosine ordering with `LIMIT 12`, plus a bounded
PostgreSQL full-text candidate query. Reciprocal-rank fusion produces at most four
sections, with `chunk_id` as deterministic tie-breaker. All values are bound
parameters; SQL fragments and table names are application constants. The extension
comes from the [pgvector 0.8.6 PostgreSQL 16 Bookworm image](https://github.com/pgvector/pgvector#docker).
The existing named PostgreSQL volume is retained. The local image changed only the
PostgreSQL minor release from 16.9 to 16.15; observed warehouse, incident and build
counts remained unchanged after service recreation. Migration `0001` creates the
extension and separate schema explicitly; default foundation startup does not run
assistant migrations or download a model.

## Response, provider and failures

The offline response is labeled exactly “Offline evidence summary — no generative
model used.” Its observed facts copy stored detector numbers and source references;
interpretation says only that the recorded comparison crossed its threshold. It
offers runbook-section review steps, not remediation execution, and it abstains from
root-cause hypotheses. Each statement references citation IDs from the validated
bundle. The response distinguishes original and current build identities, stale
historical publication, missing projection evidence, truncated references and
unobserved Spark state. Decimal values are rendered from `Decimal`, not floats.

`mode=provider` is opt-in and disabled by default. The only adapter targets the
[OpenAI Responses API](https://developers.openai.com/api/docs/guides/text) at a fixed
URL, with a model supplied by the operator rather than a hard-coded model name.
It follows the provider's [structured-output `text.format` contract](https://developers.openai.com/api/docs/guides/structured-outputs).
No endpoint, credentials or model selection comes from the HTTP request. The request
sets `store=false`, disables tools, disallows server-side truncation, limits input to
16,000 characters and output to 1,200 tokens, and sends only a bounded deterministic
summary of the evidence with supplied citation IDs. At most two concurrent requests, a 25-second API deadline,
an eight-second transport timeout and one short retry for timeout, 429 or 5xx are
configured. Authentication failure, malformed output, refusal, incomplete response,
deadline and rate limit have distinct controlled errors; there is no silent provider
switch or provider-to-offline fallback. Cancellation stops the awaiting request; a
cancelled thread performing read-only local retrieval may finish independently.

The adapter cannot replace deterministic observed facts. It may add interpretation,
hypotheses and diagnostic suggestions only. Pydantic validates structure/size and
citation IDs against evidence actually supplied to that invocation. Citation validity
is **not** semantic grounding; false causal claims remain possible and require the
separate evaluation rubric. Provider tests use mocked HTTP only. No live billable
inference or incident/runbook transmission was authorized or performed in Phase 6
verification, so the live adapter is unverified.

## Trust and operational limits

Runbook text and provider output are treated as untrusted data. The fixed provider
instructions forbid following embedded instructions, revealing secrets, inventing
citations or acting on infrastructure; no tools are exposed. React renders text as
escaped nodes and source anchors point to displayed evidence, not arbitrary links.
The API logs route, status, request ID and timing, but not raw prompts, excerpts,
keys or generated explanations. Prometheus labels are fixed mode/outcome classes,
never incident IDs or user content; telemetry failures are best-effort.

This is localhost-only and unauthenticated. Do not expose the dashboard/API on a
network or enable a billable provider without authentication, authorization, cost
controls and an explicit data-sharing decision. No explanation history is persisted;
the browser holds the current response in memory until view/filter teardown. Only
the runbook corpus and version-controlled evaluation artifacts persist. The corpus
contains project documentation, not customer secrets. This phase makes no production
accuracy, latency, security or SLA claim. Phase 7 remains separate.
