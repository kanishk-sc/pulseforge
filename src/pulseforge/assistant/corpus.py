"""Finite, allowlisted Markdown runbook indexing and bounded pgvector retrieval."""

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from importlib.resources import files
from pathlib import Path
from typing import Protocol

import psycopg
from psycopg.rows import dict_row

MODEL_NAME = "sentence-transformers/all-MiniLM-L6-v2"
MODEL_REPOSITORY = "qdrant/all-MiniLM-L6-v2-onnx"
MODEL_REVISION = "5f1b8cd78bc4fb444dd171e59b18f3a3af89a079"
MODEL_ID = f"fastembed-0.7.4:qdrant/all-MiniLM-L6-v2-onnx@{MODEL_REVISION}:384"
DIMENSIONS = 384
ALLOWED_PATHS = frozenset(
    {
        "docs/operations/runbooks.md",
        "docs/architecture/phase-4-design.md",
        "docs/architecture/phase-5-design.md",
    }
)
MAX_CHUNK_CHARS = 1800


class EmbeddingUnavailable(RuntimeError):
    """The explicitly installed local model asset is unavailable."""


class IncompatibleIndex(RuntimeError):
    """The persisted vector space cannot be searched with this model."""


class Embedder(Protocol):
    model_id: str

    def embed(self, texts: list[str]) -> list[list[float]]: ...


class LocalEmbedder:
    model_id = MODEL_ID

    def __init__(self, cache_dir: Path, *, allow_download: bool = False):
        try:
            from fastembed import TextEmbedding

            snapshot = (
                cache_dir / "models--qdrant--all-MiniLM-L6-v2-onnx" / "snapshots" / MODEL_REVISION
            )
            if allow_download:
                from huggingface_hub import snapshot_download

                downloaded = snapshot_download(
                    repo_id=MODEL_REPOSITORY,
                    revision=MODEL_REVISION,
                    cache_dir=str(cache_dir),
                    allow_patterns=[
                        "config.json",
                        "model.onnx",
                        "special_tokens_map.json",
                        "tokenizer_config.json",
                        "tokenizer.json",
                    ],
                )
                if Path(downloaded).resolve() != snapshot.resolve():
                    raise IncompatibleIndex("embedding_asset_revision_mismatch")
            required = ("config.json", "model.onnx", "tokenizer_config.json", "tokenizer.json")
            if not all((snapshot / name).is_file() for name in required):
                raise EmbeddingUnavailable("local_embedding_model_unavailable")
            self._model = TextEmbedding(
                model_name=MODEL_NAME,
                specific_model_path=str(snapshot.resolve()),
                local_files_only=True,
                threads=2,
            )
        except IncompatibleIndex:
            raise
        except Exception as exc:
            raise EmbeddingUnavailable("local_embedding_model_unavailable") from exc

    def embed(self, texts: list[str]) -> list[list[float]]:
        result = [vector.tolist() for vector in self._model.embed(texts, batch_size=8)]
        if any(len(vector) != DIMENSIONS for vector in result):
            raise IncompatibleIndex("embedding_dimension_mismatch")
        return result


@dataclass(frozen=True)
class Chunk:
    chunk_id: str
    document_id: str
    section_id: str
    section_title: str
    ordinal: int
    text: str
    content_sha256: str


@dataclass(frozen=True)
class Document:
    document_id: str
    title: str
    topic: str
    source_path: str
    content_sha256: str
    chunks: tuple[Chunk, ...]


def digest(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _slug(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")[:80]


def chunk_markdown(document_id: str, markdown: str) -> tuple[Chunk, ...]:
    """Split at headings, then paragraph boundaries; IDs survive unrelated edits."""
    sections: list[tuple[str, list[str]]] = []
    heading = "Introduction"
    lines: list[str] = []
    for line in markdown.splitlines():
        if re.match(r"^#{1,3} ", line):
            if lines:
                sections.append((heading, lines))
            heading = line.lstrip("# ").strip()
            lines = []
        else:
            lines.append(line)
    if lines:
        sections.append((heading, lines))
    chunks: list[Chunk] = []
    occurrence: dict[str, int] = {}
    for title, body in sections:
        slug = _slug(title) or "section"
        occurrence[slug] = occurrence.get(slug, 0) + 1
        section_id = f"{slug}-{occurrence[slug]}"
        paragraphs = [
            part.strip() for part in re.split(r"\n\s*\n", "\n".join(body)) if part.strip()
        ]
        pieces: list[str] = []
        current = ""
        for paragraph in paragraphs:
            # Oversized code blocks/tables remain bounded and inert.
            fragments = [
                paragraph[i : i + MAX_CHUNK_CHARS]
                for i in range(0, len(paragraph), MAX_CHUNK_CHARS)
            ]
            for fragment in fragments:
                if current and len(current) + len(fragment) + 2 > MAX_CHUNK_CHARS:
                    pieces.append(current)
                    current = ""
                current = f"{current}\n\n{fragment}".strip() if current else fragment
        if current:
            pieces.append(current)
        for ordinal, piece in enumerate(pieces):
            chunks.append(
                Chunk(
                    chunk_id=f"{document_id}:{section_id}:{ordinal}",
                    document_id=document_id,
                    section_id=section_id,
                    section_title=title,
                    ordinal=ordinal,
                    text=piece,
                    content_sha256=digest(piece),
                )
            )
    return tuple(chunks)


def load_corpus(project_root: Path) -> tuple[Document, ...]:
    root = project_root.resolve()
    manifest = root / "docs/assistant/corpus.json"
    data = json.loads(manifest.read_text(encoding="utf-8"))
    if data.get("version") != 1 or not isinstance(data.get("documents"), list):
        raise ValueError("unsupported_corpus_manifest")
    documents: list[Document] = []
    ids: set[str] = set()
    for entry in data["documents"]:
        path = entry["path"]
        if path not in ALLOWED_PATHS:
            raise ValueError("document_not_allowlisted")
        candidate = root / path
        resolved = candidate.resolve()
        if candidate.is_symlink() or not resolved.is_relative_to(root):
            raise ValueError("document_outside_project")
        if entry["id"] in ids or not re.fullmatch(r"[a-z0-9-]{1,100}", entry["id"]):
            raise ValueError("invalid_or_duplicate_document_id")
        ids.add(entry["id"])
        content = resolved.read_text(encoding="utf-8")
        documents.append(
            Document(
                document_id=entry["id"],
                title=entry["title"],
                topic=entry["topic"],
                source_path=path,
                content_sha256=digest(content),
                chunks=chunk_markdown(entry["id"], content),
            )
        )
    return tuple(documents)


def apply_migrations(connection: psycopg.Connection) -> list[str]:
    connection.execute("CREATE SCHEMA IF NOT EXISTS assistant")
    connection.execute(
        "CREATE TABLE IF NOT EXISTS assistant.schema_migrations "
        "(version text PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now())"
    )
    applied = []
    migrations = files("pulseforge.assistant.migrations").iterdir()
    for migration in sorted(migrations, key=lambda e: e.name):
        if not migration.name.endswith(".sql"):
            continue
        version = migration.name.split("_", 1)[0]
        if connection.execute(
            "SELECT 1 FROM assistant.schema_migrations WHERE version=%s", (version,)
        ).fetchone():
            continue
        connection.execute(migration.read_text(encoding="utf-8"))
        connection.execute(
            "INSERT INTO assistant.schema_migrations(version) VALUES (%s)", (version,)
        )
        applied.append(version)
    connection.commit()
    return applied


def _vector(values: list[float]) -> str:
    if len(values) != DIMENSIONS:
        raise IncompatibleIndex("embedding_dimension_mismatch")
    return "[" + ",".join(f"{value:.9g}" for value in values) + "]"


def ingest(
    connection: psycopg.Connection,
    documents: tuple[Document, ...],
    embedder: Embedder,
    *,
    reindex: bool = False,
) -> dict[str, int | str]:
    """Reconcile the complete configured corpus in one transaction."""
    connection.execute("SELECT pg_advisory_xact_lock(47921206)")
    state = connection.execute("SELECT model_id, dimensions FROM assistant.index_state").fetchone()
    if state and (state[0] != embedder.model_id or state[1] != DIMENSIONS):
        if not reindex:
            raise IncompatibleIndex("reindex_required_for_embedding_model_change")
        connection.execute("DELETE FROM assistant.documents")
        connection.execute("DELETE FROM assistant.index_state")
    active_ids = [document.document_id for document in documents]
    connection.execute(
        "DELETE FROM assistant.documents WHERE document_id <> ALL(%s)", (active_ids,)
    )
    updated = 0
    embedded = 0
    for document in documents:
        existing = connection.execute(
            "SELECT content_sha256, title, topic, source_path "
            "FROM assistant.documents WHERE document_id=%s",
            (document.document_id,),
        ).fetchone()
        if existing and existing[0] == document.content_sha256:
            if existing[1:] != (document.title, document.topic, document.source_path):
                connection.execute(
                    "UPDATE assistant.documents SET title=%s, topic=%s, source_path=%s, "
                    "ingested_at=%s WHERE document_id=%s",
                    (
                        document.title,
                        document.topic,
                        document.source_path,
                        datetime.now(UTC),
                        document.document_id,
                    ),
                )
                updated += 1
            continue
        vectors = embedder.embed([chunk.text for chunk in document.chunks])
        if len(vectors) != len(document.chunks):
            raise IncompatibleIndex("embedding_count_mismatch")
        connection.execute(
            "DELETE FROM assistant.documents WHERE document_id=%s", (document.document_id,)
        )
        connection.execute(
            "INSERT INTO assistant.documents "
            "(document_id, title, topic, source_path, content_sha256, ingested_at) "
            "VALUES (%s,%s,%s,%s,%s,%s)",
            (
                document.document_id,
                document.title,
                document.topic,
                document.source_path,
                document.content_sha256,
                datetime.now(UTC),
            ),
        )
        with connection.cursor() as cursor:
            cursor.executemany(
                "INSERT INTO assistant.chunks "
                "(chunk_id, document_id, section_id, section_title, ordinal, chunk_text, "
                "content_sha256, model_id, embedding) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s::vector)",
                [
                    (
                        chunk.chunk_id,
                        document.document_id,
                        chunk.section_id,
                        chunk.section_title,
                        chunk.ordinal,
                        chunk.text,
                        chunk.content_sha256,
                        embedder.model_id,
                        _vector(vector),
                    )
                    for chunk, vector in zip(document.chunks, vectors, strict=True)
                ],
            )
        updated += 1
        embedded += len(vectors)
    hashes = "|".join(
        f"{d.document_id}:{d.title}:{d.topic}:{d.source_path}:{d.content_sha256}" for d in documents
    )
    corpus_sha = digest(hashes)
    connection.execute(
        "INSERT INTO assistant.index_state "
        "(singleton, model_id, dimensions, corpus_sha256, updated_at) "
        "VALUES (true,%s,%s,%s,%s) ON CONFLICT (singleton) DO UPDATE SET "
        "model_id=excluded.model_id, dimensions=excluded.dimensions, "
        "corpus_sha256=excluded.corpus_sha256, updated_at=excluded.updated_at",
        (embedder.model_id, DIMENSIONS, corpus_sha, datetime.now(UTC)),
    )
    return {"updated_documents": updated, "embedded_chunks": embedded, "corpus_sha256": corpus_sha}


def retrieve(
    connection: psycopg.Connection,
    query: str,
    *,
    embedder: Embedder | None,
    top_k: int = 4,
) -> tuple[str, list[dict]]:
    if not 1 <= top_k <= 8 or not 1 <= len(query) <= 500:
        raise ValueError("invalid_retrieval_bounds")
    with connection.cursor(row_factory=dict_row) as cursor:
        cursor.execute("SELECT model_id, corpus_sha256 FROM assistant.index_state")
        state = cursor.fetchone()
        if not state:
            return "unavailable", []
        if embedder is not None:
            if state["model_id"] != embedder.model_id:
                raise IncompatibleIndex("reindex_required_for_embedding_model_change")
            vector = _vector(embedder.embed([query])[0])
            cursor.execute(
                "SELECT c.chunk_id, c.section_id, c.section_title, c.chunk_text, "
                "d.document_id, d.title, d.topic, d.source_path, d.content_sha256, "
                "(c.embedding <=> %s::vector) AS distance "
                "FROM assistant.chunks c JOIN assistant.documents d USING (document_id) "
                "WHERE c.model_id=%s ORDER BY c.embedding <=> %s::vector, c.chunk_id LIMIT %s",
                (vector, embedder.model_id, vector, 12),
            )
            vector_rows = list(cursor.fetchall())
            lexical_rows = _lexical_rows(cursor, query, 12)
            combined = {row["chunk_id"]: row for row in vector_rows + lexical_rows}
            scores: dict[str, float] = {}
            for weight, rows in ((1.0, vector_rows), (1.0, lexical_rows)):
                for rank, row in enumerate(rows, start=1):
                    scores[row["chunk_id"]] = scores.get(row["chunk_id"], 0) + weight / (60 + rank)
            ordered = sorted(combined, key=lambda key: (-scores[key], key))[:top_k]
            return "semantic", [combined[key] for key in ordered]
        return "lexical_fallback", _lexical_rows(cursor, query, top_k)


def _lexical_rows(cursor: psycopg.Cursor, query: str, limit: int) -> list[dict]:
    """Bounded keyword complement; never represented as embeddings when used alone."""
    words = list(dict.fromkeys(re.findall(r"[a-z0-9]{3,}", query.lower())))[:12]
    lexical_query = " | ".join(words) if words else "unmatchable"
    cursor.execute(
        "SELECT c.chunk_id, c.section_id, c.section_title, c.chunk_text, "
        "d.document_id, d.title, d.topic, d.source_path, d.content_sha256, "
        "ts_rank_cd(to_tsvector('english', c.chunk_text || ' ' || d.title || ' ' || "
        "c.section_title), to_tsquery('english', %s)) AS rank "
        "FROM assistant.chunks c JOIN assistant.documents d USING (document_id) "
        "WHERE to_tsvector('english', c.chunk_text || ' ' || d.title || ' ' || "
        "c.section_title) @@ to_tsquery('english', %s) "
        "ORDER BY rank DESC, c.chunk_id LIMIT %s",
        (lexical_query, lexical_query, limit),
    )
    return list(cursor.fetchall())
