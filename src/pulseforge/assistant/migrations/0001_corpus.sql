CREATE EXTENSION IF NOT EXISTS vector;
CREATE SCHEMA IF NOT EXISTS assistant;

CREATE TABLE assistant.documents (
    document_id text PRIMARY KEY,
    title text NOT NULL,
    topic text NOT NULL,
    source_path text NOT NULL UNIQUE,
    content_sha256 text NOT NULL,
    ingested_at timestamptz NOT NULL,
    CHECK (length(document_id) BETWEEN 1 AND 100)
);

CREATE TABLE assistant.chunks (
    chunk_id text PRIMARY KEY,
    document_id text NOT NULL REFERENCES assistant.documents(document_id) ON DELETE CASCADE,
    section_id text NOT NULL,
    section_title text NOT NULL,
    ordinal integer NOT NULL,
    chunk_text text NOT NULL,
    content_sha256 text NOT NULL,
    model_id text NOT NULL,
    embedding vector(384) NOT NULL,
    UNIQUE (document_id, section_id, ordinal),
    CHECK (length(chunk_text) BETWEEN 1 AND 2000)
);
CREATE INDEX assistant_chunks_document ON assistant.chunks (document_id, section_id, ordinal);
CREATE INDEX assistant_chunks_vector ON assistant.chunks
    USING hnsw (embedding vector_cosine_ops);

CREATE TABLE assistant.index_state (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    model_id text NOT NULL,
    dimensions integer NOT NULL CHECK (dimensions = 384),
    corpus_sha256 text NOT NULL,
    updated_at timestamptz NOT NULL
);
