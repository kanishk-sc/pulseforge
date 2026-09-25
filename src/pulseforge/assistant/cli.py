"""Finite assistant schema, model, corpus and explanation operations."""

import argparse
import json
from pathlib import Path
from uuid import UUID

import psycopg

from pulseforge.assistant.corpus import (
    LocalEmbedder,
    apply_migrations,
    ingest,
    load_corpus,
    retrieve,
)
from pulseforge.assistant.service import _dsn, explain_offline
from pulseforge.config import Settings


def main() -> int:
    parser = argparse.ArgumentParser(description="PulseForge finite assistant operations")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("migrate")
    sub.add_parser("download-model")
    index = sub.add_parser("ingest")
    index.add_argument("--project-root", type=Path, default=Path.cwd())
    index.add_argument("--reindex", action="store_true")
    search = sub.add_parser("retrieve")
    search.add_argument("query")
    search.add_argument("--top-k", type=int, default=4)
    explain = sub.add_parser("explain")
    explain.add_argument("incident_id", type=UUID)
    args = parser.parse_args()
    settings = Settings()
    if args.command == "download-model":
        model = LocalEmbedder(Path(settings.assistant_model_cache_dir), allow_download=True)
        print(
            json.dumps({"model_id": model.model_id, "dimensions": len(model.embed(["probe"])[0])})
        )
        return 0
    if args.command == "explain":
        print(explain_offline(settings, args.incident_id).model_dump_json(indent=2))
        return 0
    with psycopg.connect(_dsn(settings)) as connection:
        if args.command == "migrate":
            print(json.dumps({"applied": apply_migrations(connection)}))
            return 0
        if args.command == "ingest":
            model = LocalEmbedder(Path(settings.assistant_model_cache_dir))
            result = ingest(connection, load_corpus(args.project_root), model, reindex=args.reindex)
            connection.commit()
            print(json.dumps(result))
            return 0
        model = LocalEmbedder(Path(settings.assistant_model_cache_dir))
        mode, chunks = retrieve(connection, args.query, embedder=model, top_k=args.top_k)
        print(json.dumps({"mode": mode, "chunks": chunks}, default=str, indent=2))
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
