from importlib.resources import files

import psycopg


def apply_migrations(connection: psycopg.Connection) -> list[str]:
    """Apply packaged SQL migrations exactly once and return newly applied versions."""
    connection.execute("CREATE SCHEMA IF NOT EXISTS product")
    connection.execute(
        """CREATE TABLE IF NOT EXISTS product.schema_migrations (
        version text PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now())"""
    )
    applied: list[str] = []
    root = files("pulseforge.product.migrations")
    for migration in sorted(root.iterdir(), key=lambda entry: entry.name):
        if not migration.name.endswith(".sql"):
            continue
        version = migration.name.split("_", 1)[0]
        exists = connection.execute(
            "SELECT 1 FROM product.schema_migrations WHERE version=%s", (version,)
        ).fetchone()
        if exists:
            continue
        connection.execute(migration.read_text(encoding="utf-8"))
        connection.execute("INSERT INTO product.schema_migrations(version) VALUES (%s)", (version,))
        applied.append(version)
    connection.commit()
    return applied
