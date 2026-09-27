"""Disposable-database migration concurrency and repeatability checks."""

import os
from pathlib import Path
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql

from pulseforge.assistant.corpus import apply_migrations as assistant_migrate
from pulseforge.product.cli import pipeline
from pulseforge.product.schema import apply_migrations as product_migrate

pytestmark = [pytest.mark.integration, pytest.mark.deployment]


def test_release_migrations_are_serial_and_idempotent(monkeypatch):
    project = os.environ.get("PULSEFORGE_DEPLOYMENT_TEST_PROJECT", "")
    if not project.startswith("pulseforge-p7-"):
        pytest.fail("Deployment integration must target an explicit isolated project")
    host = os.environ.get("POSTGRES_HOST")
    port = os.environ.get("POSTGRES_PORT")
    if host != "127.0.0.1" or port != "15432":
        pytest.fail("Deployment integration requires the isolated PostgreSQL host port")
    admin = (
        f"host={host} port={port} dbname={os.environ['POSTGRES_DB']} "
        f"user={os.environ['POSTGRES_USER']} "
        f"password={os.environ['POSTGRES_PASSWORD']} connect_timeout=5"
    )
    database = f"pulseforge_deploy_{uuid4().hex}"
    with psycopg.connect(admin, autocommit=True) as connection:
        connection.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(database)))
    try:
        dsn = admin.replace(f"dbname={os.environ['POSTGRES_DB']}", f"dbname={database}")
        for lock_id, migrate in ((47921207, product_migrate), (47921208, assistant_migrate)):
            with psycopg.connect(dsn) as holder, psycopg.connect(dsn) as contender:
                holder.execute("SELECT pg_advisory_xact_lock(%s)", (lock_id,))
                with pytest.raises(RuntimeError, match="already running"):
                    migrate(contender)
                contender.rollback()
                holder.rollback()
                assert migrate(contender)
                assert migrate(contender) == []
        with psycopg.connect(dsn) as holder:
            holder.execute("SELECT pg_advisory_lock(72419022)")
            monkeypatch.setenv("POSTGRES_DB", database)
            with pytest.raises(RuntimeError, match="pipeline already owns"):
                pipeline("must-not-run-dbt", Path("."), Path("."))
            holder.execute("SELECT pg_advisory_unlock(72419022)")
    finally:
        with psycopg.connect(admin, autocommit=True) as connection:
            connection.execute(
                sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(database))
            )
