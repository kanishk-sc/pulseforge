import json

import pytest

from pulseforge.product.publication import _artifact_metadata


def write_artifact(tmp_path, results):
    artifact = tmp_path / "run_results.json"
    artifact.write_text(
        json.dumps({"metadata": {"invocation_id": "dbt-test"}, "results": results}),
        encoding="utf-8",
    )
    return artifact


def test_empty_dbt_artifact_fails_closed(tmp_path):
    with pytest.raises(ValueError, match="no dbt results"):
        _artifact_metadata(write_artifact(tmp_path, []))


def test_non_success_dbt_artifact_fails_closed(tmp_path):
    with pytest.raises(ValueError, match="non-success"):
        _artifact_metadata(write_artifact(tmp_path, [{"status": "error"}]))


def test_successful_dbt_artifact_returns_invocation_and_hash(tmp_path):
    invocation_id, artifact_hash = _artifact_metadata(
        write_artifact(tmp_path, [{"status": "success"}, {"status": "pass"}])
    )

    assert invocation_id == "dbt-test"
    assert len(artifact_hash) == 64
